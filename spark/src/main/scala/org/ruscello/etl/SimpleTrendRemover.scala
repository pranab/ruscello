/*
 * ruscello: on spark
 * Author: Pranab Ghosh
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You may
 * obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0 
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */


package org.ruscello.etl

import org.chombo.spark.common.JobConfiguration
import org.apache.spark.SparkContext
import scala.collection.JavaConverters._
import org.chombo.util.BasicUtils
import com.typesafe.config.Config
import org.chombo.spark.common.Record
import org.chombo.spark.common.GeneralUtility
import scala.collection.mutable.ArrayBuffer

object SimpleTrendRemover extends JobConfiguration with GeneralUtility {
   
  /**
   * @param args
   * @return
   */
   def main(args: Array[String]) {
	   val appName = "simpleTrendRemover"
	   val Array(inputPath: String, outputPath: String, configFile: String) = getCommandLineArgs(args, 3)
	   val config = createConfig(configFile)
	   val sparkConf = createSparkConf(appName, config, false)
	   val sparkCntxt = new SparkContext(sparkConf)
	   val appConfig = config.getConfig(appName)
	   
	   //configuration params
	   val fieldDelimIn = getStringParamOrElse(appConfig, "field.delim.in", ",")
	   val fieldDelimOut = getStringParamOrElse(appConfig, "field.delim.out", ",")
	   val attrOrdinals = getMandatoryIntListParam(appConfig, "attr.ordinals").asScala.toArray
	   val keyFields = getOptionalIntListParam(appConfig, "id.fieldOrdinals")
	   val keyFieldOrdinals = toOptionalIntArray(keyFields)
	   val timeStampFieldOrdinal = getMandatoryIntParam(appConfig, "time.fieldOrdinal", 
	       "missing time stamp field ordinal")
	   val keyLen = getOptinalArrayLength(keyFieldOrdinals, 1)
	   val timeStampInMs = getBooleanParamOrElse(appConfig, "time.inMili", true)
	   val numSegments = getIntParamOrElse(appConfig, "num.segments", 2)
	   val remAverage = getBooleanParamOrElse(appConfig, "rem.average", true)
	   val outputPrecision = this.getIntParamOrElse(appConfig, "output.precision", 3)
	   
	   val debugOn = getBooleanParamOrElse(appConfig, "debug.on", false)
	   val saveOutput = getBooleanParamOrElse(appConfig, "save.output", true)
	   
	   //input
	   val data = sparkCntxt.textFile(inputPath).cache 

	   //time span
	   val timeSpansCol = data.flatMap(line => {
		   val fields = BasicUtils.getTrimmedFields(line, fieldDelimIn)
		   val ts = fields(timeStampFieldOrdinal).toLong
		   val key =  Record(keyLen)
		   populateFields(fields, keyFieldOrdinals, key, "all")
		   
		   attrOrdinals.map(i => {
		     val newKey = Record(keyLen + 1, key)
		     newKey.addInt(i)
		     (newKey,  (ts, ts))
		   })
	   }).reduceByKey((v1, v2) => {
	     val min = if (v1._1 < v2._1) v1._1 else v2._1
	     val max = if (v1._2 > v2._2) v1._2 else v2._2
	     (min, max)
	   }).mapValues(v =>{ 
	     //start time, time span and time segment length
	     val span = v._2 - v._1
	     (v._1, span, span / numSegments)
	   }).collectAsMap
	   
	   //key by segment index
	   val keyedData = data.flatMap(line => {
		   val fields = BasicUtils.getTrimmedFields(line, fieldDelimIn)
		   val ts = fields(timeStampFieldOrdinal).toLong
		   val baseKey =  Record(keyLen)
		   populateFields(fields, keyFieldOrdinals, baseKey, "all")
		   
		   attrOrdinals.map(i => {
		     val key = Record(keyLen + 1, baseKey)
		     key.addInt(i)
		     val tSpan = getMapValue(timeSpansCol, key, "missing time span data")
		     val segIndex =  ((ts - tSpan._1) /  tSpan._3).toInt
		     val fullKey = Record(keyLen + 2, key)
		     key.addInt(segIndex)
		     val value = (1, fields(i).toDouble)
		     (fullKey, value)
		   })
	   })
	   
	   //indexed average 
	   val averages = findAverageDouble(keyedData).map(r => {
	     //move segment index from key to value
	     val key = r._1
	     val newKey = Record(key, 0, key.size - 1)
	     val value = Record(2)
	     value.addInt(key.getInt(key.size - 1))
	     value.add(r._2)
	     (newKey,value)
	   }).cache
	   
	   //collected averages
	   val averagesCol = averages.groupByKey.map(r => {
	      val sorted = r._2.toArray.sortBy(e => e.getInt(0)).map(r => r.getDouble(1))
	      (r._1, sorted)
	   }).collectAsMap
	    
	   //slopes
	   val indexedSlopesCol = averages.groupByKey.map(r => {
	     //segment mean slope
	     val key = r._1
	     val tSpan = getMapValue(timeSpansCol, key, "missing time span data")
	     val segLength = tSpan._3
	     val sorted = r._2.toArray.sortBy(e => e.getInt(0))
	     val slopes = ArrayBuffer[(Int,Double)]()
	     sorted.foreach(v => {
	       val i = v.getInt(0)
	       if (i > 0) {
	         val sl = (v.getDouble(1) - sorted(i - 1).getDouble(1)) / segLength
	         val inSl = (i -1, sl)
	         slopes += inSl
	       }
	     })
	     (key, slopes.toArray)
	   }).collectAsMap
	   
	   //generate additional slope by taking average of adjacent slopes
	   val slopesCol  = indexedSlopesCol.map(r => {
	     val key = r._1
	     val slopes = r._2.sortBy(v => v._1)
	     val newSlopes = ArrayBuffer[Double]()
	     slopes.foreach(sl => {
	       val i = sl._1
	       if (i == 0) {
	         newSlopes += sl._2
	       } else if (slopes.length > 1){
	         newSlopes += (sl._2 + slopes(i - 1)._2) / 2
	       }
	       if (i == slopes.size - 1) {
	         newSlopes += sl._2
	       }
	     })
	     (key, newSlopes.toArray)
	   })
	   
	   
	   //de trend
	   val adjustedData = data.map(line => {
		   val fields = BasicUtils.getTrimmedFields(line, fieldDelimIn)
		   val ts = fields(timeStampFieldOrdinal).toLong
		   val baseKey =  Record(keyLen)
		   populateFields(fields, keyFieldOrdinals, baseKey, "all")
		   
		   attrOrdinals.foreach(i => {
		     var va = fields(i).toDouble
		     val key = Record(keyLen + 1, baseKey)
		     key.addInt(i)
		     val tSpan = getMapValue(timeSpansCol, key, "missing time span data")
		     val tDelta = (ts - tSpan._1)
		     val segIndex = (tDelta / tSpan._3).toInt
		     
		     val slope = getMapValue(slopesCol, key, "missing slope data")(segIndex)
		     val  slCorr = slope * tDelta
		     va -= slCorr
		     
		     if (remAverage) {
		        val average = getMapValue(averagesCol, key, "missing average data")(segIndex) 
		        va -= average
		     }
		     fields(i) = BasicUtils.formatDouble(va, outputPrecision)
		   })
		   fields.mkString(fieldDelimOut)
	   })
	   
	   if (debugOn) {
	     adjustedData.collect.slice(0,50).foreach(s => println(s))
	   }
	   
	   if (saveOutput) {
	     adjustedData.saveAsTextFile(outputPath)
	   }
   }

}