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

package org.ruscello.explore

import org.chombo.spark.common.JobConfiguration
import org.apache.spark.SparkContext
import scala.collection.JavaConverters._
import org.chombo.util.BasicUtils
import org.chombo.util.SeasonalAnalyzer
import com.typesafe.config.Config
import org.chombo.spark.common.Record
import org.chombo.spark.common.GeneralUtility
import scala.collection.mutable.ArrayBuffer

/**
 * Selection within aligned time window
 * @author pranab
 *
 */
object TemporalSelector extends JobConfiguration with GeneralUtility {
   
  /**
   * @param args
   * @return
   */
   def main(args: Array[String]) {
	   val appName = "temporalSelector"
	   val Array(inputPath: String, outputPath: String, configFile: String) = getCommandLineArgs(args, 3)
	   val config = createConfig(configFile)
	   val sparkConf = createSparkConf(appName, config, false)
	   val sparkCntxt = new SparkContext(sparkConf)
	   val appConfig = config.getConfig(appName)
	   
	   //configuration params
	   val fieldDelimIn = getStringParamOrElse(appConfig, "field.delim.in", ",")
	   val fieldDelimOut = getStringParamOrElse(appConfig, "field.delim.out", ",")
	   val attrOrdinal = getMandatoryIntParam(appConfig, "attr.ordinals")
	   val keyFieldOrdinals = toOptionalIntArray(getOptionalIntListParam(appConfig, "id.fieldOrdinals"))
	   val timeStampFieldOrdinal = getMandatoryIntParam(appConfig, "time.fieldOrdinal", 
	       "missing time stamp field ordinal")
	   val timeStampInMs = getBooleanParamOrElse(appConfig, "time.inMili", true)
	   val aggrWindowTimeUnit = getMandatoryStringParam(appConfig, "aggr.windowTimeUnit", 
	       "missing aggr window time unit")
	   val aggrWindowTimeLength = getMandatoryIntParam(appConfig, "aggr.windowTimeLength", 
	       "missing aggr window time length")
	   val debugOn = getBooleanParamOrElse(appConfig, "debug.on", false)
	   val saveOutput = getBooleanParamOrElse(appConfig, "save.output", true)
	   
	   val timeWindow = 
	   if (timeStampInMs) {
	     BasicUtils.toEpochTime(aggrWindowTimeUnit) * aggrWindowTimeLength
	   } else {
	     BasicUtils.toEpochTime(aggrWindowTimeUnit) * aggrWindowTimeLength / 1000
	   }
	   val selType = getStringParamOrElse(appConfig, "sel.type", "max") 
	   val validSelections = Array("max", "min")
	   assertStringMember(selType, validSelections, "invalid selection type " + selType)
	   val keyLen = getOptinalArrayLength(keyFieldOrdinals) + 1
	  
	   //input
	  val data = sparkCntxt.textFile(inputPath)	  
	  //key by id, ts, field ord
	  val selData = data.map(line => {
		   val fields = BasicUtils.getTrimmedFields(line, fieldDelimIn)
		   val ts = fields(timeStampFieldOrdinal).toLong
		   val tsPart = (ts / timeWindow) * timeWindow
		   val key = Record(keyLen)
		   Record.populateFieldsWithIndex(fields, keyFieldOrdinals, key)
		   key.addLong(tsPart)
		   val value = Record(2)
		   value.addString(line)
		   value.addDouble(fields(attrOrdinal).toDouble)
		   (key, value)
	  }).reduceByKey((v1, v2) => {
	    //select within window
	    selType match {
	      case "max" => {
	        if (v1.getDouble(1) > v2.getDouble(1)) v1 else  v2
	      }
	      case "min" => {
	        if (v1.getDouble(1) < v2.getDouble(1)) v1 else  v2
	      }
	    }
	  }).map(r => {
	    //replace time stamp with window aligned time stamp
	    val alignedTime = r._1.getLong(r._1.size - 1)
	    val fields = BasicUtils.getTrimmedFields(r._2.getString(0), fieldDelimIn)
	    fields(timeStampFieldOrdinal) = alignedTime.toString
	    val keyRec = Record(r._1, 0, r._1.size - 1)
	    (keyRec, fields)
	  }).groupByKey.flatMap(r => {
	    val values = r._2.toArray.sortBy(v => {v(timeStampFieldOrdinal).toLong})
	    val newValues = ArrayBuffer[String]()
	    var lastTs:Long = 0
	    var curTs:Long = 0
	    values.foreach(v => {
	      //fill gaps in case of under sampling
	      if (lastTs == 0) {
	        lastTs = v(timeStampFieldOrdinal).toLong
	        newValues += v.mkString(fieldDelimOut)
	      } else {
	        curTs =  v(timeStampFieldOrdinal).toLong
	        if (curTs - lastTs > timeWindow) {
	          var genTs = lastTs + timeWindow
	          while(genTs <= curTs) {
	            v(timeStampFieldOrdinal) = genTs.toString
	            newValues += v.mkString(fieldDelimOut)
	            genTs += timeWindow
	          }
	        } else {
	          newValues += v.mkString(fieldDelimOut)
	        }
	      }
	      lastTs = curTs
	    })
	    newValues
	  })
	  
	  if (debugOn) {
	     selData.collect.slice(0,50).foreach(s => println(s))
	  }
	   
	  if (saveOutput) {
	     selData.saveAsTextFile(outputPath)
	  }
   }
}