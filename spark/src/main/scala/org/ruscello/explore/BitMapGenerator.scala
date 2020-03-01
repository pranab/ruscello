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

import scala.collection.JavaConverters.mapAsScalaMapConverter
import scala.collection.mutable.ArrayBuffer
import scala.util.control.Breaks.break
import scala.util.control.Breaks.breakable

import org.apache.spark.SparkContext
import org.chombo.spark.common.GeneralUtility
import org.chombo.spark.common.JobConfiguration
import org.chombo.spark.common.Record
import org.chombo.stats.HistogramStat
import org.chombo.util.BasicUtils
import org.chombo.util.Utility


object BitMapGenerator extends JobConfiguration with GeneralUtility {
  
   /**
    * @param args
    * @return
    */
  def main(args: Array[String]) {
    val appName = "bitMapGenerator"
    val Array(inputPath: String, outputPath: String, configFile: String) = getCommandLineArgs(args, 3)
    val config = createConfig(configFile)
    val sparkConf = createSparkConf(appName, config, false)
    val sparkCntxt = new SparkContext(sparkConf)
    val appConfig = config.getConfig(appName)
	   
	  //configurations
    val fieldDelimIn = getStringParamOrElse(appConfig, "field.delim.in", ",")
	  val fieldDelimOut = getStringParamOrElse(appConfig, "field.delim.out", ",")
	  val seqFieldOrd = getMandatoryIntParam(appConfig, "seq.fieldOrdinal", "missing sequence filed ordinal")
	  val keyFields = getOptionalIntListParam(appConfig, "id.fieldOrdinals")
	  val keyFieldOrdinals = toOptionalIntArray(keyFields)
	  val numAttrOrdinals = toIntArray(getMandatoryIntListParam(appConfig, "attr.ordinals", 
	      "missing quant attribute ordinals"))
    val distrFilePath = getMandatoryStringParam(appConfig, "distr.file.path", "missing distr file path")
    val isHdfsFile = getBooleanParamOrElse(appConfig, "hdfs.file", false)
	  val outputPrecision = getIntParamOrElse(appConfig, "output.precision", 3);
    val debugOn = appConfig.getBoolean("debug.on")
    val saveOutput = appConfig.getBoolean("save.output")
    
    //empirical distribution
    val numBins = getIntParamOrElse(appConfig, "num.bins", 10)
		val keyLen = getOptinalArrayLength(keyFieldOrdinals, 1)
		val keyedPercentiles = createEqProbHist(distrFilePath, isHdfsFile, keyLen, numBins)
		
	  //input
	  val data = sparkCntxt.textFile(inputPath)
	   
	  val keyedData = data.map(line => {
	   val items = BasicUtils.getTrimmedFields(line, fieldDelimIn)
	   val key = Record(keyLen)
	   populateFields(items, keyFieldOrdinals, key, "all")

	   val value = Record(2)
	   val seq = items(seqFieldOrd).toLong
	   value.addLong(seq)
	   value.addString(line)
	   (key, value)
	 })	   
		
	 val symPairData = keyedData.groupByKey.flatMap(v => {
	   val key = v._1
	   val percentiles = keyedPercentiles.get(key).get
	   val keyStr = key.toString
	   val values = v._2.toArray.sortBy(v => v.getLong(0))
	   
	   val keyedAttrValues = Map[Int, ArrayBuffer[Double]]()
	   values.map(r => {
	     val line = r.getString(1)
	     val items = BasicUtils.getTrimmedFields(line, fieldDelimIn)
	     numAttrOrdinals.foreach(attr => {
	       val value = items(attr).toDouble
	       val attrValues = keyedAttrValues.getOrElse(attr, ArrayBuffer[Double]())
	       attrValues += value
	     })
	   })
	   
	   val symPairs = keyedAttrValues.flatMap(r => {
	     val attr = r._1
	     val attrValues = r._2
	     
	     var preSym = getPercentileIndex(percentiles, attrValues(0)).toString()
	     var curSym = getPercentileIndex(percentiles, attrValues(1)).toString()
	     val symPairs = ArrayBuffer[Record]()
	     for (i <- 1 to attrValues.length - 1) {
	       val symPair = Record(key.size + 3, key)
	       symPair.addInt(attr)
	       symPair.addString(preSym)
	       symPair.addString(curSym)
	       symPairs += symPair
	     }
	     symPairs
	   })
	   symPairs
	 })		
	 
	 
	 val countPerPair = symPairData.map(r => (r, 1)).reduceByKey((v1,v2) => v1+v2).cache
	 
	 val countPerKey = countPerPair.map(r => {
	   val key = Record(r._1.size - 2, r._1)
	   val value = createIntFieldRec(r._2)
	   (key, value)
	 }).reduceByKey((v1,v2) => {
	   val count = v1.getInt(0) + v2.getInt(0)
	   createIntFieldRec(count)
	 }).cache
	 
	 val serBiMap = countPerPair.map(r => {
	   val sz = r._1.size
	   val key = Record(sz - 2, r._1)
	   val value = Record(3, r._1, sz-2, sz)
	   value.addInt(r._2)
	   (key, value)
	 }).union(countPerKey).reduceByKey((r1, r2) => {
	   val netCountRec = if (r1.size == 1) r1 else r2
	   val pairCountRec = if (r1.size == 3) r1 else r2
	   val normCount = pairCountRec.getInt(2).toDouble / netCountRec.getInt(0)
	   val value = Record(3, pairCountRec, 0, 2)
	   value.addDouble(normCount)
	   value
	 }).map(r => {
	   r._1.toString() +  r._2.withFloatPrecision(outputPrecision).toString()
	 })
	 
	 
	 if (debugOn) {
     val records = serBiMap.collect
     records.slice(0, 500).foreach(r => println(r))
   }
	   
	 if(saveOutput) {	   
     serBiMap.saveAsTextFile(outputPath) 
	 }	 

  }

  /**
	* @param distrFilePath
	* @param isHdfsFile
	* @param keyLen
	* @param numBins
	* @return
	*/
  def createEqProbHist(distrFilePath:String, isHdfsFile:Boolean, keyLen:Int, numBins:Int): 
    scala.collection.mutable.Map[Record, Array[Double]] = {
    val fs = if(isHdfsFile) {
			Utility.getFileStream(distrFilePath);
		} else {
			BasicUtils.getFileStream(distrFilePath);
		}
		if (null == fs) {
			BasicUtils.assertFail("distribution file could not be opened at path " + distrFilePath);
		}
		val keyedHist = HistogramStat.createHistograms(fs,  keyLen, false).asScala
		
		val keyedPercentiles = keyedHist.map(r => {
		  val key = Record(r._1)
		  val hist = r._2
		  val percentiles = hist.getAllPercentiles(numBins)
		  (key, percentiles)
		})
		keyedPercentiles
  }
  
  /**
	* @param keyFieldOrdinals
	* @return
	*/  
  def getPercentileIndex(percentiles:Array[Double], value:Double) : Int =  {
    var index = 0
    if (value > percentiles.last) {
      index = percentiles.length + 1
    } else if (value > percentiles(0)){
      breakable {
        for (i <- 0 to percentiles.length - 1) {
          if (value < percentiles(i)) {
            index = i
            break
          }
        }
      }
    }
    
    index
  }
  
}