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
import org.chombo.spark.common.Record
import org.chombo.util.BasicUtils
import org.chombo.spark.common.GeneralUtility
import scala.collection.mutable.ArrayBuffer
import org.chombo.math.MathUtils
import org.hoidla.window.WindowUtils


/**
* extracts features from spikey data i.e gap, spike value, spike width
* @param args
* @return
*/
object SpikeFeatureExtractor extends JobConfiguration with GeneralUtility {
  
   /**
    * @param args
    * @return
    */
   def main(args: Array[String]) {
	   val appName = "spikeFeatureExtractor"
	   val Array(inputPath: String, outputPath: String, configFile: String) = getCommandLineArgs(args, 3)
	   val config = createConfig(configFile)
	   val sparkConf = createSparkConf(appName, config, false)
	   val sparkCntxt = new SparkContext(sparkConf)
	   val appConfig = config.getConfig(appName)
	   
	   //configurations
	   val fieldDelimIn = getStringParamOrElse(appConfig, "field.delim.in", ",")
	   val fieldDelimOut = getStringParamOrElse(appConfig, "field.delim.out", ",")
	   val keyFieldOrdinals = toOptionalIntArray(getOptionalIntListParam(appConfig, "id.fieldOrdinals"))
	   val seqFieldOrd = getMandatoryIntParam(appConfig, "seq.field.ordinal","missing sequence field ordinal") 
	   val diffMax = getDoubleParamOrElse(appConfig, "diff.max", .01)
	   val attrOrds = BasicUtils.fromListToIntArray(getMandatoryIntListParam(appConfig, "attr.ordinals"))
	   val keyLen = getOptinalArrayLength(keyFieldOrdinals, 1)
	   val debugOn = appConfig.getBoolean("debug.on")
	   val saveOutput = appConfig.getBoolean("save.output")
	   
	   //input
	   var data = sparkCntxt.textFile(inputPath)
	   
	   //keyed data
	   val keyedData =  getKeyedValueWithSeq(data, fieldDelimIn, keyLen, keyFieldOrdinals, seqFieldOrd)

	   val spikeData = keyedData.groupByKey.flatMap(v => {
	     val key = v._1
	     val keyStr = key.toString
	     val values = v._2.toList.sortBy(v => v.getLong(0))
	     val size = values.length
	     
	     val atSpikeValues = attrOrds.flatMap(a => {
  	     val line = values(0).getString(1)
  	     val items = BasicUtils.getTrimmedFields(line, fieldDelimIn)
 	       var prQuant = items(a).toDouble
 	       val seq = items(seqFieldOrd).toLong 	
	       var prPeakSeq = seq	
	       var prSeq = seq
 	       var inSpike = false
	       var peakValue = 0.0
	       var peakSeq:Long = 0
	       var spikeBegSeq:Long = 0
	       
	       val spikeValues = ArrayBuffer[(Record, Record)]()
	       
  	     for (i <- 1 to size-1) {
  	       val line = values(i).getString(1)
  	       val items = BasicUtils.getTrimmedFields(line, fieldDelimIn)
 	         val quant = items(a).toDouble
 	         val seq = items(seqFieldOrd).toLong 	  
 	         val diff = Math.abs(quant - prQuant)
 	         
 	         if (diff > diffMax) {
 	           if (!inSpike) {
 	             //enter spike
 	             peakValue = quant
 	             peakSeq = seq
 	             spikeBegSeq = prSeq
 	             inSpike = true
 	           } else if (quant > peakValue) {
 	             peakValue = quant
 	             peakSeq = seq
 	           }
 	         } else {
 	           if (inSpike) {
 	             //create spike data
 	             val spKey = Record(key)
 	             val spVal = Record(4)
 	             spVal.addInt(a)
 	             spVal.addLong(peakSeq)
 	             spVal.addInt((peakSeq - prPeakSeq).toInt)
 	             spVal.addInt((prSeq - spikeBegSeq).toInt)
 	             spVal.addDouble(peakValue)
 	             val sp = (spKey, spVal)
 	             spikeValues += sp
 	             
 	             //enter base line
 	             inSpike = false
 	             prPeakSeq = peakSeq
 	           } 	           
 	         }
  	       prQuant = quant 	  
  	       prSeq = seq
  	     }
  	     
  	     spikeValues
	     })
	     atSpikeValues
	   }).map(r =>  r._1.toString(fieldDelimOut) + fieldDelimOut + r._2.toString(fieldDelimOut))	   
	   
	   if (debugOn) {
         val records = spikeData.collect
         records.slice(0, 20).foreach(r => println(r))
     }
	   
	   if(saveOutput) {	   
	     spikeData.saveAsTextFile(outputPath) 
	   }	 
	   
	   
	   
   }
  
}