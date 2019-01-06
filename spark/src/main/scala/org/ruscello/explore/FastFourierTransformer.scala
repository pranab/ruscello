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
import org.chombo.spark.common.Record
import org.chombo.util.BasicUtils
import scala.collection.mutable.ArrayBuffer
import org.chombo.spark.common.GeneralUtility
import org.chombo.math.MathUtils
import org.chombo.math.Complex
import org.hoidla.analyze.FastFourierTransform

/**
 * FFT analysis
 * @param args
 * @return
 */
object FastFourierTransformer extends JobConfiguration with GeneralUtility {
  
   /**
    * @param args
    * @return
    */
   def main(args: Array[String]) {
	   val appName = "fastFourierTransformer"
	   val Array(inputPath: String, outputPath: String, configFile: String) = getCommandLineArgs(args, 3)
	   val config = createConfig(configFile)
	   val sparkConf = createSparkConf(appName, config, false)
	   val sparkCntxt = new SparkContext(sparkConf)
	   val appConfig = config.getConfig(appName)
	   
	   //configurations
	   val fieldDelimIn = getStringParamOrElse(appConfig, "field.delim.in", ",")
	   val fieldDelimOut = getStringParamOrElse(appConfig, "field.delim.out", ",")
	   val attrOrdinals = getMandatoryIntListParam(appConfig, "attr.ordinals").asScala.toArray
	   val keyFields = getOptionalIntListParam(appConfig, "id.fieldOrdinals")
	   val keyFieldOrdinals = toOptionalIntArray(keyFields)
	   val timeStampFieldOrdinal = getMandatoryIntParam(appConfig, "time.fieldOrdinal", 
	       "missing time stamp field ordinal")
	   val keyLen = getOptinalArrayLength(keyFieldOrdinals, 1)
	   val samplingFreq = 1.0 / getMandatoryDoubleParam(appConfig, "samplig.interval", "missing sampling interval")
	   val outputPrecision = getIntParamOrElse(appConfig, "output.precision", 3)
	   val numFreqOutput = getIntParamOrElse(appConfig, "num.FreqOutput", 16)
	   
	   val debugOn = getBooleanParamOrElse(appConfig, "debug.on", false)
	   val saveOutput = getBooleanParamOrElse(appConfig, "save.output", true)

	   //input
	   val data = sparkCntxt.textFile(inputPath)
	   
	   val fftOutput = data.flatMap(line => {
		   val fields = BasicUtils.getTrimmedFields(line, fieldDelimIn)
		   val ts = fields(timeStampFieldOrdinal).toLong
		   val baseKey =  Record(keyLen)
		   populateFields(fields, keyFieldOrdinals, baseKey, "all")
		   
		   attrOrdinals.map(i => {
		     val key = Record(keyLen + 1, baseKey)
		     key.addInt(i)
		     val data = fields(i).toDouble
		     (key,  (ts, data))
		   })
	   }).groupByKey.flatMap(r => {
	     val key = r._1
	     val va = r._2.toArray.sortBy(v => v._1)
	     val newLength = MathUtils.binaryPowerFloor(va.length)
	     val half = newLength / 2
	     val freqDelta = samplingFreq / half
	     
	     //run fft, need first n/2 + 1 element from output
	     val fftInput = va.map(v => v._2).slice(0, newLength).map(v => new Complex(v, 0))
	     val fftOutput = FastFourierTransform.fft(fftInput).slice(0, numFreqOutput)
	     val amplitudes = fftOutput.map(v => v.abs())
	     amplitudes.zipWithIndex.map(v => {
	       val res = (v._2 * freqDelta, v._1)
	       (key, res)
	     })
	   }).sortByKey(true, 1)
	   
	   val formattedOutput = fftOutput.map(r => {
	     r._1.toString + fieldDelimOut + BasicUtils.formatDouble(r._2._1, outputPrecision) + 
	     	fieldDelimOut + BasicUtils.formatDouble(r._2._2, outputPrecision)
	   })
	   
	  if (debugOn) {
	     formattedOutput.collect.slice(0,100).foreach(s => println(s))
	  }
	   
	  if (saveOutput) {
	     formattedOutput.saveAsTextFile(outputPath)
	  }
	   
   }

}