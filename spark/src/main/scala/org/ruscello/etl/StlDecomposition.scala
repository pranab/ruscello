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
 * decomposes time series into trend, deasonality and remainder with STL algorithm
 * @param args
 * @return
 */
object StlDecomposition extends JobConfiguration with GeneralUtility {
  
   /**
    * @param args
    * @return
    */
   def main(args: Array[String]) {
	   val appName = "stlDecomposition"
	   val Array(inputPath: String, outputPath: String, configFile: String) = getCommandLineArgs(args, 3)
	   val config = createConfig(configFile)
	   val sparkConf = createSparkConf(appName, config, false)
	   val sparkCntxt = new SparkContext(sparkConf)
	   val appConfig = config.getConfig(appName)
	   
	   //configurations
	   val fieldDelimIn = getStringParamOrElse(appConfig, "field.delim.in", ",")
	   val fieldDelimOut = getStringParamOrElse(appConfig, "field.delim.out", ",")
	   val keyFieldOrdinals = toIntArray(getMandatoryIntListParam(appConfig, "id.field.ordinals"))
	   val seqFieldOrd = getMandatoryIntParam(appConfig, "seq.field.ordinal","missing sequence field ordinal") 
	   val quantFldOrd = getMandatoryIntParam(appConfig, "attr.ordinals")
	   val outerIterCount = getIntParamOrElse(appConfig, "iterCount.outer", 0)
	   val innerIterCount = getIntParamOrElse(appConfig, "iterCount.inner", 1)
	   val seasonalPeriod = getMandatoryIntParam(appConfig, "seasonal.period", "missing seasonal period")
	   val levelLoessSize = getMandatoryIntParam(appConfig, "level.loessSize", "missing level loess size")
	   val seasonalLoessSize = getMandatoryIntParam(appConfig, "seasonal.loessSize", "missing seasonal loess size")
	   val trendLoessSize = getMandatoryIntParam(appConfig, "trend.loessSize", "missing trend loess size")
	   val precision = getIntParamOrElse(appConfig, "output.precision", 3)
	   val debugOn = appConfig.getBoolean("debug.on")
	   val saveOutput = appConfig.getBoolean("save.output")
	   
	   val keyLen = keyFieldOrdinals.length

	   //read input
	   val data = sparkCntxt.textFile(inputPath)
	   val decomposeddData = data.map(line => {
		   val items = BasicUtils.getTrimmedFields(line, fieldDelimIn)
		   val keyRec = Record(items, keyFieldOrdinals)
		   val valRec = Record(2)
		   valRec.addLong(items(seqFieldOrd).toLong)
		   valRec.addDouble(items(quantFldOrd).toDouble)
		   (keyRec, valRec)
	   }).groupByKey.flatMap(r => {
	     val keyRec =  r._1
	     val valuesWithSeq = r._2.toArray.sortWith((v1, v2) => v1.getLong(0) < v2.getLong(0))
	     val values = valuesWithSeq.map(r => r.getDouble(1))
	     val size = values.length
	     var trendValues = Array[Double]()
	     var seasonalValues = Array[Double]()
	     var detrendedValues = values
	     
	     for (ou <- 0 to outerIterCount) {
	       for (in <- 0 to innerIterCount) {
		     val seasonalSubSeq = Array.ofDim[Array[Double]](seasonalPeriod)
		     if (debugOn) {
		       println("seasonalSubSeq length " + seasonalSubSeq.length)
		     }
		     
		     //values for all seasonal index
		     for (i <- 0 to seasonalPeriod-1) {
		       val subSeq = ArrayBuffer[Double]()
		       for (j <- i to (detrendedValues.length-1) by seasonalPeriod) {
		         subSeq += detrendedValues(j)
		       }
		       
		       //smooth it
		       val subSeqArr = subSeq.toArray
		       if (debugOn) {
		         println("subseq length " + subSeqArr.length)
		       }
		       MathUtils.loessSmooth(subSeqArr, seasonalLoessSize)
		       
		       seasonalSubSeq(i) = subSeqArr
		     }
		     
		     //sanity check
		     val totLength = seasonalSubSeq.map(a => a.length).reduce((l1,l2) => l1 + l2)
		     BasicUtils.assertCondition(totLength == size, "seasonal sub sequence total length does not match")
		     
		     //reconstruct from seasonal sub series
		     val smoothedValues = new Array[Double](size)
		     var vi = 0
		     var i = 0
		     while (vi < size) {
		       seasonalSubSeq.foreach(a => {
		    	 if (i < a.length) {
		    	   //some  sub sequences may be of shorter length
		    	   smoothedValues(vi) = a(i)
		    	   vi += 1
		    	 }
		       })
		       i += 1
		     }
		     BasicUtils.assertCondition(vi == size, "series reconstruction issue final index " + vi + " size " + size)
		     
		     //pad cycle at each end
		     var levelValues = new Array[Double](size + 2 * seasonalPeriod)
		     Array.copy(smoothedValues, 0, levelValues, seasonalPeriod, size)
		     Array.copy(smoothedValues, 0, levelValues, 0, seasonalPeriod)
		     Array.copy(smoothedValues, size - seasonalPeriod, levelValues, size + seasonalPeriod, seasonalPeriod)
		     
		     //level with lp filter and smoothing
		     levelValues = WindowUtils.lowPassFilter(levelValues, seasonalPeriod)
		     val leOne = levelValues.length
		     levelValues = WindowUtils.lowPassFilter(levelValues, seasonalPeriod)
		     val leTwo = levelValues.length
		     levelValues = WindowUtils.lowPassFilter(levelValues, 3)
		     val leThree = levelValues.length
		     
		     MathUtils.loessSmooth(levelValues, levelLoessSize)
		     BasicUtils.assertCondition(leThree == size, "level data size " + leThree + 
		         " does not match with original " + size + " length after filters " + leOne + " " + leTwo + " " + leThree)
		     
		     //seasonal
		     seasonalValues = MathUtils.subtractVector(smoothedValues, levelValues)
		     
		     //trend
		     trendValues = MathUtils.subtractVector(values, seasonalValues)
		     MathUtils.loessSmooth(trendValues, trendLoessSize)
		     
		     //dtrended values
		     detrendedValues = MathUtils.subtractVector(values, trendValues)
	       }
	     }
	     val remainder = MathUtils.subtractVector(detrendedValues, seasonalValues)
	     
	     (0 to size-1).map(i => {
	       val stBld = new StringBuilder(keyRec.toString(fieldDelimOut))
	       stBld.append(fieldDelimOut).append(valuesWithSeq(i).getLong(0)).append(fieldDelimOut).
	         append(BasicUtils.formatDouble(values(i), precision)).append(fieldDelimOut).
	         append(BasicUtils.formatDouble(trendValues(i), precision)).append(fieldDelimOut).
	         append(BasicUtils.formatDouble(seasonalValues(i), precision)).append(fieldDelimOut).
	         append(BasicUtils.formatDouble(remainder(i), precision))
	       stBld.toString
	     })
	   })
	   if (debugOn) {
         val records = decomposeddData.collect
         records.slice(0, 50).foreach(r => println(r))
	   }
	   
	   if(saveOutput) {	   
	     decomposeddData.saveAsTextFile(outputPath) 
	   }	 
	   
   }
}