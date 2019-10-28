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
import org.chombo.spark.common.GeneralUtility
import scala.collection.mutable.ArrayBuffer
import org.chombo.math.MathUtils
import org.hoidla.window.WindowUtils
import org.hoidla.analyze.ChangePointDetection

/**
 * finds change points in a time series 
 * @param args
 * @return
 */
object ChangePointDetector extends JobConfiguration with GeneralUtility {
  
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
	   val quantFldOrd = getMandatoryIntParam(appConfig, "attr.ordinals", "missing quant field ordinal")
	   val numChangePoints =  getMandatoryIntParam(appConfig, "changePoint.num", "missing number of change points")
	   val changePointStrategy = getStringParamOrElse(appConfig, "changePoint.strategy", "window")
	   val windowLen = getConditionalMandatoryIntParam(changePointStrategy.equals("window"), appConfig, 
	       "changePoint.windowLen", "missng window length")
	   val minSegmentLength = getConditionalMandatoryIntParam(changePointStrategy.equals("binarySearch"), appConfig, 
	       "changePoint.minSegmentLength", "missng minimum segment length")
	   val precision = getIntParamOrElse(appConfig, "output.precision", 3)
	   val debugOn = appConfig.getBoolean("debug.on")
	   val saveOutput = appConfig.getBoolean("save.output")
	   
	   //read input
	   val data = sparkCntxt.textFile(inputPath)
	   val changePoints = data.map(line => {
		   val items = BasicUtils.getTrimmedFields(line, fieldDelimIn)
		   getTimeSeriesKeyValue(items, keyFieldOrdinals, seqFieldOrd, quantFldOrd)
	   }).groupByKey.flatMap(r => {
	     val keyRec =  r._1
	     val valuesWithSeq = r._2.toArray.sortWith((v1, v2) => v1.getLong(0) < v2.getLong(0))
	     val values = valuesWithSeq.map(r => r.getDouble(1))	
	     val timeStamps  = valuesWithSeq.map(r => r.getLong(0))	
	     
	     val detector = new ChangePointDetection(values)
	     val changePoints = changePointStrategy match {
	       case "window" => detector.detectByWindow(windowLen/2, numChangePoints)
	       case "binarySearch" => detector.detectByBinarySearch(numChangePoints, minSegmentLength)
	     }
	     
	     changePoints.map(r => {
	       val indx = r.getIndex
	       val timeStamp = timeStamps(indx)
	       val stBld = new StringBuilder(keyRec.toString(fieldDelimOut))
	       stBld.append(fieldDelimOut).append(timeStamp).append(fieldDelimOut).
	         append(BasicUtils.formatDouble(r.getDiscrepancy, precision))
	       stBld.toString()
	     })
	   })
	   
	   if (debugOn) {
         val records = changePoints.collect
         records.slice(0, 50).foreach(r => println(r))
	   }
	   
	   if(saveOutput) {	   
	     changePoints.saveAsTextFile(outputPath) 
	   }	 
	   
   }

}