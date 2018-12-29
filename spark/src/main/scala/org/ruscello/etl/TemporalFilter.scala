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
import org.chombo.util.SeasonalAnalyzer
import org.chombo.spark.common.SeasonalUtility
import org.chombo.util.BasicUtils

/**
 * Temporal filtering. Return records that are within all seasonal cycles
 * @author pranab
 */
object TemporalFilter extends JobConfiguration with SeasonalUtility {
  
   /**
    * @param args
    * @return
    */
   def main(args: Array[String]) {
	   val appName = "autoCorrelation"
	   val Array(inputPath: String, outputPath: String, configFile: String) = getCommandLineArgs(args, 3)
	   val config = createConfig(configFile)
	   val sparkConf = createSparkConf(appName, config, false)
	   val sparkCntxt = new SparkContext(sparkConf)
	   val appConfig = config.getConfig(appName)
	   
	   //configurations
	   val fieldDelimIn = getStringParamOrElse(appConfig, "field.delim.in", ",")
	   val fieldDelimOut = getStringParamOrElse(appConfig, "field.delim.out", ",")
	   val timeStampFieldOrd = getMandatoryIntParam(appConfig, "time.fieldOrdinal", "missing sequetimence filed ordinal")
	   val seasonalities = getMandatoryStringListParam(appConfig, "seasonality.list", "missing seasonality list").asScala.toArray
	   val timeStampInMili = getBooleanParamOrElse(appConfig, "time.inMili", true)
	   val seasonalAnalyzers = seasonalities.map(s => {
	     createSeasonalAnalyzer(this, appConfig,s, 0, timeStampInMili)
	   })
	   val debugOn = getBooleanParamOrElse(appConfig, "debug.on", false)
	   val saveOutput = getBooleanParamOrElse(appConfig, "save.output", true)

	   val data = sparkCntxt.textFile(inputPath)
	   val filtData = data.filter(line => {
		   val items = BasicUtils.getTrimmedFields(line, fieldDelimIn)
		   val timeStamp = items(timeStampFieldOrd).toLong
		   
		   //must be within all seasonal cycles
		   val toInclude = seasonalAnalyzers.find(sa => !sa.withinSeasonalCycle(timeStamp)) match {
		     case Some(sa) => false
		     case None => true
		   }
		   toInclude
	   })

	  if (debugOn) {
	     filtData.collect.slice(0,100).foreach(s => println(s))
	  }
	   
	  if (saveOutput) {
	     filtData.saveAsTextFile(outputPath)
	  }

   }

}