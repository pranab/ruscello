/*
 * ruscello: real time time series analytic  on big data streaming platform
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

package org.ruscello.event

import org.chombo.spark.common.JobConfiguration
import org.chombo.spark.common.Record
import scala.collection.JavaConverters._
import org.apache.spark.SparkContext
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds

/**
* Converts event cluster or burst of events  in a short time to one events, to
* prevent flooding of alarms
* 
*/
object EventCluster extends JobConfiguration {

   def main(args: Array[String]) {
	   val appName = "eventCluster"
	   val Array(inputPath: String, outputPath: String, configFile: String) = getCommandLineArgs(args, 3)
	   val config = createConfig(configFile)
	   val sparkConf = createSparkConf(appName, config, false)
	   val sparkCntxt = new SparkContext(sparkConf)
	   val appConfig = config.getConfig(appName)
	   
	   val batchDuration = appConfig.getInt("batch.duration")
	   val ssc = new StreamingContext(sparkConf, Seconds(batchDuration))
	   val source = config.getString("stream.source")
	   ssc.checkpoint(appConfig.getString("checkpoint.dir"))
	   val keyFieldOrdinals = appConfig.getIntList("key.field.ordinals").asScala
	   val timeStampFieldOrdinal = appConfig.getInt("timeStamp.field.ordinal")

	   
   }  
}