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
import  org.chombo.spark.common.StreamUtil
import org.hoidla.window.TimeBoundEventLocalityAnalyzer
import org.hoidla.window.EventLocality
import org.chombo.spark.common.Record
import scala.collection.JavaConverters._
import org.apache.spark.SparkContext
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.{State, StateSpec, Time}
import org.hoidla.util.ExplicitlyTimeStampedFlag

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
	   
	   //config params
	   val batchDuration = appConfig.getInt("batch.duration")
	   val strContxt = new StreamingContext(sparkConf, Seconds(batchDuration))
	   val source = config.getString("stream.source")
	   strContxt.checkpoint(appConfig.getString("checkpoint.dir"))
	   val keyFieldOrdinals = appConfig.getIntList("key.field.ordinals").asScala
	   val timeStampFieldOrdinal = appConfig.getInt("timeStamp.field.ordinal")
	   val windowTimeSpan = appConfig.getLong("window.timeSpan")
	   val windowTimeStep = appConfig.getLong("window.timeStep")
	   val minOccurence = appConfig.getInt("window.minOccurence")
	   val maxIntervalAverage: Long = appConfig.getLong("window.maxIntervalAverage")
	   val maxIntervalMax: Long = appConfig.getLong("window.maxIntervalMax")
	   val minRangeLength: Long = appConfig.getLong("window.minRangeLength")
	   val strategies = appConfig.getStringList("clustering.strategies")
	   val anyCond = appConfig.getBoolean("any.cond")
	   val fieldDelimIn = appConfig.getString("field.delim.in")
	     
	   //state update function
	   val  stateUpdateFunction = (entityID: Record, timeStamp: Option[Long], 
	       state: State[TimeBoundEventLocalityAnalyzer]) => {
	         
	     def getWindow() : TimeBoundEventLocalityAnalyzer = {
	       val context = new EventLocality.Context(minOccurence, maxIntervalAverage, maxIntervalMax, 
	           minRangeLength, strategies, anyCond)
	       new TimeBoundEventLocalityAnalyzer(windowTimeSpan, windowTimeStep,  context)
	     }
	     
	     //get window and add new event
	     val window = state.getOption.getOrElse(getWindow)
	     val event = new ExplicitlyTimeStampedFlag(timeStamp.get)
	     window.add(event)
	     state.update(window)
	     
	     val score = window.getScore()
	     val alarm = new Record(2)
	     val alarmOn = score > 0.99
	     alarm.addLong(timeStamp.get).addBoolean(alarmOn)
	     (entityID, alarm)
	   }
	   
	   //extract time stamp
	   val strm = StreamUtil.getKeyedStreamSource(appConfig, strContxt)   
	   val tsStrm = strm.mapValues(v => {
	     val fields = v.split(fieldDelimIn)
	     fields(timeStampFieldOrdinal).toLong
	   })
	   
	   val spec = StateSpec.function(stateUpdateFunction)
	   val mappedStatefulStream = tsStrm.mapWithState(spec)

   }  
}