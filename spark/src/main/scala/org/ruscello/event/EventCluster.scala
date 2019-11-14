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
	   val Array(outputPath: String, configFile: String) = getCommandLineArgs(args, 2)
	   val config = createConfig(configFile)
	   val sparkConf = createSparkConf(appName, config, false)
	   sparkConf.set("spark.executor.memory", "1g")
	   val appConfig = config.getConfig(appName)
	   
	   //config params
	   val genConfig = appConfig.getConfig("general")
	   val fieldConfig = appConfig.getConfig("field")
	   val windowConfig = appConfig.getConfig("window")
	   val sourceConfig = appConfig.getConfig("source")
	   val clustConfig = appConfig.getConfig("clustering")
	   
	   val batchDuration = genConfig.getInt("batch.duration")
	   val strContxt = new StreamingContext(sparkConf, Seconds(batchDuration))
	   val source = genConfig.getString("stream.source")
	   strContxt.checkpoint(genConfig.getString("checkpoint.dir"))
	   val keyFieldOrdinals = fieldConfig.getIntList("key.ordinals").asScala
	   val timeStampFieldOrdinal = fieldConfig.getInt("timeStamp.ordinal")
	   val windowTimeSpan = windowConfig.getLong("timeSpan")
	   val windowTimeStep = windowConfig.getLong("timeStep")
	   val minOccurence = windowConfig.getInt("minOccurence")
	   val maxIntervalAverage: Long = windowConfig.getLong("maxIntervalAverage")
	   val findClusterWithin = windowConfig.getBoolean("findClusterWithin")
	   val minClusterSize = windowConfig.getLong("minClusterSize")
	   val maxIntervalMax = windowConfig.getLong("maxIntervalMax")
	   val minRangeLength = windowConfig.getLong("minRangeLength")
	   val strategies = clustConfig.getStringList("strategies")
	   val anyCond = clustConfig.getBoolean("any.cond")
	   val fieldDelimIn = fieldConfig.getString("delim.in")
	   val scoreMinThreshold = windowConfig.getDouble("scoreMinThreshold")
	   val duration = genConfig.getInt("run.duration")
	   val debugOn = genConfig.getBoolean("debug.on")
	   val saveOutput = genConfig.getBoolean("save.output")
	   val minEventTimeInterval = windowConfig.getLong("minEventTimeInterval")
	     
	   //state update function
	   val  stateUpdateFunction = (entityID: Record, timeStamp: Option[Long], 
	       state: State[TimeBoundEventLocalityAnalyzer]) => {
	         
	     def getWindow() : TimeBoundEventLocalityAnalyzer = {
	       if (false)
	         println("*** creating window")
	         
	       val context = new EventLocality.Context(minOccurence, maxIntervalAverage, findClusterWithin, minClusterSize, 
	           maxIntervalMax, minRangeLength, strategies, anyCond)
	       new TimeBoundEventLocalityAnalyzer(windowTimeSpan, windowTimeStep,  minEventTimeInterval, 
	           scoreMinThreshold, context)
	     }
	     
	     //get window and add new event
	     val window = state.getOption.getOrElse(getWindow)
	     val event = new ExplicitlyTimeStampedFlag(timeStamp.get)
	     window.add(event)
	     state.update(window)
	     
	     val alarmOn = window.isTriggered()
	     val result = (entityID, timeStamp.get, alarmOn)
	     if (debugOn && result._3) {
	       println(result)
	     }
	     result
	   }
	   
	   //extract time stamp
	   val strm = StreamUtil.getKeyedStreamSource(appConfig, strContxt)   
	   val tsStrm = strm.mapValues(v => {
	     val fields = v.split(fieldDelimIn)
	     fields(timeStampFieldOrdinal).toLong
	   })
	   
	   if (false) {
	     tsStrm.foreachRDD(rdd => {
	       val count = rdd.count
	       println("*** num of records: " + count)
	       rdd.foreach(r => {
	         println("*** key: " + r._1 + " timestamp: " + r._2 )
	       })
	     })
	   }
	   
	   
	   val spec = StateSpec.function(stateUpdateFunction)
	   val mappedStatefulStream = tsStrm.mapWithState(spec)
	   
	   if (false) {
	     println("*** state stream")
	     mappedStatefulStream.foreachRDD(rdd => {
	       rdd.foreach(r => {
	         println("*** key: " + r._1 + " timestamp: " + r._2 + "alarm: " + r._3)
	       })
	     })
	   }
	 
	   //alarm stream
	   val alarmStream = mappedStatefulStream.filter(r => {
	     r._3
	   })

	   if (false) {
	     println("*** alarms" )
	     alarmStream.foreachRDD(rdd => {
	       rdd.foreach(r => {
	         println(r)
	       })
	     })
	   }	
	   
	   //output
	   if (saveOutput)
		   alarmStream.saveAsTextFiles(outputPath)
	   
	   // start our streaming context and wait for it to "finish"
	   strContxt.start()
	   
	   // Wait and then exit. To run forever call without a timeout
	   if (duration > 0) {
		   strContxt.awaitTermination()
	   } else  {
		   strContxt.awaitTermination()
	   }
   }  
}