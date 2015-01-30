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

package org.ruscello.similarity

import scala.collection.JavaConversions._
import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import com.typesafe.config.ConfigFactory
import org.apache.spark.streaming.dstream.PairDStreamFunctions
import org.hoidla.window.SizeBoundWindow
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.SparkContext

case class WindowConfig(windowSize : Int, windowStep : Int, levelThrehold : Int, 
    levelThresholdMargin : Int, levelCrossingCountThreshold : Double, checkingStrategy : String, 
    readingOrdinal : Int)

object LevelSimilarity {
  /**
 * @param args
 */
  def main(args: Array[String]) {
	val Array(master: String, configFile: String) = args.length match {
		case x: Int if x == 2 => args.take(2)
		case _ => throw new IllegalArgumentException("missing command line args")
	}
	
	//load configuration
	System.setProperty("config.file", configFile)
	val config = ConfigFactory.load()
	val batchDuration = config.getInt("batch.duration")
	
	val conf = new SparkConf()
		.setMaster(master)
		.setAppName("LevelSimilarity")
		.set("spark.executor.memory", "1g")
		

	val ssc = new StreamingContext(conf, Seconds(batchDuration))
	//val brConf = ssc.sparkContext.broadcast(config)
	val source = config.getString("stream.source")
	ssc.checkpoint(config.getString("checkpoint.dir"))
	
	//ssc.
	val windowSize = config.getInt("window.size")
	val windowStep = config.getInt("window.step")
	val levelThrehold = config.getInt("level.threshold.value")
	val levelCrossingCountThreshold = config.getDouble("level.crossing.count.threshold")
	val levelThresholdMargin = config.getInt("level.threshold.margin")
	val checkingStrategy = config.getString("checking.strategy")
	val readingOrdinal = config.getInt("reading.ordinal")
	val winConfig = WindowConfig(windowSize, windowStep, levelThrehold, 
			levelThresholdMargin, levelCrossingCountThreshold, checkingStrategy, readingOrdinal)
	val brConf = ssc.sparkContext.broadcast(winConfig)
	
	val sc = ssc.sparkContext
	sc.addJar(config.getString("hoidla.jar"))
	sc.addJar(config.getString("commons.lang.jar"))
	sc.addJar(config.getString("commons.math.jar"))
	
	val updateFunc = (values: Seq[String], state: Option[LevelCheckingWindow]) => {
	  
	  def getWindow() : LevelCheckingWindow = {
	    val conf = brConf.value
	    val windowSize = conf.windowSize
	    val windowStep = conf.windowStep
	    val levelThrehold = conf.levelThrehold
	    val levelThresholdMargin = conf.levelThresholdMargin
	    val levelCrossingCountThreshold = conf.levelCrossingCountThreshold
	    val checkingStrategy = conf.checkingStrategy
	      
	    new LevelCheckingWindow(windowSize, windowStep, levelThrehold, 
	    		levelThresholdMargin, levelCrossingCountThreshold, checkingStrategy)
	  }
	  
	  def extractReading(line : String) : Int = {
	    val items = line.split(",")
	    val readingOrdinal = brConf.value.readingOrdinal
	    Integer.parseInt(items(readingOrdinal))
	  }
	  
	  val window = state.getOrElse(getWindow)
	  values.foreach(l => {
	    window.addAndCheck(extractReading(l))
	  	}
	  )
	  Some(window)		 
	}
	
	//ssc.sparkContext.
	val idOrdinal = config.getInt("id.ordinal")
	source match {
	  case "hdfs" => {
	    val path = config.getString("hdfs.path")
	    val lines = ssc.textFileStream(path)
	    
	    //map with id as the key
	    val keyedLines =  getKeyedLines(lines, idOrdinal)
	    val pairStream = new PairDStreamFunctions(keyedLines)
	    val stateStrem = pairStream.updateStateByKey(updateFunc)
	  }
	  
	  case "socketText" => {
	    val host = config.getString("socket.receiver.host")
	    val port = config.getInt("socket.receiver.port")
	    val lines = ssc.socketTextStream(host, port, StorageLevel.MEMORY_AND_DISK_SER_2)
	    
	    /*
	    lines.foreach(lr => {
	      lr.foreach(l => {
	        println(l)
	      }
	      )
	    }
	    )
	    */
	    
	    //map with id as the key
	    /*
	    val keyedLines =  getKeyedLines(lines, idOrdinal)
	    val pairStream = new PairDStreamFunctions(keyedLines)
	    val stateStream = pairStream.updateStateByKey(updateFunc)
	    stateStream.count.print
	    */
	    
	    
	    val keyedLines =  getKeyedLines(lines, idOrdinal)
	    /*
	    keyedLines.foreach(kls => {
	      kls.foreach(kl => {
	        println(kl._1)
	      })
	      
	    })
	    */
	    
	    val pairStream = new PairDStreamFunctions(keyedLines)
	    val stateStream = pairStream.updateStateByKey(updateFunc)
	    stateStream.foreach(ssrdd => {
	      ssrdd.foreach(ss => {
	        val res = ss._2
	        println("device:" + ss._1 + " num violations:" + res.numViolations)
	      })
	    })
	    
	  }
	  case "kafka" => {
	    
	  }
	  
	  case _ => {
	    throw new IllegalArgumentException("unsupported input stream source")
	  }
	}

	
	// start our streaming context and wait for it to "finish"
	ssc.start()
	
	// Wait for 10 seconds then exit. To run forever call without a timeout
	val duration = config.getInt("run.duration")
	println("duration: "+ duration)
	//ssc.awaitTermination(duration * 1000)
	ssc.awaitTermination()
	//ssc.stop()	
  }

  private def getKeyedLines(lines : DStream[String], idOrdinal : Int ) : DStream[(String, String)] = {
    val keyed = lines.map { line =>
		val items = line.split(",")
	    val id = items(idOrdinal)
	    (id, line)
	}
    keyed
  }
	
}