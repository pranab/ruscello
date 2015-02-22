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
import org.apache.spark.streaming.kafka.KafkaUtils
import kafka.serializer.StringDecoder

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
	  //HDFS files as stream source
	  case "hdfs" => {
	    val path = config.getString("hdfs.path")
	    val lines = ssc.textFileStream(path)
	    val stateStrem = getStateStream(lines, idOrdinal, updateFunc)
	  }
	  
	  case "socketText" => {
	    //socket server as stream source
	    val host = config.getString("socket.receiver.host")
	    val port = config.getInt("socket.receiver.port")
	    val lines = ssc.socketTextStream(host, port, StorageLevel.MEMORY_AND_DISK_SER_2)
	    val stateStream = getStateStream(lines, idOrdinal, updateFunc)
	    
	    stateStream.foreach(ssrdd => {
	      ssrdd.foreach(ss => {
	        val res = ss._2
	        println("device:" + ss._1 + " num violations:" + res.numViolations)
	      })
	    })
	    
	  }
	  
	  case "kafka" => {
	    //kafka as stream source 
	    val zooKeeperServerLList = config.getString("zookeeper.connect")
	    val zooKeeperSessTmOut = config.getString("zookeeper.session.timeout.ms")
	    val zooKeeperSyncTime = config.getString("zookeeper.sync.time.ms")
	    
	    
	    val consumerGroupId = config.getString("kafka.consumer.group.id")
	    val topic = config.getString("kafka.topic")
	    val numDStreams = config.getInt("kafka.num.input.stream")
	    val autoCommitInterval = config.getString("auto.commit.interval.ms")
	    
	    val kafkaParams: Map[String, String] = Map(
	        "zookeeper.connect" -> zooKeeperServerLList,
	        "zookeeper.session.timeout.ms" -> zooKeeperSessTmOut,
	        "zookeeper.sync.time.ms" -> zooKeeperSyncTime,
	        "auto.commit.interval.ms" -> autoCommitInterval,
	        "group.id" -> "consumerGroupId"
	    )
	    val topics = Map(topic -> 1)
	    val kafkaDStreams = (1 to numDStreams).map { _ =>
	    	KafkaUtils.createStream[String, String, StringDecoder, StringDecoder](
	    	    ssc, kafkaParams, topics, StorageLevel.MEMORY_AND_DISK)
	    }
	    
	    val partitonedBySensor = config.getBoolean("partitoned.by.sensor")
	    partitonedBySensor match {
	      case true => {
	        //partitioned by sensor, each stream may handle one or partitions
	        kafkaDStreams.foreach(str => {
	    	  val lines = str.map { v => v._2 }
	          val stateStrem = getStateStream(lines, idOrdinal, updateFunc)
	          
	        })
	      }
	      case false => {
	    	  //not partitioned by sensor
	    	  if (numDStreams != 1) {
	    	    throw new IllegalArgumentException(
	    	        "when not partitoned by sensor, there should be only 1 partition");
	    	  }
	    	  val unifiedStream = ssc.union(kafkaDStreams)
	    	  val lines = unifiedStream.map { v => v._2 }
	          val stateStrem = getStateStream(lines, idOrdinal, updateFunc)
	      }
	    }
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

  private def getStateStream(lines : DStream[String], idOrdinal : Int, 
      updateFunc: (Seq[String], Option[LevelCheckingWindow]) => Some[LevelCheckingWindow]) : DStream[(String, LevelCheckingWindow)] = {
	  //map with id as the key
	  val keyedLines =  getKeyedLines(lines, idOrdinal)
	  //printKeyedInput(keyedLines)
	  val pairStream = new PairDStreamFunctions(keyedLines)
	  val stateStream = pairStream.updateStateByKey(updateFunc)
      stateStream
  }
  
  private def getKeyedLines(lines : DStream[String], idOrdinal : Int ) : DStream[(String, String)] = {
    val keyed = lines.map { line =>
		val items = line.split(",")
	    val id = items(idOrdinal)
	    (id, line)
	}
    keyed
  }
	
  private def printInput(lines : DStream[String]) {
    lines.foreach(lr => {
	      lr.foreach(l => {
	        println(l)
	      }
	      )
	})    
  }
  
  private def printKeyedInput(keyedLines : DStream[(String, String)]) {
    keyedLines.foreach(kls => {
	      kls.foreach(kl => {
	        println(kl._1)
	      })
	      
	})
  }
}