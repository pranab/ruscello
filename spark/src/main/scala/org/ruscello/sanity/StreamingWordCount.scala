package org.ruscello.sanity

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds

object StreamingWordCount {
	def main(args: Array[String]) {
		if (args.length < 2) {
			System.err.println("Usage StreamingWordCount <master> <output>")
		}
		val Array(inputDir, output) = args.take(2)
		val conf = new SparkConf().setAppName("BasicStreamingExample")
		val ssc = new StreamingContext(conf, Seconds(30))
		val lines = ssc.textFileStream(inputDir)
		val words = lines.flatMap(_.split(" "))
		val wc = words.map(x => (x, 1))
		wc.foreachRDD(rdd => {
				val counts = rdd.reduceByKey((x, y) => x + y)
				counts.saveAsTextFile(output)
				val collectedCounts = counts.collect
				collectedCounts.foreach(c => println(c))
			}
		)
		
		println("StreamingWordCount: sscstart")
		ssc.start()
		println("StreamingWordCount: awaittermination")
		ssc.awaitTermination()
		println("StreamingWordCount: done!")
	}
}