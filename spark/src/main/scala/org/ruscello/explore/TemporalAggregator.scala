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
import org.chombo.util.BasicUtils
import org.chombo.util.SeasonalAnalyzer
import com.typesafe.config.Config
import org.chombo.spark.common.Record

/**
 * @author pranab
 *
 */
object TemporalAggregator extends JobConfiguration {
   
  /**
   * @param args
   * @return
   */
   def main(args: Array[String]) {
	   val appName = "temporalAggregator"
	   val Array(inputPath: String, outputPath: String, configFile: String) = getCommandLineArgs(args, 3)
	   val config = createConfig(configFile)
	   val sparkConf = createSparkConf(appName, config, false)
	   val sparkCntxt = new SparkContext(sparkConf)
	   val appConfig = config.getConfig(appName)
	   
	   //configuration params
	   val fieldDelimIn = getStringParamOrElse(appConfig, "field.delim.in", ",")
	   val fieldDelimOut = getStringParamOrElse(appConfig, "field.delim.out", ",")
	   val attrOrdinals = getMandatoryIntListParam(appConfig, "attr.ordinals").asScala.toArray
	   val keyFields = getOptionalIntListParam(appConfig, "id.fieldOrdinals")
	   val keyFieldOrdinals = keyFields match {
	     case Some(fields:java.util.List[Integer]) => Some(fields.asScala.toArray)
	     case None => None  
	   }
	   val timeStampFieldOrdinal = getMandatoryIntParam(appConfig, "time.fieldOrdinal", 
	       "missing time stamp field ordinal")
	   val timeStampInMs = this.getBooleanParamOrElse(appConfig, "time.inMili", true)
	   val aggrWindowTimeUnit = getMandatoryStringParam(appConfig, "aggr.windowTimeUnit", 
	       "missing aggr window time unit")
	   val aggrWindowTimeLength = getMandatoryIntParam(appConfig, "aggr.windowTimeLength", 
	       "missing aggr window time length")
	   val timeWindow = 
	   if (timeStampInMs) {
	     BasicUtils.toEpochTime(aggrWindowTimeUnit) * aggrWindowTimeLength
	   } else {
	     BasicUtils.toEpochTime(aggrWindowTimeUnit) * aggrWindowTimeLength / 1000
	   }
	   val aggrType = getStringParamOrElse(appConfig, "aggr.type", "average") 
	   val outputCompact = getBooleanParamOrElse(appConfig, "output.compact", true)
	   val outputPrecision = this.getIntParamOrElse(appConfig, "output.precision", 3)
	   
	  //key length
	  var keyLen = 0
	  keyFieldOrdinals match {
	    case Some(fields : Array[Integer]) => keyLen +=  fields.length
	    case None => 
	  }
	  keyLen += 2
	  
	  val debugOn = getBooleanParamOrElse(appConfig, "debug.on", false)
	  val saveOutput = getBooleanParamOrElse(appConfig, "save.output", true)
	  
	   //input
	  val data = sparkCntxt.textFile(inputPath)	  

	  //key by id, ts, field ord
	  val keyedData = data.flatMap(line => {
		   val fields = BasicUtils.getTrimmedFields(line, fieldDelimIn)
		   val ts = fields(timeStampFieldOrdinal).toLong
		   val tsPart = (ts / timeWindow) * timeWindow
		   
		   val recs = attrOrdinals.map(fld => {
			   val key = Record(keyLen)
			   Record.populateFields(fields, keyFieldOrdinals, key)
			   key.addLong(tsPart)
			   key.addInt(fld)
			   
			   val fieldVal = fields(fld).toDouble
			   val value = Record(2)
			   if (aggrType.equals("minMax")) {
			     value.add(fieldVal, fieldVal)
			   } else {
			     val count = 1
			     value.add(count, fieldVal)
			   }
			   (key, value)
		   })
		   
		   recs
	  })
	  
	  
	  //reduce
	  val redData = if (aggrType.equals("minMax")) {
	    keyedData.reduceByKey((v1,v2) => {
	      val min = if (v1.getDouble(0) < v2.getDouble(0)) v1.getDouble(0) else v2.getDouble(0)
	      val max = if (v1.getDouble(1) > v2.getDouble(1)) v1.getDouble(1) else v2.getDouble(1)
	      val newVal = Record(2)
	      newVal.add(min, max)
	      newVal
	    })
	  } else {
	    keyedData.reduceByKey((v1,v2) => {
	      val count = v1.getInt(0) + v2.getInt(0)
	      val sum = v1.getDouble(1) + v2.getDouble(1)
	      val newVal = Record(2)
	      newVal.add(count, sum)
	      newVal
	    })
	  }
	  
	  //aggregate
	  val aggrData = redData.mapValues(v => {
	    val value =  aggrType match {
	      case "count" => {
	        val value = Record(1)
	        value.addInt(v.getInt(0))
	        value
	      }
	      case "sum" =>  {
	        val value = Record(1)
	        value.addDouble(v.getDouble(1))
	        value
	      }
	      case "average" => {
	        val value = Record(1)
	        value.addDouble(v.getDouble(1) /  v.getInt(0))
	        value
	      }
	      case "minMax" => {
	        val value = Record(v)
	        value
	      }
	    }
	    value
	  })
	    
	    
	  //formatting
	  val outData = 
	  if (outputCompact) {
	    //all quant fields in one line
	    aggrData.map(r => {
	      val key = r._1
	      val value = r._2
	      val newKey = Record(key, 0, key.size - 1)
	      val newValue = if (aggrType.equals("minMax")) {
		    val newValue = Record(3)
		    newValue.addInt(key.getInt(key.size - 1))
		    newValue.addDouble(value.getDouble(0))
		    newValue.addDouble(value.getDouble(1))
	        newValue
	      } else {
		    val newValue = Record(2)
		    newValue.addInt(key.getInt(key.size - 1))
		    newValue.addDouble(value.getDouble(0))
		    newValue
	      }
	      (newKey, newValue)
	    }).groupByKey.map(r => {
	      val key = r._1
	      val values = r._2.toList
	      values.sortBy(v => v.getInt(0))
	      val aggrValues = values.map(v => {
	        val str = BasicUtils.formatDouble(v.getDouble(1), outputPrecision)
	        if (aggrType.equals("minMax")) {
	          str + fieldDelimOut + BasicUtils.formatDouble(v.getDouble(2), outputPrecision)
	        } else {
	          str
	        }
	      })
	      key.toString + fieldDelimOut + aggrValues.mkString(fieldDelimOut)
	    })
	  } else {
		  //one line per quant field
		  aggrData.map(r => r._1.toString + fieldDelimOut + r._2.withFloatPrecision(outputPrecision).toString)
	  }
	  
	  if (debugOn) {
	     outData.collect.slice(0,50).foreach(s => println(s))
	  }
	   
	  if (saveOutput) {
	     outData.saveAsTextFile(outputPath)
	  }
	  
   }

}