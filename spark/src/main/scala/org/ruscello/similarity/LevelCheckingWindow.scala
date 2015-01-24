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

import org.hoidla.window.SizeBoundWindow
import org.hoidla.window.WindowUtils
import org.hoidla.query.Predicate
import org.hoidla.query.Criteria

class LevelCheckingWindow(val windowSize : Int, val windowStep : Int, val levelThrehold : Int, 
    val levelThresholdMargin : Int, val checkingStrategy : String) extends Serializable {
	val window = new SizeBoundWindow[Int](windowSize, windowStep)
	var violations = List[(Long, Double)]()
	val criteria = new Criteria();
	buildCriteria()
	
	/**
	 * @param value
	 */
	def addAndCheck(value : Int) {
	  window.add(value)

	  //check threshold
	  if (window.isFull()) {
	    val result = criteria.evaluate(window)
	    result match  {
	      case true => {
	        val mean = WindowUtils.getMean(window)
	        violations = violations :+  (System.currentTimeMillis(), mean)
	      }
	      
	      case _ => 
	    }
	  }
	}
	
	/**
	 * 
	 */
	private def buildCriteria() {
	    checkingStrategy match {
	      case "simpleMean" => {
	        criteria.
	        	withPredicate(new Predicate(Predicate.OPERAND_MEAN, Predicate.OPERATOR_GE, 
	            levelThrehold + levelThresholdMargin)).
	            withOr().
	            withPredicate(new Predicate(Predicate.OPERAND_MEAN, Predicate.OPERATOR_LE, 
	            levelThrehold - levelThresholdMargin))
	      }
	      
	      case _ => throw new IllegalArgumentException("invalid checking strategy")
	    }
	}
	
	def numViolations() : Int = {
	  violations.size
	}
	
}