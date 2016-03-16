/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.cg.monadic.transformer

import com.typesafe.scalalogging.slf4j.LazyLogging
import scala.util.{Failure, Success, Try}
import scala.util.control.NonFatal
import com.typesafe.config.Config

/**
 * @author WZ
 */

trait TransformationPipeline[IN, OUT]  extends LazyLogging {
  
  /**
   * Implement this to apply transformations for the pipeline
   */
  protected def execute(in: IN, config: Config = null): OUT
  
  /**
   * Validates the input. Throw an exception if it is invalid.
   */
  protected def validateInput(model: IN): Unit = {}
  
  /**
   * Apply transformations for the pipeline
   */
  def apply(in: IN, config: Config = null): Try[OUT] = {
    try {
      validateInput(in)
      val toTry = Try(execute(in, config))
      toTry match {
        case Success(s) => 
          logger.debug("[transform]success: " + s.toString())
        case Failure(e) => 
          val errSw = new StringWriter
          e.printStackTrace(new PrintWriter(errSw))
          logger.error("[transform]failed to transform: \n" + errSw.toString())  
      }
      toTry
    } catch {
      case NonFatal(e) => 
        val errSw = new StringWriter
        e.printStackTrace(new PrintWriter(errSw))
        logger.error("invalid transformation: \n" + errSw.toString())
        Failure(e)
    }
  }
  
}