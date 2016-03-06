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
import com.typesafe.config.{ConfigFactory, Config}
import scala.collection.JavaConverters._

import java.net.URL

/**
 * A context wrapper for transformation pipeline 
 * that can load configurations for Worker nodes
 * @author WZ
 */
class TransformationPipelineContext extends LazyLogging {
  
  var config: Option[Config] = None
  
  def initIfUndefined(): Unit = {
   if (!config.isDefined) {
    config = Some(ConfigFactory.load())
    assert(config.isDefined && !config.get.isEmpty, 
          "Failed to init context")
     config =  Option(config.get.resolve())      
    logger.info("default:\n\n"+getConfigString(config.get))
   }
  }
  
  def initIfUndefined(resourceName: String): Unit = {
    if (!config.isDefined) {
      config = Some(ConfigFactory.load(resourceName))
      assert(config.isDefined && !config.get.isEmpty, 
          "Failed to init context")
      config =  Option(config.get.resolve())     
      logger.info(s"$resourceName:\n\n"+getConfigString(config.get))
    }
  }
    
  def initIfUndefined(url: URL): Unit = {
    if (!config.isDefined) {
      config = Some(ConfigFactory.parseURL(url))
      assert(config.isDefined && !config.get.isEmpty, 
          "Failed to init context")
      config =  Option(config.get.resolve())   
      logger.info(s"$url:\n\n"+getConfigString(config.get))
    }
  }
    
  def initIfUndefined(theConfig: Config): Unit = {
    if (!config.isDefined) {
      config = Some(theConfig)
      assert(config.isDefined && !config.get.isEmpty, 
          "Failed to init context")
      config =  Option(config.get.resolve())     
      logger.info("\n\n"+getConfigString(config.get))    
    }
  }
    
  def loadConfig(node: String): Config = {
    //verify that the Config is sane and has the reference config node.
    assert(config.isDefined && !config.get.isEmpty, 
        "context must be initialized")
    config.get.checkValid(ConfigFactory.defaultReference(), node)
    //TODO more validation
    logger.info(s"> Loading $node configuration")
    val theConfig = config.get.getConfig(node)
    theConfig
  }
  
  def getConfigString(config: Config): String = {    
    var configStr = "***************************\n"
    val iter =  config.entrySet().asScala
    for (e <- iter) {
      configStr += e.getKey + "=" + e.getValue + "\n"
    }
    configStr
  }
}