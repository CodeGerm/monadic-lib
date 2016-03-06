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
import scala.concurrent.{Await, Future}
import scala.concurrent.duration.Duration
import scala.concurrent.ExecutionContext.Implicits.global 

/**
 * A monadic type transformer that can be used to chain in for-comprehension
 * Implementations can be Case class so that parameters can be passed in class level 
 * @author WZ
 */
trait Transformer[OUT] extends LazyLogging { self: Transformer[OUT] =>
  
  /**
   * Override to perform transformation
   * and return the desired type
   */
  def transform(): OUT
  
  /**
   * monadic operation that can chain another transformer 
   * (will be called implicitly in for-comprehension )
   */
  def flatMap[NEXT] (f: OUT => Transformer[NEXT]): Transformer[NEXT] = {
    this.validateInput
    new Transformer[NEXT] {
      def transform (): NEXT = {
        val nextTransformer: Transformer[NEXT] = f(self.transform)
        nextTransformer.transform
      }
    }
  }

  /**
   * monadic operation that can yield the last transformer 
   * (will be called implicitly at the end of for-comprehension )
   */
  def map[NEXT] (f: OUT => NEXT): Transformer[NEXT] = {
    this.validateInput
    new Transformer[NEXT] {
      def transform (): NEXT = {
        f(self.transform)
      }
    }
  }
  
  /**
   * Shouldn't be required in for-comprehension
   * Known issue:
   * https://issues.scala-lang.org/browse/SI-1336
   */
  def filter(p: OUT => Boolean): Transformer[OUT] = {
    map { r => 
      if (p(r)) 
        r 
      else 
        throw new NoSuchElementException("not satisfied")
    }
  }
  
  /**
   * can be used to chain transformers that can run in parallel
   */
  def zip[ANOTHER_OUT](that: Transformer[ANOTHER_OUT]): Transformer[(OUT, ANOTHER_OUT)] = {
    new Transformer[(OUT, ANOTHER_OUT)] {
      def transform (): (OUT, ANOTHER_OUT) = {
        val transformer1 = Future { 
          self.validateInput
          self.transform
        }
        val transformer2 = Future {
          that.validateInput
          that.transform
        }
        val result = transformer1 zip transformer2
        Await.result(result, Duration.Inf)
      }
    }
  }
  
  /**
   * Override this to do input validation
   */
  protected def validateInput(): Unit = {}
  
}