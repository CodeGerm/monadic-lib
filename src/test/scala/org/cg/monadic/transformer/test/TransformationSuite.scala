package org.cg.monadic.transformer.test

import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import org.junit.runner.RunWith

import org.cg.monadic.transformer.Transformer

/**
 * @author WZ
 */
case class Transformation0(input: Int) extends Transformer[Integer] {
  override def transform() = {
    val result = input
    val threadName = Thread.currentThread().getName
    println(s"[$threadName]transformation0 result: $result " + System.currentTimeMillis())
    Thread.sleep(500)
    result
  }
}

case class Transformation1(input: Int) extends Transformer[Integer] {
  override def transform() = {
    val result = input+1
    val threadName = Thread.currentThread().getName
    println(s"[$threadName]transformation1 result: $result " + System.currentTimeMillis())
    Thread.sleep(500)
    result
  }
}

case class Transformation2(input: Int) extends Transformer[Integer] {
  override def transform() = {
    val result = input+2
    val threadName = Thread.currentThread().getName
    println(s"[$threadName]transformation2 result: $result " + System.currentTimeMillis())
    Thread.sleep(500)
    result
  }
}

/**
 * @author WZ
 */
@RunWith(classOf[JUnitRunner])
class TransformationSuite extends FunSuite {
  
  test ("test pipeline basic functionality without zip parallelism") {
    val result = 
      for {
        transformer0A <- Transformation0(0)
        transformer0B <- Transformation0(1)
        transformer1A <- Transformation1(transformer0A) 
        transformer1B <- Transformation1(transformer0B) 
        transformer2A <- Transformation2(transformer1A)
        transformer2B <- Transformation2(transformer1B)
      } yield (transformer2A, transformer2B)
    println(result.transform)  
  }
  
  test ("test pipeline basic functionality with zip parallelism") {
    val result = 
      for {
        (transformer0A, transformer0B) <- Transformation0(0) zip Transformation0(1)
        (transformer1A, transformer1B) <- Transformation1(transformer0A) zip Transformation1(transformer0B)
        (transformer2A, transformer2B) <- Transformation2(transformer1A) zip Transformation2(transformer1B)
      } yield (transformer2A, transformer2B)
    println(result.transform)  
  }
  
}