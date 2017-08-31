package com.zqh.spark.connectors.test

/**
  * Created by zhengqh on 17/8/31.
  */
trait TestClassToLoader {

  def method(): Unit
}

class TestClass extends TestClassToLoader {

  override def method() = {
    println("hello")
  }
}
