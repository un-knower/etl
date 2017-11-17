package com.jiuzhi.etl.fad

import util.Random

object Test {

  def rand {
    val rand = new Random()
    for (i <- 0 to 1000) {
      println(rand.nextInt(100))
    }
  }

  def main(args: Array[String]): Unit = {
    rand
  }

}