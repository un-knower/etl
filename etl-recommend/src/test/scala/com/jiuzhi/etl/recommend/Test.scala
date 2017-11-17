package com.jiuzhi.etl.recommend

import scala.math._

object Test {

  def main(args: Array[String]): Unit = {
    //sort

    //page

    //concat

    split
  }

  def sort {
    val list = Array((1, 8), (2, 5), (3, 7), (4, 5))
    val result = list.sortWith((smaller, bigger) => smaller._2 < bigger._2).mkString("\t")
    println(result)
  }

  def page {
    val list = Array(1, 8, 5, 2, 7, 3, 6, 9, 4, 0).sorted.reverse
    println(list.mkString("\t"))
    val length = list.length
    val pageSize = 3
    val totalPage = length / pageSize + min(length % pageSize, 1)
    for (i <- 1 to totalPage) {
      println(list.slice((i - 1) * pageSize, i * pageSize).mkString(","))
    }
  }

  def concat {
    var str = ""
    str.concat("zhang").concat(" ").concat("chao")
    println("str=" + str)
    str = str.concat("zhang").concat(" ").concat("chao")
    println("str=" + str)
  }

  def split {
    val str = "zhang,li,wang,liu"
    val str1 = str.split(",").mkString("'", "','", "'")
    println(str)
    println(str1)
    println("".split(",").length)
    println("".split(",").mkString("'", "','", "'"))
  }

}