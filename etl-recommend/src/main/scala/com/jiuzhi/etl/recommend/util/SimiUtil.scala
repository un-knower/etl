package com.jiuzhi.etl.recommend.util

import java.text.DecimalFormat
import java.math.RoundingMode

import scala.math._

object SimiUtil {

  def rating(params: Map[String, Double]): Double = {
    var score = 0d

    val eventWeight = params.get("event_name").get
    val dateDiff = params.get("date_diff").get
    val userCount = params.get("user_count").get
    val totalCount = params.get("total_count").get

    score = eventWeight * (1 / log(dateDiff) + 1) * (userCount / totalCount)

    score
  }

  /**
   * 相似度
   */
  def similarity(obj1: Map[Long, Double], obj2: Map[Long, Double]): Double = {
    val keys = obj1.keys.toSet.intersect(obj2.keys.toSet)
    var sum = 0d
    for (key <- keys) sum += obj1(key) * obj2(key)

    sum
  }

  /**
   * 余弦相似度
   */
  def cos_sim(obj1: Map[Long, Double], obj2: Map[Long, Double]): Double = {
    val keys = obj1.keys.toSet.intersect(obj2.keys.toSet)
    var sum1 = 0d
    for (key <- keys) sum1 += obj1(key) * obj2(key)

    var sum2 = 0d
    for (key <- obj1.keys) sum2 += pow(obj1(key), 2)

    var sum3 = 0d
    for (key <- obj2.keys) sum3 += pow(obj2(key), 2)

    sum1 / (sqrt(sum2) * sqrt(sum3))
  }

  /**
   * 皮尔逊相关系数
   */
  def pearsonsr(obj1: Map[Long, Double], obj2: Map[Long, Double]): Double = {
    val keys = obj1.keys.toSet.intersect(obj2.keys.toSet)
    val size = keys.size
    if (size == 0) return 0

    // 求和
    val sum1 = {
      var sum: Double = 0
      for (key <- keys) sum += obj1(key)
      sum
    }
    val sum2 = {
      var sum: Double = 0
      for (key <- keys) sum += obj2(key)
      sum
    }

    // 求平方和
    val sqsum1 = {
      var sum: Double = 0
      for (key <- keys) sum += pow(obj1(key), 2)
      sum
    }
    val sqsum2 = {
      var sum: Double = 0
      for (key <- keys) sum += pow(obj2(key), 2)
      sum
    }

    // 乘机之和
    val psum = {
      var sum: Double = 0
      for (key <- keys) sum += obj1(key) * obj2(key)
      sum
    }

    // 计算皮尔逊相关系数
    val num = psum - (sum1 * sum2 / size)
    val den = sqrt((sqsum1 - pow(sum1, 2) / size).abs * (sqsum2 - pow(sum2, 2) / size).abs)

    if (den == 0) 0 else (num / den)
  }

  def main(args: Array[String]): Unit = {
    val info1 = Map(1l -> 1.31, 2l -> 1.03, 3l -> 0.53)
    val info2 = Map(1l -> 1.01, 4l -> 0.99, 5l -> 0.59)
    val info3 = Map(2l -> 1.35, 3l -> 0.98, 6l -> 0.37)
    printf("%s\n%s\t%s\t%s\n", "相似度算法比较", "similarity", "cos_sim", "pearsonsr")
    printf("%s\t%s\t%s\n", similarity(info1, info2), cos_sim(info1, info2), pearsonsr(info1, info2))
    printf("%s\t%s\t%s\n", similarity(info1, info3), cos_sim(info1, info3), pearsonsr(info1, info3))
    printf("%s\t%s\t%s\n", similarity(info2, info3), cos_sim(info2, info3), pearsonsr(info2, info3))
  }

}