package com.learning.spark

import org.apache.spark.{SparkConf, SparkContext}

object LetterCounter {
  val fileName = "star-wars.txt"

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Learning with Scala")
    val sc = new SparkContext(conf)
    val logData = sc.textFile(fileName, 2).cache()
    val numAs = logData.filter(line => line.contains("a")).count()
    val numBs = logData.filter(line => line.contains("b")).count()
    println(s"Lines with a: $numAs, Lines with b: $numBs")
    sc.stop()
  }
}
