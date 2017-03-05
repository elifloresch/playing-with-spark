package com.learning.spark

import org.apache.spark.{SparkConf, SparkContext}

object LetterCounter {
  val fileName = "star-wars.txt"

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Learning with Scala")
    val sc = new SparkContext(conf)
    val logData = sc.textFile(fileName, 2).cache()
    println(s"logData ==> $logData")
    val numberOfItemsInRDD = logData.count()
    println(s" Number of items in this RDD ==> $numberOfItemsInRDD")
    val firstItemInRDD = logData.first()
    println(s" First item in this RDD ==> $firstItemInRDD")
    val wordsPerLine = logData.map(line => line.split(" ").size)
    println(s" Words per line: $wordsPerLine")
    val lineWithMostWords = wordsPerLine.reduce((a,b) => Math.max(a, b))
    println(s"Line with the most words: $lineWithMostWords")
    val linesWithStarWars = logData.filter(line => line.contains("Star Wars"))
    println(s" Lines with Star Wars: $linesWithStarWars")
    val numberOfLinesWithStarWars = linesWithStarWars.count()
    println(s" Number of lines with Star Wars: $numberOfLinesWithStarWars")
    val numAs = logData.filter(line => line.contains("a")).count()
    val numBs = logData.filter(line => line.contains("b")).count()
    println(s"Lines with a: $numAs, Lines with b: $numBs")
    sc.stop()
  }
}
