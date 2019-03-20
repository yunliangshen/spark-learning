package com.haiziwang.spark

import org.apache.spark.{SparkConf, SparkContext}

/**
  * parse the linkage csv
  *
  * User: SHEN_YL
  * Date: 2018/12/26 15:46
  * Ver.: 1.0
  */
object Linkage {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setAppName("WordCountLocal").setMaster("local")

    val sc = new SparkContext(sparkConf)

    val rawBlocks = sc.textFile("linkage/")

    val head = rawBlocks.take(10)

    //head.foreach(println)

    //head.filter(isHeader).foreach(println)

    //head.filterNot(isHeader).length

    // anonymous functions support
    //println(head.filter(x => !isHeader(x)).length)

    // _ the param of anonymous functions
    //println(head.filter(!isHeader(_)).length)

    //val noHeader = rawBlocks.filter(!isHeader(_))

    //println(noHeader.count())

    //println(noHeader.first)

    val line = head(5)

    println(line)

    val tup = parseTup(line)

    println(tup)

    println(tup._1)

    println(tup.productElement(0))

    println(tup.productArity)

    val md = parse(line)

    println(md.matched)

    //val mds = head.filter(x => !isHeader(x)).map(x => parse(x))

    //println(mds)

    val noHeader = rawBlocks.filter(x => !isHeader(x))

    val parsed = noHeader.map(line => parse(line))

    println(parsed)

    sc.stop()

  }

  def isHeader(line: String): Boolean = {

    line.contains("id_1")
  }

  def toDouble(s: String) = {

    if ("?".equals(s))
      Double.NaN
    else
      s.toDouble
  }

  def parseTup(line: String) = {

    val pieces = line.split(",")
    val id1 = pieces(0).toInt
    val id2 = pieces(1).toInt
    val scores = pieces.slice(2, 11).map(toDouble)
    val matched = pieces(11).toBoolean
    (id1, id2, scores, matched)

  }

  def parse(line: String) = {

    val pieces = line.split(",")
    val id1 = pieces(0).toInt
    val id2 = pieces(1).toInt
    val scores = pieces.slice(2, 11).map(toDouble)
    val matched = pieces(11).toBoolean
    MatchData(id1, id2, scores, matched)

  }

  case class MatchData(id1: Int, id2: Int, scores: Array[Double], matched: Boolean)

}
