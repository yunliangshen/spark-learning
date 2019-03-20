package com.cloudera.datascience

import org.apache.spark.sql.functions.first
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  *
  * @Description: Advanced Analytics with spark introduce
  *               RunIntro
  * @Author: SHEN_YL
  * @CreateDate: 2018/12/27 15:04
  * @UpdateUser: SHEN_YL
  * @UpdateDate: 2018/12/27 15:04
  * @UpdateRemark: 修改内容
  * @Version: 1.0
  *
  */
object RunIntro {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local[2]")
      //.enableHiveSupport()
      .appName("Intro")
      .getOrCreate()

    // For implicit conversions like converting RDDs to DataFrames
    // toDF() & "$"
    import spark.implicits._

    //val preview =spark.read.csv("linkage/")

    //preview.show()
    //preview.printSchema()

    val parsed = spark.read
      .option("header", "true")
      .option("nullValue", "?")
      .option("inferSchema", "true")
      .csv("linkage/")

    parsed.show()

    parsed.printSchema()

    println("cnt:" + parsed.count())

    // countByValue() is only the right thing to do when we know that there are just a few distinct values in dataset
    val resRdd = parsed.rdd.
      map(_.getAs[Boolean]("is_match")).
      countByValue()

    println(resRdd)

    // DataFrame
    // Under the covers, the Spark engine determines the most efficient way to perform the aggregation and return the results.
    // without us having to worry about the details if which RDD APIs to use.
    // The result is a cleaner, faster and more expressive way to do data analysis in Spark.
    parsed
      .groupBy("is_match")
      .count()
      .orderBy($"count".desc)
      .show()

    // In addition to count, we can also compute more complex aggregations like sum, min, max, mean and standard deviation
    // using the agg method of the DataFrame API in conjunction with the aggregation functions defined in the
    // org.apache.spark.sql.functions package
    import org.apache.spark.sql.functions._
    parsed.agg(avg($"cmp_sex"), stddev($"cmp_sex")).show()

    // register temporary table with the Spark SQL engine
    parsed.createOrReplaceTempView("linkage")

    spark.sql(
      """
        SELECT is_match, COUNT(*) cnt
        FROM linkage
        GROUP BY is_match
        ORDER BY cnt DESC
      """).show()

    // Fast Summary Statistics for DataFrames
    val summary = parsed.describe()

    summary.show()

    // summary.select("summary", "cmp_fname_c1", "cmp_fname_c2").show()

    val matches = parsed.where("is_match = true")
    val matchSummary = matches.describe()

    val misses = parsed.filter($"is_match" === false)
    val missSummary = misses.describe()

    summary.printSchema()

    val schema = summary.schema

    // flatMap: it takes a function argument that processes each input record and returns a sequence of zero or more output records
    val longForm = summary.flatMap(row => {
      val metric = row.getString(0)
      (1 until row.size).map(i => {
        (metric, schema(i).name, row.getString(i).toDouble)
      })
    })

    val longDF = longForm.toDF("metric", "field", "value")
    longDF.show()

    // pivot needs to know the distinct set of values in the pivot column that we want to use for the column
    // agg(first) specify the value in each cell of the wide table
    val wideDF = longDF.toDF.
      groupBy("field").
      pivot("metric", Seq("count", "mean", "stddev", "min", "max")).
      agg(first("value"))

    wideDF.select("field", "count", "mean").show()

    // function
    val matchSummaryT = pivotSummary(matchSummary)
    val missSummaryT = pivotSummary(missSummary)

    matchSummaryT.createOrReplaceTempView("match_desc")
    missSummaryT.createOrReplaceTempView("miss_desc")

    spark.sql(
      """
        |SELECT a.field,
        |       a.count + b.count total,
        |       a.mean - b.mean delta
        |FROM match_desc a
        |INNER JOIN miss_desc b
        |ON a.field = b.field
        |WHERE a.field NOT IN ("id_1", "id_2")
        |ORDER BY delta DESC, total DESC
      """.stripMargin).show()

    // case class
    val matchData = parsed.as[MatchData]

    matchData.show(10)

    val scored = matchData.map { md =>
      (scoreMatchData(md), md.is_match)
    }.toDF("score", "is_match")

    scored.show(10)

    // Applying a high threshold value of 4.0, meaning that the average of the five features is
    // 0.8, we can filter out almost all of the nonmatches while keeping over 90% of the matches
    crossTabs(scored, 4.0)
        .show()

    // Applying the lower threshold of 2.0, we can ensure that we capture all of the known
    // matching records, but at a substantial cost in terms of false positive (top-right cell)
    crossTabs(scored, 2.0)
        .show()

    spark.stop()

  }

  // transpose a summary DataFrame
  def pivotSummary(desc: DataFrame): DataFrame = {
    val schema = desc.schema
    import desc.sparkSession.implicits._

    val lf = desc.flatMap(row => {
      val metric = row.getString(0)
      (1 until row.size).map(i => {
        (metric, schema(i).name, row.getString(i).toDouble)
      })
    }).toDF("metric", "field", "value")

    lf.groupBy("field").
      pivot("metric", Seq("count", "mean", "stddev", "min", "max")).
      agg(first("value"))
  }

  case class Score(value: Double) {
    def +(oi: Option[Int]) = {
      Score(value + oi.getOrElse(0))
    }
  }

  def scoreMatchData(md: MatchData): Double = {
    (Score(md.cmp_lname_c1.getOrElse(0.0)) + md.cmp_plz + md.cmp_by + md.cmp_bd + md.cmp_bm).value
  }

  // Model Evaluation
  def crossTabs(scored: DataFrame, t: Double): DataFrame = {
    scored.
      selectExpr(s"score>=$t as above", "is_match").
      groupBy("above").
      pivot("is_match", Seq("true", "false")).
      count()
  }

}

// use Option[T] type to represent fields whose values may be null
case class MatchData(
                      id_1: Int,
                      id_2: Int,
                      cmp_fname_c1: Option[Double],
                      cmp_fname_c2: Option[Double],
                      cmp_lname_c1: Option[Double],
                      cmp_lname_c2: Option[Double],
                      cmp_sex: Option[Int],
                      cmp_bd: Option[Int],
                      cmp_bm: Option[Int],
                      cmp_by: Option[Int],
                      cmp_plz: Option[Int],
                      is_match: Boolean
                    )





