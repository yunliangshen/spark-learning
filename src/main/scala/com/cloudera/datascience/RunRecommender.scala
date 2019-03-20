package com.cloudera.datascience

import org.apache.spark.sql.SparkSession

object RunRecommender {


  def main(args: Array[String]): Unit = {


    val spark = SparkSession
      .builder()
      .master("local[2]")
      .appName("Recommender")
      .getOrCreate()

    val base = "ds/"

    // user artist data
    val rawUserArtistData = spark.read.textFile(base + "user_artist_data.txt")

    import spark.implicits._

    rawUserArtistData.take(5).foreach(println)

    val userArtistDF = rawUserArtistData.map { line =>
      val Array(user, artist, _*) = line.split(' ')
      (user.toInt, artist.toInt)
    }.toDF("user", "artist")

    import org.apache.spark.sql.functions._

    userArtistDF.agg(
      min("user"), max("user"), min("artist"), max("artist")
    ).show()

    // artist data
    val rawArtistData = spark.read.textFile(base + "artist_data.txt")

    val artistById = rawArtistData.flatMap { line =>
      val (id, name) = line.span(_ != '\t')
      if (name.isEmpty) {
        None
      } else {
        try {
          Some(id.toInt, name.trim)
        } catch {
          case _: NumberFormatException => None
        }
      }
    }.toDF("id", "name")

    artistById.take(5).foreach(println)

    // artist alias
    val rawArtistAlias = spark.read.textFile(base + "artist_alias.txt")

    val artistAlias = rawArtistAlias.flatMap { line =>
      val tokens = line.split("\t")
      if (tokens(0).isEmpty) {
        None
      } else {
        Some(tokens(0).toInt, tokens(1).toInt)
      }
    }.collect().toMap

    println(artistAlias.head)

    artistById.filter($"id" isin (1208690,1003926)).show()

    spark.stop()

  }

}
