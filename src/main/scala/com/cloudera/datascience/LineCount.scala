package com.cloudera.datascience

import org.apache.spark.sql.SparkSession

/**
  *
 * @Description:
 * @Author:         SHEN_YL
 * @CreateDate:     2018/12/27 14:41
 * @UpdateUser:     yc
 * @UpdateDate:     2018/12/27 14:41
 * @UpdateRemark:   修改内容
 * @Version:        1.0
 *
 */
object LineCount {


  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().master("local").getOrCreate()

    println("lines:" + countLines(spark, "linkage/"))

    spark.stop()
  }


  def countLines(spark : SparkSession, path : String) : Long = {
    spark.read.textFile(path).count()
  }

}
