package com.github.fescalhao

import org.apache.spark.SparkConf

import java.util.Properties
import scala.io.Source

package object SparkUtils {
  def getSparkConf(appName: String): SparkConf = {
    val sparkConf = new SparkConf()
    val props = new Properties()
    props.load(Source.fromFile("spark.conf").bufferedReader())
    props.forEach((k,v) => {
      sparkConf.set(k.toString, v.toString)
    })

    sparkConf.setAppName(appName)
  }
}
