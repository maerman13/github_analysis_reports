package org.altenar.github_monitoring.utils

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object Common {

  def loadSourceGithubData(path: String, spark: SparkSession): DataFrame = {

    spark
      .read
      .json(path)
      .cache()
  }

  def writeBatchToClick(
      df: DataFrame,
      targetUri: String,
      targetUser: String,
      targetPassword: String,
      targetTable: String,
      targetTableCreateOptions: String): Unit = {

    df
      .write
      .format("jdbc")
      .option("driver", "com.clickhouse.jdbc.ClickHouseDriver")
      .option("url", targetUri)
      .option("user", targetUser)
      .option("password", targetPassword)
      .option("dbtable", targetTable)
      .option("createtableoptions", targetTableCreateOptions)
      .mode(SaveMode.Overwrite)
      .save()
  }
}