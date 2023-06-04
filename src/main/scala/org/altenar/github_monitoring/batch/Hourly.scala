package org.altenar.github_monitoring.batch

import java.io.File
import com.typesafe.config.ConfigFactory
import org.altenar.github_monitoring.utils.Common.{loadSourceGithubData, writeBatchToClick}
import org.apache.logging.log4j.scala.Logging
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SparkSession}

object Hourly extends App with Logging {

  // List of Developers that own more than one repository
  def updateReport2(
      sourceGithubData: DataFrame,
      targetUri: String,
      targetUser: String,
      targetPassword: String,
      targetTable: String): Unit = {

    val result =
      sourceGithubData
        .select("actor.login", "repo.name")
        .groupBy("login")
        .count().distinct()
        .where(col("count") > 1)
    writeBatchToClick(
      result,
      targetUri,
      targetUser,
      targetPassword,
      targetTable,
      "ENGINE=MergeTree PRIMARY KEY (login)")
  }

  // just for demo the master is local
  implicit val spark: SparkSession = SparkSession.builder()
    .master("local")
    .appName("Test application")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .getOrCreate()

  val sourceDataFolder = "/Users/andreimaiorov/Desktop/source_data/*"
  val config = ConfigFactory.load(ConfigFactory.parseFile(new File("src/main/resources/application_hourly.conf")))
  val targetUri = config.getString("clickhouse_uri")
  val targetUser = config.getString("clickhouse_user")
  val targetPassword = config.getString("clickhouse_password")
  val targetTableReport2 = config.getString("target_table_report2")

  val sourceGithubData = loadSourceGithubData(sourceDataFolder, spark)
  updateReport2(sourceGithubData, targetUri, targetUser, targetPassword, targetTableReport2)
}