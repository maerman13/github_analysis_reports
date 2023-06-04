package org.altenar.github_monitoring.batch

import java.io.File
import com.typesafe.config.ConfigFactory
import org.altenar.github_monitoring.utils.Common.{loadSourceGithubData, writeBatchToClick}
import org.apache.logging.log4j.scala.Logging
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.col

object Daily extends App with Logging {

  // List of Developers that own more than one repository
  def updateReport1(
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

  // List of Developers with less than one commit in a day
  def updateReport3(
      sourceGithubData: DataFrame,
      targetUri: String,
      targetUser: String,
      targetPassword: String,
      targetTable: String): Unit = {

    val result =
      sourceGithubData
      .select(col("actor.login").as("login"), col("payload.commits.url").as("commits"))
      .select("login")
      .distinct()
    writeBatchToClick(
      result,
      targetUri,
      targetUser,
      targetPassword,
      targetTable,
      "ENGINE=MergeTree PRIMARY KEY (login)")
  }

  // Total Developers grouped by gender
  def updateReport4(
      sourceGithubData: DataFrame,
      targetUri: String,
      targetUser: String,
      targetPassword: String,
      targetTable: String): Unit = {

    val result =
      sourceGithubData
        .select(col("actor.login").as("login"))
        .distinct()
        .groupBy()
        .count()
    writeBatchToClick(
      result,
      targetUri,
      targetUser,
      targetPassword,
      targetTable,
      "ENGINE=MergeTree PRIMARY KEY (count)")
  }

  // Total projects with more than 10 members
  def updateReport5(
      sourceGithubData: DataFrame,
      targetUri: String,
      targetUser: String,
      targetPassword: String,
      targetTable: String): Unit = {

    val result =
      sourceGithubData
      .select("actor.login", "repo.name")
      .distinct()
      .groupBy("name")
      .count()
      .where(col("count") > 10)
    writeBatchToClick(
      result,
      targetUri,
      targetUser,
      targetPassword,
      targetTable,
      "ENGINE=MergeTree PRIMARY KEY (name)")
  }

  // just for demo the master is local
  implicit val spark: SparkSession = SparkSession.builder()
    .master("local")
    .appName("Test application")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .getOrCreate()
  
  val config = ConfigFactory.load(ConfigFactory.parseFile(new File("src/main/resources/application_daily.conf")))
  val sourceDataFolder = config.getString("source_data_folder")
  val targetUri = config.getString("clickhouse_uri")
  val targetUser = config.getString("clickhouse_user")
  val targetPassword = config.getString("clickhouse_password")
  val targetTableReport1 = config.getString("target_table_report1")
  val targetTableReport3 = config.getString("target_table_report3")
  val targetTableReport4 = config.getString("target_table_report4")
  val targetTableReport5 = config.getString("target_table_report5")

  val sourceGithubData = loadSourceGithubData(sourceDataFolder, spark)
  updateReport1(sourceGithubData, targetUri, targetUser, targetPassword, targetTableReport1)
  updateReport3(sourceGithubData, targetUri, targetUser, targetPassword, targetTableReport3)
  updateReport4(sourceGithubData, targetUri, targetUser, targetPassword, targetTableReport4)
  updateReport5(sourceGithubData, targetUri, targetUser, targetPassword, targetTableReport5)
}