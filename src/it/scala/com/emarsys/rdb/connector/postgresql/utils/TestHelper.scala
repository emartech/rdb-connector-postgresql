package com.emarsys.rdb.connector.postgresql.utils

import java.util.Properties

import com.emarsys.rdb.connector.postgresql.PostgreSqlConnector.{PostgreSqlConnectionConfig, createUrl}
import slick.util.AsyncExecutor

import scala.concurrent.Future

import slick.jdbc.PostgresProfile.api._

object TestHelper {
  import com.typesafe.config.ConfigFactory

  val value = ConfigFactory.load().getString("dbconf.host")

  lazy val TEST_CONNECTION_CONFIG = PostgreSqlConnectionConfig(
    host = ConfigFactory.load().getString("dbconf.host"),
    port= ConfigFactory.load().getInt("dbconf.port"),
    dbName= ConfigFactory.load().getString("dbconf.dbName"),
    dbUser= ConfigFactory.load().getString("dbconf.user"),
    dbPassword= ConfigFactory.load().getString("dbconf.password"),
    certificate= ConfigFactory.load().getString("dbconf.certificate"),
    connectionParams= ConfigFactory.load().getString("dbconf.connectionParams")
  )

  private lazy val executor = AsyncExecutor.default()

  private lazy val db: Database = {
    Database.forURL(
      url = createUrl(TEST_CONNECTION_CONFIG),
      driver = "org.postgresql.Driver",
      user = TEST_CONNECTION_CONFIG.dbUser,
      password = TEST_CONNECTION_CONFIG.dbPassword,
      prop = new Properties(),
      executor = executor
    )

  }

  def executeQuery(sql: String): Future[Int] = {
    db.run(sqlu"""#$sql""")
  }
}
