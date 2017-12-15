package com.emarsys.rdb.connector.postgres

import java.util.Properties

import com.emarsys.rdb.connector.common.ConnectorResponse
import com.emarsys.rdb.connector.common.models.{ConnectionConfig, Connector, ConnectorCompanion, MetaData}
import com.emarsys.rdb.connector.common.models.Errors.ErrorWithMessage
import com.emarsys.rdb.connector.postgres.PostgreSqlConnector.{PostgreSqlConnectionConfig, PostgreSqlConnectorConfig}
import slick.jdbc.PostgresProfile.api._
import slick.util.AsyncExecutor

import scala.concurrent.duration._
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future}

class PostgreSqlConnector(
                         protected val db: Database,
                         protected val connectorConfig: PostgreSqlConnectorConfig
                       )(
                         implicit val executionContext: ExecutionContext
                       )
  extends Connector
    with PostgreSqlTestConnection
    with PostgreSqlMetadata
    with PostgreSqlSimpleSelect
    with PostgreSqlRawSelect
    with PostgreSqlIsOptimized
    with PostgreSqlRawDataManipulation {

  override def close(): Future[Unit] = {
    db.shutdown
  }
}

object PostgreSqlConnector extends ConnectorCompanion {

  case class PostgreSqlConnectionConfig(
                                       host: String,
                                       port: Int,
                                       dbName: String,
                                       dbUser: String,
                                       dbPassword: String,
                                       connectionParams: String
                                     ) extends ConnectionConfig

  case class PostgreSqlConnectorConfig(
                                      queryTimeout: FiniteDuration,
                                      streamChunkSize: Int
                                    )

  private val defaultConfig = PostgreSqlConnectorConfig(
    queryTimeout = 20.minutes,
    streamChunkSize = 5000
  )

  def apply(
             config: PostgreSqlConnectionConfig,
             connectorConfig: PostgreSqlConnectorConfig = defaultConfig
           )(
             executor: AsyncExecutor
           )(
             implicit executionContext: ExecutionContext
           ): ConnectorResponse[PostgreSqlConnector] = {

    if (checkSsl(config.connectionParams)) {

      val db = Database.forURL(
        url = createUrl(config),
        driver = "org.postgresql.Driver",
        user = config.dbUser,
        password = config.dbPassword,
        prop = new Properties(),
        executor = executor
      )

      Future(Right(new PostgreSqlConnector(db, connectorConfig)))

    } else {
      Future(Left(ErrorWithMessage("SSL Error")))
    }
  }

  override def meta() = MetaData("\"", "'", "\\")

  private[postgres] def checkSsl(connectionParams: String): Boolean = {
    true
  }

  private[postgres] def createUrl(config: PostgreSqlConnectionConfig) = {
    s"jdbc:postgresql://${config.host}:${config.port}/${config.dbName}${safeConnectionParams(config.connectionParams)}"
  }

  private[postgres] def safeConnectionParams(connectionParams: String) = {
    if (connectionParams.startsWith("?") || connectionParams.isEmpty) {
      connectionParams
    } else {
      s"?$connectionParams"
    }
  }
}