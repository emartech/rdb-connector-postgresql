package com.emarsys.rdb.connector.postgresql

import java.io.{File, PrintWriter}
import java.util.Properties

import com.emarsys.rdb.connector.common.ConnectorResponse
import com.emarsys.rdb.connector.common.models.Errors.{ConnectorError, ErrorWithMessage}
import com.emarsys.rdb.connector.common.models.{ConnectionConfig, Connector, ConnectorCompanion, MetaData}
import com.emarsys.rdb.connector.postgresql.PostgreSqlConnector.PostgreSqlConnectorConfig
import slick.jdbc.PostgresProfile.api._
import slick.util.AsyncExecutor

import scala.concurrent.duration._
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
                                         certificate: String,
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

    if (!checkSsl(config.connectionParams)) {
      Future.successful(Left(ErrorWithMessage("SSL Error")))
    } else {

      val prop = new Properties()
      prop.setProperty("ssl", "true")
      prop.setProperty("sslmode", "verify-ca")
      prop.setProperty("loggerLevel", "OFF")
      prop.setProperty("sslrootcert", createTempFile(config.certificate))

      val db = Database.forURL(
        url = createUrl(config),
        driver = "org.postgresql.Driver",
        user = config.dbUser,
        password = config.dbPassword,
        prop = prop,
        executor = executor
      )

      checkConnection(db).map[Either[ConnectorError, PostgreSqlConnector]] {
        _ => Right(new PostgreSqlConnector(db, connectorConfig))
      }.recover {
        case _ => Left(ErrorWithMessage("Cannot connect to the sql server"))
      }
    }
  }

  override def meta() = MetaData("\"", "'", "\\")

  private def createTempFile(certificate: String): String = {
    val temp: File = File.createTempFile("root", ".crt")
    new PrintWriter(temp) {
      write(certificate)
      close
    }
    temp.deleteOnExit()
    temp.getAbsolutePath
  }

  private[postgresql] def checkSsl(connectionParams: String): Boolean = {
    !connectionParams.matches(".*ssl=false.*") &&
      !connectionParams.matches(".*sslmode=.*") &&
      !connectionParams.matches(".*sslrootcert=.*")
  }

  private def checkConnection(db: Database)(implicit executionContext: ExecutionContext): Future[Unit] = {
    db.run(sql"SELECT 1".as[(String)]).map(_ => {})
  }

  private[postgresql] def createUrl(config: PostgreSqlConnectionConfig) = {
    s"jdbc:postgresql://${config.host}:${config.port}/${config.dbName}${safeConnectionParams(config.connectionParams)}"
  }

  private def safeConnectionParams(connectionParams: String) = {
    if (connectionParams.startsWith("?") || connectionParams.isEmpty) {
      connectionParams
    } else {
      s"?$connectionParams"
    }
  }
}