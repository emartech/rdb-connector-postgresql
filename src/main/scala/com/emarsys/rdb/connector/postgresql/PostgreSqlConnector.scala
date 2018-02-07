package com.emarsys.rdb.connector.postgresql

import java.io.{File, PrintWriter}
import java.util.Properties

import com.emarsys.rdb.connector.common.ConnectorResponse
import com.emarsys.rdb.connector.common.models.Errors.{ConnectorError, ErrorWithMessage}
import com.emarsys.rdb.connector.common.models._
import com.emarsys.rdb.connector.postgresql.PostgreSqlConnector.{PostgreSqlConnectionConfig, PostgreSqlConnectorConfig}
import com.typesafe.config.{ConfigFactory, ConfigValueFactory}
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

object PostgreSqlConnector extends PostgreSqlConnectorTrait {

  case class PostgreSqlConnectionConfig(
                                         host: String,
                                         port: Int,
                                         dbName: String,
                                         dbUser: String,
                                         dbPassword: String,
                                         certificate: String,
                                         connectionParams: String
                                       ) extends ConnectionConfig {
    override def toCommonFormat: CommonConnectionReadableData = {
      CommonConnectionReadableData("postgres", s"$host:$port", dbName, dbUser)
    }
  }

  case class PostgreSqlConnectorConfig(
                                        queryTimeout: FiniteDuration,
                                        streamChunkSize: Int
                                      )

}

trait PostgreSqlConnectorTrait extends ConnectorCompanion {

  private val defaultConfig = PostgreSqlConnectorConfig(
    queryTimeout = 20.minutes,
    streamChunkSize = 5000
  )

  val useHikari: Boolean = Config.db.useHikari

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

      val db = if(!useHikari) {
        val prop = new Properties()
        prop.setProperty("ssl", "true")
        prop.setProperty("sslmode", "verify-ca")
        prop.setProperty("loggerLevel", "OFF")
        prop.setProperty("sslrootcert", createTempFile(config.certificate))

        Database.forURL(
          url = createUrl(config),
          driver = "org.postgresql.Driver",
          user = config.dbUser,
          password = config.dbPassword,
          prop = prop,
          executor = executor
        )
      } else {
        val customDbConf = ConfigFactory.load()
          .withValue("postgredb.properties.url", ConfigValueFactory.fromAnyRef(createUrl(config)))
          .withValue("postgredb.properties.user", ConfigValueFactory.fromAnyRef(config.dbUser))
          .withValue("postgredb.properties.password", ConfigValueFactory.fromAnyRef(config.dbPassword))
          .withValue("postgredb.properties.driver", ConfigValueFactory.fromAnyRef("org.postgresql.Driver"))
          .withValue("postgredb.properties.properties.ssl", ConfigValueFactory.fromAnyRef("true"))
          .withValue("postgredb.properties.properties.sslmode", ConfigValueFactory.fromAnyRef("verify-ca"))
          .withValue("postgredb.properties.properties.loggerLevel", ConfigValueFactory.fromAnyRef("OFF"))
          .withValue("postgredb.properties.properties.sslrootcert", ConfigValueFactory.fromAnyRef(createTempFile(config.certificate)))
        Database.forConfig("postgredb", customDbConf)
      }

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