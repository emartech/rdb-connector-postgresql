package com.emarsys.rdb.connector.postgresql

import java.io.{File, PrintWriter}
import java.util.UUID

import com.emarsys.rdb.connector.common.ConnectorResponse
import com.emarsys.rdb.connector.common.models.Errors.{ConnectionConfigError, ConnectionError, ConnectorError}
import com.emarsys.rdb.connector.common.models._
import com.emarsys.rdb.connector.postgresql.PostgreSqlConnector.{PostgreSqlConnectionConfig, PostgreSqlConnectorConfig}
import com.typesafe.config.{ConfigFactory, ConfigValueFactory}
import slick.jdbc.PostgresProfile.api._
import slick.util.AsyncExecutor

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try

class PostgreSqlConnector(
    protected val db: Database,
    protected val connectorConfig: PostgreSqlConnectorConfig,
    protected val poolName: String,
    protected val schemaName: String
)(implicit val executionContext: ExecutionContext)
    extends Connector
    with PostgreSqlErrorHandling
    with PostgreSqlTestConnection
    with PostgreSqlMetadata
    with PostgreSqlSimpleSelect
    with PostgreSqlRawSelect
    with PostgreSqlIsOptimized
    with PostgreSqlRawDataManipulation {

  override def close(): Future[Unit] = {
    db.shutdown
  }

  override def innerMetrics(): String = {
    import java.lang.management.ManagementFactory

    import com.zaxxer.hikari.HikariPoolMXBean
    import javax.management.{JMX, ObjectName}
    Try {
      val mBeanServer = ManagementFactory.getPlatformMBeanServer
      val poolObjectName =
        new ObjectName(s"com.zaxxer.hikari:type=Pool ($poolName)")
      val poolProxy = JMX.newMXBeanProxy(mBeanServer, poolObjectName, classOf[HikariPoolMXBean])

      s"""{
         |"activeConnections": ${poolProxy.getActiveConnections},
         |"idleConnections": ${poolProxy.getIdleConnections},
         |"threadAwaitingConnections": ${poolProxy.getThreadsAwaitingConnection},
         |"totalConnections": ${poolProxy.getTotalConnections}
         |}""".stripMargin
    }.getOrElse(super.innerMetrics)
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

  case class PostgreSqlConnectorConfig(queryTimeout: FiniteDuration, streamChunkSize: Int)

}

trait PostgreSqlConnectorTrait extends ConnectorCompanion {
  private[postgresql] val defaultConfig = PostgreSqlConnectorConfig(
    queryTimeout = 20.minutes,
    streamChunkSize = 5000
  )

  override def meta() = MetaData("\"", "'", "\\")

  def apply(config: PostgreSqlConnectionConfig, connectorConfig: PostgreSqlConnectorConfig = defaultConfig)(
      executor: AsyncExecutor
  )(implicit executionContext: ExecutionContext): ConnectorResponse[PostgreSqlConnector] = {
    val poolName = UUID.randomUUID.toString

    if (!checkSsl(config.connectionParams)) {
      Future.successful(Left(ConnectionConfigError("SSL Error")))
    } else {
      configureDb(config, poolName).flatMap(checkConnection(connectorConfig, poolName, config))
    }
  }

  private def configureDb(config: PostgreSqlConnector.PostgreSqlConnectionConfig, poolName: String) = {
    val customDbConf = ConfigFactory
      .load()
      .withValue("postgredb.poolName", ConfigValueFactory.fromAnyRef(poolName))
      .withValue("postgredb.registerMbeans", ConfigValueFactory.fromAnyRef(true))
      .withValue("postgredb.properties.url", ConfigValueFactory.fromAnyRef(createUrl(config)))
      .withValue("postgredb.properties.user", ConfigValueFactory.fromAnyRef(config.dbUser))
      .withValue("postgredb.properties.password", ConfigValueFactory.fromAnyRef(config.dbPassword))
      .withValue("postgredb.properties.driver", ConfigValueFactory.fromAnyRef("org.postgresql.Driver"))
      .withValue("postgredb.properties.properties.ssl", ConfigValueFactory.fromAnyRef("true"))
      .withValue("postgredb.properties.properties.sslmode", ConfigValueFactory.fromAnyRef("verify-ca"))
      .withValue("postgredb.properties.properties.loggerLevel", ConfigValueFactory.fromAnyRef("OFF"))
      .withValue(
        "postgredb.properties.properties.sslrootcert",
        ConfigValueFactory.fromAnyRef(createTempFile(config.certificate))
      )

    Future.successful(Database.forConfig("postgredb", customDbConf))
  }

  private def checkConnection(
      connectorConfig: PostgreSqlConnectorConfig,
      poolName: String,
      config: PostgreSqlConnectionConfig
  )(db: Database)(implicit ec: ExecutionContext) =
    isConnectionAvailable(db) map [Either[ConnectorError, PostgreSqlConnector]] { _ =>
      Right(new PostgreSqlConnector(db, connectorConfig, poolName, createSchemaName(config)))
    } recover {
      case ex => Left(ConnectionError(ex))
    } map {
      case Left(e) =>
        db.shutdown
        Left(e)
      case r => r
    }

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

  private def isConnectionAvailable(db: Database)(implicit executionContext: ExecutionContext): Future[Unit] = {
    db.run(sql"SELECT 1".as[(String)]).map(_ => {})
  }

  private[postgresql] def createUrl(config: PostgreSqlConnectionConfig) = {
    s"jdbc:postgresql://${config.host}:${config.port}/${config.dbName}${safeConnectionParams(config.connectionParams)}"
  }

  private def createSchemaName(config: PostgreSqlConnectionConfig) = {
    config.connectionParams
      .split("&")
      .toList
      .find(_.startsWith("currentSchema="))
      .flatMap(_.split("=").toList.tail.headOption)
      .getOrElse("public")
  }

  private def safeConnectionParams(connectionParams: String) = {
    if (connectionParams.startsWith("?") || connectionParams.isEmpty) {
      connectionParams
    } else {
      s"?$connectionParams"
    }
  }
}
