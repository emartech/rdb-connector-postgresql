package com.emarsys.rdb.connector.postgresql

import com.emarsys.rdb.connector.common.models.Errors.{ConnectionConfigError, ConnectionError}
import com.emarsys.rdb.connector.postgresql.utils.TestHelper
import org.scalatest.{Matchers, WordSpecLike}
import slick.util.AsyncExecutor

import scala.concurrent.{Await, Future}
import concurrent.duration._

class PostgreSqlConnectorItSpec extends WordSpecLike with Matchers {
  "PostgreSqlConnector" when {

    implicit val executionContext = scala.concurrent.ExecutionContext.Implicits.global
    val executor = AsyncExecutor.default()

    "create connector" should {

      "connect success" in {
        val connectorEither = Await.result(PostgreSqlConnector(TestHelper.TEST_CONNECTION_CONFIG)(AsyncExecutor.default()), 5.seconds)

        connectorEither shouldBe a[Right[_, _]]

        connectorEither.right.get.close()
      }

      "connect ok when hikari enabled" in {

        object PostgresWithHikari extends PostgreSqlConnectorTrait {
          override val useHikari: Boolean = true
        }

        val connectorEither = Await.result(PostgresWithHikari(TestHelper.TEST_CONNECTION_CONFIG)(AsyncExecutor.default()), 5.seconds)

        connectorEither shouldBe a[Right[_, _]]
      }

      "connect fail when ssl disabled" in {
        val conn = TestHelper.TEST_CONNECTION_CONFIG.copy(
          connectionParams = "ssl=false"
        )
        val connectorEither = Await.result(PostgreSqlConnector(conn)(AsyncExecutor.default()), 5.seconds)

        connectorEither shouldBe Left(ConnectionConfigError("SSL Error"))
      }

      "connect fail when sslrootcert modified" in {
        val conn = TestHelper.TEST_CONNECTION_CONFIG.copy(
          connectionParams = "sslrootcert=/root.crt"
        )
        val connectorEither = Await.result(PostgreSqlConnector(conn)(AsyncExecutor.default()), 5.seconds)

        connectorEither shouldBe Left(ConnectionConfigError("SSL Error"))
      }

      "connect fail when sslmode modified" in {
        val conn = TestHelper.TEST_CONNECTION_CONFIG.copy(
          connectionParams = "sslmode=disable"
        )
        val connectorEither = Await.result(PostgreSqlConnector(conn)(AsyncExecutor.default()), 5.seconds)

        connectorEither shouldBe Left(ConnectionConfigError("SSL Error"))
      }

      "connect fail when wrong certificate" in {
        val conn = TestHelper.TEST_CONNECTION_CONFIG.copy(certificate = "")
        val connectorEither = Await.result(PostgreSqlConnector(conn)(AsyncExecutor.default()), 5.seconds)

        connectorEither shouldBe a[Left[_, _]]
        connectorEither.left.get shouldBe a[ConnectionError]
      }

      "connect fail when wrong host" in {
        val conn = TestHelper.TEST_CONNECTION_CONFIG.copy(host = "wrong")
        val connectorEither = Await.result(PostgreSqlConnector(conn)(AsyncExecutor.default()), 5.seconds)

        connectorEither shouldBe a[Left[_, _]]
        connectorEither.left.get shouldBe a[ConnectionError]
      }

      "connect fail when wrong user" in {
        val conn = TestHelper.TEST_CONNECTION_CONFIG.copy(dbUser = "")
        val connectorEither = Await.result(PostgreSqlConnector(conn)(AsyncExecutor.default()), 5.seconds)

        connectorEither shouldBe a[Left[_, _]]
        connectorEither.left.get shouldBe a[ConnectionError]
      }

      "connect fail when wrong password" in {
        val conn = TestHelper.TEST_CONNECTION_CONFIG.copy(dbPassword = "")
        val connectorEither = Await.result(PostgreSqlConnector(conn)(AsyncExecutor.default()), 5.seconds)

        connectorEither shouldBe a[Left[_, _]]
        connectorEither.left.get shouldBe a[ConnectionError]
      }

    }

    "#testConnection" should {

      "success" in {
        val connection = Await.result(PostgreSqlConnector(TestHelper.TEST_CONNECTION_CONFIG)(executor), 3.seconds).toOption.get
        val result = Await.result(connection.testConnection(), 3.seconds)
        result shouldBe Right()
        connection.close()
      }

    }
  }
}
