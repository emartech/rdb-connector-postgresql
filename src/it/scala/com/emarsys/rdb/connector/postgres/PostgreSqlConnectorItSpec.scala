package com.emarsys.rdb.connector.postgres

import com.emarsys.rdb.connector.common.models.Errors.ErrorWithMessage
import com.emarsys.rdb.connector.postgres.utils.TestHelper
import org.scalatest.{Matchers, WordSpecLike}
import slick.util.AsyncExecutor

import scala.concurrent.{Await, Future}
import concurrent.duration._

class PostgreSqlConnectorItSpec extends WordSpecLike with Matchers {
  "PostgreSqlConnector" when {

    implicit val executionContext = scala.concurrent.ExecutionContext.Implicits.global
    val executor = AsyncExecutor.default()

    /*"create connector" should {

      "return error if not use ssl" in {
        var connectionParams = TestHelper.TEST_CONNECTION_CONFIG.connectionParams
        if (!connectionParams.isEmpty) {
          connectionParams += "&"
        }
        connectionParams += "ssl=false"

        val badConnection = TestHelper.TEST_CONNECTION_CONFIG.copy(connectionParams = connectionParams)
        val connection = Await.result(PostgreSqlConnector(badConnection)(executor), 3.seconds)
        connection shouldBe Left(ErrorWithMessage("SSL Error"))
      }

    }*/

    "#testConnection" should {

      "return ok in happy case" in {
        val connection = Await.result(PostgreSqlConnector(TestHelper.TEST_CONNECTION_CONFIG)(executor), 3.seconds).toOption.get
        val result = Await.result(connection.testConnection(), 3.seconds)
        result shouldBe Right()
        connection.close()
      }

      "return error if cant connect" in {
        val badConnection = TestHelper.TEST_CONNECTION_CONFIG.copy(host = "asd.asd.asd")
        val connection = Await.result(PostgreSqlConnector(badConnection)(executor), 3.seconds).toOption.get
        val result = Await.result(connection.testConnection(), 3.seconds)
        result shouldBe Left(ErrorWithMessage("Cannot connect to the sql server"))
      }

    }
  }
}
