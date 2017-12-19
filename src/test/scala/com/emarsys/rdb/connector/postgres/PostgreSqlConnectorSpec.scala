package com.emarsys.rdb.connector.postgres

import com.emarsys.rdb.connector.common.models.MetaData
import com.emarsys.rdb.connector.postgres.PostgreSqlConnector.PostgreSqlConnectionConfig
import org.scalatest.{Matchers, WordSpecLike}

class PostgreSqlConnectorSpec extends WordSpecLike with Matchers{

  "PostgreSqlConnectorSpec" when {

    "#createUrl" should {

      val exampleConnection = PostgreSqlConnectionConfig(
        host = "host",
        port = 123,
        dbName = "database",
        dbUser = "me",
        dbPassword = "secret",
        certificate = "cert",
        connectionParams = "?param1=asd"
      )

      "creates url from config" in {
        PostgreSqlConnector.createUrl(exampleConnection) shouldBe "jdbc:postgresql://host:123/database?param1=asd"
      }

      "handle missing ? in params" in {
        val exampleWithoutMark = exampleConnection.copy(connectionParams = "param1=asd")
        PostgreSqlConnector.createUrl(exampleWithoutMark) shouldBe "jdbc:postgresql://host:123/database?param1=asd"
      }

      "handle empty params" in {
        val exampleWithoutMark = exampleConnection.copy(connectionParams = "")
        PostgreSqlConnector.createUrl(exampleWithoutMark) shouldBe "jdbc:postgresql://host:123/database"
      }


    }

    "#checkSsl" should {

      "return true if empty connection params" in {
        PostgreSqlConnector.checkSsl("") shouldBe true
      }

      "return true if not contains ssl=false or sslrootcert or sslmode" in {
        PostgreSqlConnector.checkSsl("?param1=param&param2=param2") shouldBe true
      }

      "return false if contains ssl=false" in {
        PostgreSqlConnector.checkSsl("?param1=param&ssl=false&param2=param2") shouldBe false
      }

      "return false if contains sslrootcert" in {
        PostgreSqlConnector.checkSsl("?param1=param&sslrootcert=false&param2=param2") shouldBe false
      }

      "return false if contains sslmode" in {
        PostgreSqlConnector.checkSsl("?param1=param&sslmode=false&param2=param2") shouldBe false
      }

    }

    "#meta" should {

      "return postgresql qouters" in {
        PostgreSqlConnector.meta() shouldEqual MetaData(nameQuoter = "\"", valueQuoter = "'", escape = "\\")
      }

    }

  }
}
