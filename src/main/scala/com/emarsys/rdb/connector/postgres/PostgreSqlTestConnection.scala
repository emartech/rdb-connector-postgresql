package com.emarsys.rdb.connector.postgres

import com.emarsys.rdb.connector.common.ConnectorResponse
import com.emarsys.rdb.connector.common.models.Errors.ErrorWithMessage

import slick.jdbc.PostgresProfile.api._

trait PostgreSqlTestConnection {
  self: PostgreSqlConnector =>

  override def testConnection(): ConnectorResponse[Unit] = {
    db.run(sql"SELECT 1".as[Int]).map(_ => Right()).recover { case _ =>
      Left(ErrorWithMessage("Cannot connect to the sql server"))
    }
  }
}
