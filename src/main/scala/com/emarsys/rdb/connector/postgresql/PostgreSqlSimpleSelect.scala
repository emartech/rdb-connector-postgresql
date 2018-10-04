package com.emarsys.rdb.connector.postgresql

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.emarsys.rdb.connector.common.ConnectorResponse
import com.emarsys.rdb.connector.common.models.SimpleSelect
import com.emarsys.rdb.connector.common.defaults.SqlWriter._
import PostgreSqlWriters._

import scala.concurrent.duration.FiniteDuration

trait PostgreSqlSimpleSelect extends PostgreSqlStreamingQuery {
  self: PostgreSqlConnector =>

  override def simpleSelect(
      select: SimpleSelect,
      timeout: FiniteDuration
  ): ConnectorResponse[Source[Seq[String], NotUsed]] = {
    streamingQuery(timeout)(select.toSql)
  }
}
