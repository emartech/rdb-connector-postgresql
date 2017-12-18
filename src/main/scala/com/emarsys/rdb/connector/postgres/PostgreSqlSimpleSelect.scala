package com.emarsys.rdb.connector.postgres

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.emarsys.rdb.connector.common.ConnectorResponse
import com.emarsys.rdb.connector.common.models.SimpleSelect

import com.emarsys.rdb.connector.common.defaults.SqlWriter._
import PostgreSqlWriters._

trait PostgreSqlSimpleSelect extends PostgreSqlStreamingQuery {
  self: PostgreSqlConnector =>

  override def simpleSelect(select: SimpleSelect): ConnectorResponse[Source[Seq[String], NotUsed]] = {
    streamingQuery(select.toSql)
  }
}
