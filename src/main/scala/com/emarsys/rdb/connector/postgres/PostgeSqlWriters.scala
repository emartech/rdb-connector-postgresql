package com.emarsys.rdb.connector.postgres

import com.emarsys.rdb.connector.common.defaults.SqlWriter.createEscapeQuoter
import com.emarsys.rdb.connector.common.defaults.{DefaultSqlWriters, SqlWriter}
import com.emarsys.rdb.connector.common.models.SimpleSelect._

object PostgeSqlWriters extends DefaultSqlWriters {
  override implicit lazy val tableNameWriter: SqlWriter[TableName] = SqlWriter.createTableNameWriter("\"", "\\")
  override implicit lazy val fieldNameWriter: SqlWriter[FieldName] = postgreSqlCreateFieldNameWriter("\"", "\\")


  def postgreSqlCreateFieldNameWriter(symbol: String, escape: String): SqlWriter[FieldName] =
    (fieldName: FieldName) => createEscapeQuoter(symbol, escape, fieldName.f.toLowerCase)
}
