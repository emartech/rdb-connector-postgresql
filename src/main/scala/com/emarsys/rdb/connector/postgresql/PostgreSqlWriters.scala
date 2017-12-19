package com.emarsys.rdb.connector.postgresql

import com.emarsys.rdb.connector.common.defaults.SqlWriter.createEscapeQuoter
import com.emarsys.rdb.connector.common.defaults.{DefaultSqlWriters, SqlWriter}
import com.emarsys.rdb.connector.common.models.SimpleSelect._

object PostgreSqlWriters extends DefaultSqlWriters {
  override implicit lazy val tableNameWriter: SqlWriter[TableName] = SqlWriter.createTableNameWriter("\"", "\\")
  override implicit lazy val fieldNameWriter: SqlWriter[FieldName] = postgreSqlCreateFieldNameWriter("\"", "\\")


  def postgreSqlCreateFieldNameWriter(symbol: String, escape: String): SqlWriter[FieldName] =
    (fieldName: FieldName) => createEscapeQuoter(symbol, escape, fieldName.f.toLowerCase)
}
