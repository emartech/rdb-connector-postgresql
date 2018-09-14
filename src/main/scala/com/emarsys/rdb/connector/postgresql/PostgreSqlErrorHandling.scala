package com.emarsys.rdb.connector.postgresql

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.emarsys.rdb.connector.common.models.Errors.{ConnectionError, ConnectorError, QueryTimeout}
import org.postgresql.util.PSQLException

trait PostgreSqlErrorHandling {
  self: PostgreSqlConnector =>

  val PSQL_STAT_QUERY_CANCELLED = "57014"

  private def errorHandler(): PartialFunction[Throwable, ConnectorError] = {
    case ex: PSQLException if ex.getSQLState == PSQL_STAT_QUERY_CANCELLED => QueryTimeout(ex.getMessage)
    case ex: Exception => ConnectionError(ex)
  }

  protected def eitherErrorHandler[T](): PartialFunction[Throwable, Either[ConnectorError, T]] =
    errorHandler andThen Left.apply

  protected def streamErrorHandler[A]: PartialFunction[Throwable, Source[A, NotUsed]] =
    errorHandler andThen Source.failed

}
