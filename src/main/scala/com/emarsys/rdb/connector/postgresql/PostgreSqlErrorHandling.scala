package com.emarsys.rdb.connector.postgresql

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.emarsys.rdb.connector.common.models.Errors._
import org.postgresql.util.PSQLException

trait PostgreSqlErrorHandling {
  self: PostgreSqlConnector =>

  val PSQL_STATE_QUERY_CANCELLED    = "57014"
  val PSQL_STATE_SYNTAX_ERROR       = "42601"
  val PSQL_STATE_PERMISSION_DENIED  = "42501"
  val PSQL_STATE_RELATION_NOT_FOUND = "42P01"

  private def errorHandler(): PartialFunction[Throwable, ConnectorError] = {
    case ex: PSQLException if ex.getSQLState == PSQL_STATE_QUERY_CANCELLED    => QueryTimeout(ex.getMessage)
    case ex: PSQLException if ex.getSQLState == PSQL_STATE_SYNTAX_ERROR       => SqlSyntaxError(ex.getMessage)
    case ex: PSQLException if ex.getSQLState == PSQL_STATE_PERMISSION_DENIED  => AccessDeniedError(ex.getMessage)
    case ex: PSQLException if ex.getSQLState == PSQL_STATE_RELATION_NOT_FOUND => TableNotFound(ex.getMessage)
    case ex: Exception                                                        => ConnectionError(ex)
  }

  protected def eitherErrorHandler[T](): PartialFunction[Throwable, Either[ConnectorError, T]] =
    errorHandler andThen Left.apply

  protected def streamErrorHandler[A]: PartialFunction[Throwable, Source[A, NotUsed]] =
    errorHandler andThen Source.failed

}
