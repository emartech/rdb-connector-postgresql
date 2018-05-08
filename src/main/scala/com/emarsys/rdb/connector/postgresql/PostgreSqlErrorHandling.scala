package com.emarsys.rdb.connector.postgresql

import com.emarsys.rdb.connector.common.models.Errors.{ConnectionError, ConnectorError}

trait PostgreSqlErrorHandling {
  self: PostgreSqlConnector =>

  protected def errorHandler[T](): PartialFunction[Throwable, Either[ConnectorError,T]] = {
    case ex: Exception => Left(ConnectionError(ex))
  }

}
