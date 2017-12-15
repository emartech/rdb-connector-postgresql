package com.emarsys.rdb.connector.postgres

import akka.actor.ActorSystem
import akka.stream.{ActorMaterializer, Materializer}
import akka.testkit.TestKit
import com.emarsys.rdb.connector.postgres.utils.SelectDbInitHelper
import com.emarsys.rdb.connector.test.SimpleSelectItSpec

import concurrent.duration._

class PostgreSqlSimpleSelectItSpec extends TestKit(ActorSystem()) with SimpleSelectItSpec with SelectDbInitHelper {

  override implicit val materializer: Materializer = ActorMaterializer()

  override val awaitTimeout = 15.seconds

  override def afterAll(): Unit = {
    system.terminate()
    super.afterAll()
  }

}
