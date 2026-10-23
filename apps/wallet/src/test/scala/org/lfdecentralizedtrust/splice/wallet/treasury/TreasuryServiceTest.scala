// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package org.lfdecentralizedtrust.splice.wallet.treasury

import com.digitalasset.canton.{BaseTest, HasActorSystem, HasExecutionContext}
import org.apache.pekko.stream.Materializer
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.time.{Millis, Seconds, Span}
import org.scalatest.wordspec.AnyWordSpec

import java.util.concurrent.TimeoutException
import scala.concurrent.Promise
import scala.concurrent.duration.*

class TreasuryServiceTest
    extends AnyWordSpec
    with BaseTest
    with ScalaFutures
    with HasActorSystem
    with HasExecutionContext {

  implicit override val patienceConfig: PatienceConfig =
    PatienceConfig(timeout = Span(20, Seconds), interval = Span(20, Millis))

  private lazy val mat: Materializer = Materializer(actorSystem)

  "awaitIngestionOrTimeout" should {
    "complete when ingestion finishes before the timeout" in {
      val ingested = Promise[Unit]()
      val result = TreasuryService.awaitIngestionOrTimeout(
        ingested.future,
        5.seconds,
        mat,
        offset = 11L,
        logger,
      )
      ingested.success(())
      result.futureValue shouldBe ()
    }

    "fail when ingestion does not arrive in time" in {
      val ex = loggerFactory.assertLogs(
        TreasuryService
          .awaitIngestionOrTimeout(
            Promise[Unit]().future,
            300.millis,
            mat,
            offset = 7L,
            logger,
          )
          .failed
          .futureValue,
        _.warningMessage should include("Timed out").and(include("offset 7")),
      )
      ex shouldBe a[TimeoutException]
      ex.getMessage should include("offset 7")
    }

    "propagate an ingestion failure" in {
      val ingested = Promise[Unit]()
      val result = TreasuryService.awaitIngestionOrTimeout(
        ingested.future,
        5.seconds,
        mat,
        offset = 9L,
        logger,
      )
      val failure = new IllegalStateException("ingest failed")
      ingested.failure(failure)
      result.failed.futureValue shouldBe failure
    }
  }
}
