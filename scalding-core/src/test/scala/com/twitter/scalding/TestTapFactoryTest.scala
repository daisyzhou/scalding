package com.twitter.scalding

import scala.collection.mutable.Buffer

import org.specs._
import cascading.tuple.{Fields, Tuple}
import java.lang.IllegalArgumentException
import cascading.tap.Tap

class TestTapFactoryTest extends Specification {
  noDetailedDiffs()

  "A test tap created by TestTapFactory" should {
    // Source to use for this test.
    val testSource = new Tsv("path")

    // Map of sources to use when creating the tap-- does not contain testSource
    val emptySourceMap = Map[Source, Buffer[Tuple]]()
    def buffers(s: Source): Option[Buffer[Tuple]] = {
      if (emptySourceMap.contains(s)) {
        Some(emptySourceMap(s))
      } else {
        None
      }
    }

    val testFields = new Fields()

    val testMode = Test(buffers)
    val testTapFactory = TestTapFactory(testSource, testFields)

    def createIllegalTap(): Tap[Any, Any, Any] =
      testTapFactory.createTap(Read)(testMode).asInstanceOf[Tap[Any, Any, Any]]

    "error helpfully when a source is not in the map for test buffers" >> {
      createIllegalTap() must throwA[IllegalArgumentException].like {
        case iae: IllegalArgumentException =>
            iae.getMessage mustVerify(_.contains(testSource.toString))
      }
    }

  }

}