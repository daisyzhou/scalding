package com.twitter.scalding

import scala.collection.mutable.Buffer

import org.specs._
import cascading.tuple.{Fields, Tuple}
import java.lang.IllegalArgumentException

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

    "error helpfully when a source is not in the map for test buffers" in {
//      {
//        testTapFactory.createTap(Read)(testMode)
//      } must throwA[RuntimeException] like {
//        case e: RuntimeException => e.getMessage.contains("HELLOOO")
//      }
      // TODO: Figure out how to inspect exception messages in specs2.
      try {
        testTapFactory.createTap(Read)(testMode)
        fail("Should have thrown an exception")
      } catch {
        case iae: IllegalArgumentException => assert(iae.getMessage.contains("HELLO"))
        case e: Exception =>
          fail("Should throw an IllegalArgumentException, instead threw: %s".format(e))
      }
    }

  }

}
