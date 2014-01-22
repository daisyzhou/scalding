package com.twitter.scalding

import org.specs.Specification


class SimpleTestJob(args: Args) extends Job(args) {
  Tsv(args("input")).read.write(Tsv(args("output")))
}

class JobTestTest extends Specification {
  // TODO: write test using JobTest that tests sources error helpfully.

  "A JobTest should" {
    "error helpfully when a source in the job doesn't have a corresponding .source call" in {
      val testInput = List(("a", 1), ("b", 2))
      
      def runJobTest = JobTest(new SimpleTestJob(_))
        .arg("input", "input")
        .arg("output", "output")
        .source(Tsv("different-input"), testInput)
        .sink[(String, Int)](Tsv("output")){ outBuf => { /** No-op **/ }}
        .run

      // runJobTest must throwA[IllegalArgumentException]

      try {
        runJobTest
        fail("Should have thrown an exception")
      } catch {
        case iae: IllegalArgumentException => iae.getMessage must contain("HELLO")
        case e: Exception =>
          fail("Should have thrown an IllegalArgumentException, instead threw %s".format(e))
      }
    }
  }
}
