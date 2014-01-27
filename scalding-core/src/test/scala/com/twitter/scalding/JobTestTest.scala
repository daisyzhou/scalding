package com.twitter.scalding

import org.specs.Specification


class SimpleTestJob(args: Args) extends Job(args) {
  Tsv(args("input")).read.write(Tsv(args("output")))
}

class JobTestTest extends Specification {
  // TODO: write test using JobTest that tests sources error helpfully.

  "A JobTest" should {
    "error helpfully when a source in the job doesn't have a corresponding .source call" in {
      val testInput: List[(String, Int)] = List(("a", 1), ("b", 2))

      // A method that runs a JobTest where the sources don't match.
      def runJobTest() = JobTest(new SimpleTestJob(_))
        .arg("input", "input")
        .arg("output", "output")
        .source(Tsv("different-input"), testInput)
        .sink[(String, Int)](Tsv("output")){ outBuf => { assert(outBuf == testInput) }}
        .run

      runJobTest() must throwA[IllegalArgumentException].like {
        case iae: IllegalArgumentException => iae.getMessage mustVerify(_.contains("HELLO"))
      }
    }
  }
}
