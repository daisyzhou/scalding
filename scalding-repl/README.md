# Scalding REPL

The Scalding REPL is an extension of the Scala REPL, with added functionality
that allows you to interactively experiment with Scalding flows.

If Scalding is installed in SCALDING_HOME, then the REPL is launched via:
`${SCALDING_HOME}/scripts/scald-repl.sh [hadoopGenericOptions] --local|--hdfs`

The repl imports com.twitter.scalding._ as well as some REPL specific implicits
from com.twitter.scalding.ReplImplicits. Within the REPL, you can define sources
and operate on Pipes as you would in a normal Scalding job.

Pipes can be run in local or hdfs modes by calling the 'run()' method on it. Pipes can
only be run once in the REPL. To run in hdfs mode, you need to have Hadoop installed, 
and available in your path.

## Tutorial0 in REPL Form
We will show you how to define and run a Scalding job equivalent to the one
defined in tutorial/Tutorial0.scala

From the root directory, where Scalding is installed, you can launch the REPL with:
`./scripts/scald-repl.sh --local`

You will see some lovely ASCII art:

     (                                           
     )\ )            (   (                       
    (()/(         )  )\  )\ )  (          (  (   
     /(_)) (   ( /( ((_)(()/( )\   (     )\))(  
    (_))   )\  )(_)) _   ((_)((_)  )\ ) ((_))\  
    / __| ((_)((_)_ | |  _| | (_) _(_/(  (()(_) 
    \__ \/ _| / _` || |/ _` | | || ' \))/ _` |  
    |___/\__| \__,_||_|\__,_| |_||_||_| \__, |  
                                    |___/   

You can then define vals to represent the inputs and outputs you want:

    scalding> val input = TextLine("tutorial/data/hello.txt")
    input: com.twitter.scalding.TextLine = com.twitter.scalding.TextLineWrappedArray(tutorial/data/hello.txt)

    scalding> val output = TextLine("tutorial/data/output0.txt")
    output: com.twitter.scalding.TextLine = com.twitter.scalding.TextLineWrappedArray(tutorial/data/output0.txt)

You can then define a pipe that reads the source and writes to the sink. 

    scalding> val pipe = input.read.write(output)
    pipe: cascading.pipe.Pipe = Pipe(com.twitter.scalding.TextLineWrappedArray(tutorial/data/hello.txt))

And then run it! (But only once.)

    scalding> pipe.run
    13/12/10 20:35:56 INFO property.AppProps: using app.id: 23A7A455F26F4FB39A0101465F4EDE45
    13/12/10 20:35:56 INFO util.Version: Concurrent, Inc - Cascading 2.2.0
    13/12/10 20:35:56 INFO flow.Flow: [ScaldingShell] starting
    13/12/10 20:35:56 INFO flow.Flow: [ScaldingShell]  source: FileTap["TextLine[['offset', 'line']->[ALL]]"]["tutorial/data/hello.txt"]
    13/12/10 20:35:56 INFO flow.Flow: [ScaldingShell]  sink: FileTap["TextLine[['offset', 'line']->[ALL]]"]["tutorial/data/output0.txt"]
    13/12/10 20:35:56 INFO flow.Flow: [ScaldingShell]  parallel execution is enabled: true
    13/12/10 20:35:56 INFO flow.Flow: [ScaldingShell]  starting jobs: 1
    13/12/10 20:35:56 INFO flow.Flow: [ScaldingShell]  allocating threads: 1
    13/12/10 20:35:56 INFO flow.FlowStep: [ScaldingShell] starting step: local

