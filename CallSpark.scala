package com.local.instance.package.launchspark
import org.apache.spark.launcher.SparkLauncher

object CallSpark {
    BasicConfigurator.configure()
    val parsedInput = ParsedInput
    var startTime = 0L
	
    object LogHolder extends Serializable {
        @transient lazy val log = LogManager.getRootLogger
        log.setLevel(Level.INFO)
    }

    @throws[Exception]
    def main(args: Array[String]): Unit = {
        runSparkJob(args)
    }
	
    def runSparkJob(args: Array[String]): Unit ={
        startTime = System.currentTimeMillis
        parseInput(args)
        readDB()
        startSpark()
        val estimatedTime = System.currentTimeMillis - startTime
        LogHolder.log.info("estimatedTime (millis)=" + estimatedTime)
        
    }
    //read input
    def parseInput(args : Array[String]): Unit = {
        val argList = args.toList
        def nextOption(list: List[String]) : Unit = {
            def isSwitch(s : String) = (s(0) == '-')
            list match {
                case Nil =>
                case "--mainClass" :: value :: tail => {
                    nextOption(tail)
                    parsedInput.mainClass = value
                }
                case "--appParam" :: value :: tail => {
                    nextOption(tail)
                    parsedInput.appJar = value
                }
                // as many cases as needed

                case _ :: tail => nextOption(tail)
            }
        }

        nextOption(argList)
    }
    
    def readDB() = {
        //read from DB and insert into ParsedInput
    }

	def startSpark(arguments: Array[String]): Unit = {
        val spark = setupSparkLauncher()
        // Launches a sub-process that will start the configured Spark application.
        val proc = spark.launch
        val inputStreamReaderRunnable = new InputStreamReaderRunnable(proc.getInputStream, "spark stdout stream")
        val inputThread = new Thread(inputStreamReaderRunnable, "LogStreamReader input")
        inputThread.start()

        val errorStreamReaderRunnable = new InputStreamReaderRunnable(proc.getErrorStream, "spark stderr stream")//T(proc.getErrorStream(), "error")
        val errorThread = new Thread(errorStreamReaderRunnable, "LogStreamReader error")
        errorThread.start()
        proc.waitFor()
    }
    
    private def setupSparkLauncher(): SparkLauncher = {
        val env: util.HashMap[String, String] = new util.HashMap[String, String]
        env.put("HADOOP_CONF_DIR", slParams.confPath)
        var sl = new SparkLauncher(env).
               setVerbose(true). // setting output
               setJavaHome("path/to/java"). 
               setSparkHome("/path/to/spark/home").
               setAppResource("path/to/submitted/jar").
               setMainClass("name.of.spark.job.class").
               setMaster("yarn or something else").
               setDeployMode("cluster/client/local")
        sl.addSparkArg("--principal", parsedInput.keytabUser).addSparkArg("--keytab", parsedInput.keytabPath)
        sl.setConf(SparkLauncher.DRIVER_MEMORY, parsedInput.driverMemory).setConf(SparkLauncher.EXECUTOR_MEMORY,parsedInput.executorMemory)
        if(parsedInput.overrideQuorum){
            sl.addSparkArg("--conf", parsedInput.quorum)
        }
        addAppArgs(spark)
        return spark
    }

    private def addAppArgs(spark: SparkLauncher) = {
        spark.addAppArgs("--cluster")
        spark.addAppArgs(slParams.cluster)
        spark.addAppArgs("--devEnv")
        spark.addAppArgs(slParams.devEnv)
        if(SLParams.appParam != null) {
            spark.addAppArgs("--appParam")
            spark.addAppArgs(SLParams.appParam)
        }
    }

    class InputStreamReaderRunnable(is: InputStream, nameInp: String) extends Runnable{
        var reader: BufferedReader = null
        var currentName = nameInp
        override def run(): Unit = {
            val inpStream = new InputStreamReader(is)
            reader = new BufferedReader(inpStream)
            try {
                var line = reader.readLine
                while ( {
                    line != null
                }) {
                    line = reader.readLine
                    println("output is from " + currentName + ":" + line)
                }
                inpStream.close()
                reader.close()
            } catch {
                case e: IOException =>
                    e.printStackTrace()
            }
        }
    }
}

			   

