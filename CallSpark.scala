package com.local.instance.package.launchspark
import org.apache.spark.launcher.SparkLauncher

object CallSpark {
	BasicConfigurator.configure()
    val slParams = SLParams
    var startTime = 0L
	
    object LogHolder extends Serializable { // object to log within the executors
        @transient lazy val log = LogManager.getRootLogger
        log.setLevel(Level.INFO)
    }
	
	def runSparkJob(args: Array[String]): Unit ={
        startTime = System.currentTimeMillis
        var parsedInput = parseInput(args)
		var dataFromDB = readDB(parsedInput)
        startSpark(parsedInput, dataFromDB)
        val estimatedTime = System.currentTimeMillis - startTime
        LogHolder.log.info("estimatedTime (millis)=" + estimatedTime)
        return
    }

	
}
var sl = new SparkLauncher(env).
               setVerbose(true). // setting output
               setJavaHome("path/to/java"). 
               setSparkHome("/path/to/spark/home").
               setAppResource("path/to/submitted/jar").
               setMainClass("name.of.spark.job.class").
               setMaster("yarn or something else").
               setDeployMode("cluster/client/local")
			   
    @throws[Exception]
    def main(args: Array[String]): Unit = {
        val retVal = runSparkJob(args)
    }
