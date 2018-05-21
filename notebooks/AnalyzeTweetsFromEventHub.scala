// Databricks notebook source
import org.apache.spark.eventhubs._
import java.time.Duration	
// Build connection string with the above information
//val connectionString = ConnectionStringBuilder("Endpoint=sb://sbuxeventhubns---servicebus.windows.net.rproxy.goskope.com/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=iD21BZ3NNvrWN07cbr+c14yWRmdLxr3vryF1nSylJMc=")
  //.setEventHubName("sbuxeventhub")
 // .build

val namespaceName = "sbuxeventhubns"
val eventHubName = "sbuxeventhub"
val sasKeyName = "RootManageSharedAccessKey"
val sasKey = "######"
val connStr = ConnectionStringBuilder()
            .setNamespaceName(namespaceName)
            .setEventHubName(eventHubName)
            .setSasKeyName(sasKeyName)
            .setSasKey(sasKey)
            .build


val customEventhubParameters =
  EventHubsConf(connStr)
  .setMaxEventsPerTrigger(5)
  .setReceiverTimeout(Duration.ofSeconds(300))

val incomingStream = spark.readStream.format("eventhubs").options(customEventhubParameters.toMap).load()

incomingStream.printSchema

// Sending the incoming stream into the console.
// Data comes in batches!
incomingStream.writeStream.outputMode("append").format("console").option("truncate", false).start().awaitTermination()