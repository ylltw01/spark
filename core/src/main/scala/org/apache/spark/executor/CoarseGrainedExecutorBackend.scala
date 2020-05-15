/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.executor

import java.io.{BufferedInputStream, FileInputStream}
import java.net.URL
import java.nio.ByteBuffer
import java.util.Locale
import java.util.concurrent.atomic.AtomicBoolean

import scala.collection.mutable
import scala.util.{Failure, Success}
import scala.util.control.NonFatal

import com.fasterxml.jackson.databind.exc.MismatchedInputException
import org.json4s.DefaultFormats
import org.json4s.JsonAST.JArray
import org.json4s.MappingException
import org.json4s.jackson.JsonMethods._

import org.apache.spark._
import org.apache.spark.TaskState.TaskState
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.deploy.worker.WorkerWatcher
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config._
import org.apache.spark.rpc._
import org.apache.spark.scheduler.{ExecutorLossReason, TaskDescription}
import org.apache.spark.scheduler.cluster.CoarseGrainedClusterMessages._
import org.apache.spark.serializer.SerializerInstance
import org.apache.spark.util.{ThreadUtils, Utils}

// Executor container 入口类
private[spark] class CoarseGrainedExecutorBackend(
    override val rpcEnv: RpcEnv,
    driverUrl: String,
    executorId: String,
    hostname: String,
    cores: Int,
    userClassPath: Seq[URL],
    env: SparkEnv,
    resourcesFile: Option[String])
  extends ThreadSafeRpcEndpoint with ExecutorBackend with Logging {

  private implicit val formats = DefaultFormats

  private[this] val stopping = new AtomicBoolean(false)
  var executor: Executor = null
  @volatile var driver: Option[RpcEndpointRef] = None

  // If this CoarseGrainedExecutorBackend is changed to support multiple threads, then this may need
  // to be changed so that we don't share the serializer instance across threads
  private[this] val ser: SerializerInstance = env.closureSerializer.newInstance()

  override def onStart() {
    logInfo("Connecting to driver: " + driverUrl)
    val resources = parseOrFindResources(resourcesFile)
    rpcEnv.asyncSetupEndpointRefByURI(driverUrl).flatMap { ref =>
      // This is a very fast action so we can use "ThreadUtils.sameThread"
      driver = Some(ref)
      ref.ask[Boolean](RegisterExecutor(executorId, self, hostname, cores, extractLogUrls,
        extractAttributes, resources))
    }(ThreadUtils.sameThread).onComplete {
      // This is a very fast action so we can use "ThreadUtils.sameThread"
      case Success(msg) =>
        // Always receive `true`. Just ignore it
      case Failure(e) =>
        exitExecutor(1, s"Cannot register with driver: $driverUrl", e, notifyDriver = false)
    }(ThreadUtils.sameThread)
  }

  // Check that the actual resources discovered will satisfy the user specified
  // requirements and that they match the configs specified by the user to catch
  // mismatches between what the user requested and what resource manager gave or
  // what the discovery script found.
  private def checkResourcesMeetRequirements(
      resourceConfigPrefix: String,
      reqResourcesAndCounts: Array[(String, String)],
      actualResources: Map[String, ResourceInformation]): Unit = {

    reqResourcesAndCounts.foreach { case (rName, reqCount) =>
      if (actualResources.contains(rName)) {
        val resourceInfo = actualResources(rName)

        if (resourceInfo.addresses.size < reqCount.toLong) {
          throw new SparkException(s"Resource: $rName with addresses: " +
            s"${resourceInfo.addresses.mkString(",")} doesn't meet the " +
            s"requirements of needing $reqCount of them")
        }
        // also make sure the resource count on start matches the
        // resource configs specified by user
        val userCountConfigName =
          resourceConfigPrefix + rName + SPARK_RESOURCE_COUNT_POSTFIX
        val userConfigCount = env.conf.getOption(userCountConfigName).
          getOrElse(throw new SparkException(s"Resource: $rName not specified " +
            s"via config: $userCountConfigName, but required, " +
            "please fix your configuration"))

        if (userConfigCount.toLong > resourceInfo.addresses.size) {
          throw new SparkException(s"Resource: $rName, with addresses: " +
            s"${resourceInfo.addresses.mkString(",")} " +
            s"is less than what the user requested for count: $userConfigCount, " +
            s"via $userCountConfigName")
        }
      } else {
        throw new SparkException(s"Executor resource config missing required task resource: $rName")
      }
    }
  }

  // visible for testing
  def parseOrFindResources(resourcesFile: Option[String]): Map[String, ResourceInformation] = {
    // only parse the resources if a task requires them
    val taskResourceConfigs = env.conf.getAllWithPrefix(SPARK_TASK_RESOURCE_PREFIX)
    val resourceInfo = if (taskResourceConfigs.nonEmpty) {
      val execResources = resourcesFile.map { resourceFileStr => {
        val source = new BufferedInputStream(new FileInputStream(resourceFileStr))
        val resourceMap = try {
          val parsedJson = parse(source).asInstanceOf[JArray].arr
          parsedJson.map { json =>
            val name = (json \ "name").extract[String]
            val addresses = (json \ "addresses").extract[Array[String]]
            new ResourceInformation(name, addresses)
          }.map(x => (x.name -> x)).toMap
        } catch {
          case e @ (_: MappingException | _: MismatchedInputException) =>
            throw new SparkException(
              s"Exception parsing the resources in $resourceFileStr", e)
        } finally {
          source.close()
        }
        resourceMap
      }}.getOrElse(ResourceDiscoverer.findResources(env.conf, isDriver = false))

      if (execResources.isEmpty) {
        throw new SparkException("User specified resources per task via: " +
          s"$SPARK_TASK_RESOURCE_PREFIX, but can't find any resources available on the executor.")
      }
      // get just the map of resource name to count
      val resourcesAndCounts = taskResourceConfigs.
        withFilter { case (k, v) => k.endsWith(SPARK_RESOURCE_COUNT_POSTFIX)}.
        map { case (k, v) => (k.dropRight(SPARK_RESOURCE_COUNT_POSTFIX.size), v)}

      checkResourcesMeetRequirements(SPARK_EXECUTOR_RESOURCE_PREFIX, resourcesAndCounts,
        execResources)

      logInfo("===============================================================================")
      logInfo(s"Executor $executorId Resources:")
      execResources.foreach { case (k, v) => logInfo(s"$k -> $v") }
      logInfo("===============================================================================")

      execResources
    } else {
      if (resourcesFile.nonEmpty) {
        logWarning(s"A resources file was specified but the application is not configured " +
          s"to use any resources, see the configs with prefix: ${SPARK_TASK_RESOURCE_PREFIX}")
      }
      Map.empty[String, ResourceInformation]
    }
    resourceInfo
  }

  def extractLogUrls: Map[String, String] = {
    val prefix = "SPARK_LOG_URL_"
    sys.env.filterKeys(_.startsWith(prefix))
      .map(e => (e._1.substring(prefix.length).toLowerCase(Locale.ROOT), e._2))
  }

  def extractAttributes: Map[String, String] = {
    val prefix = "SPARK_EXECUTOR_ATTRIBUTE_"
    sys.env.filterKeys(_.startsWith(prefix))
      .map(e => (e._1.substring(prefix.length).toUpperCase(Locale.ROOT), e._2))
  }

  override def receive: PartialFunction[Any, Unit] = {
    case RegisteredExecutor =>
      logInfo("Successfully registered with driver")
      try {
        executor = new Executor(executorId, hostname, env, userClassPath, isLocal = false)
      } catch {
        case NonFatal(e) =>
          exitExecutor(1, "Unable to create executor due to " + e.getMessage, e)
      }

    case RegisterExecutorFailed(message) =>
      exitExecutor(1, "Slave registration failed: " + message)

    case LaunchTask(data) =>
      if (executor == null) {
        exitExecutor(1, "Received LaunchTask command but executor was null")
      } else {
        val taskDesc = TaskDescription.decode(data.value)
        logInfo("Got assigned task " + taskDesc.taskId)
        executor.launchTask(this, taskDesc)
      }

    case KillTask(taskId, _, interruptThread, reason) =>
      if (executor == null) {
        exitExecutor(1, "Received KillTask command but executor was null")
      } else {
        executor.killTask(taskId, interruptThread, reason)
      }

    case StopExecutor =>
      stopping.set(true)
      logInfo("Driver commanded a shutdown")
      // Cannot shutdown here because an ack may need to be sent back to the caller. So send
      // a message to self to actually do the shutdown.
      self.send(Shutdown)

    case Shutdown =>
      stopping.set(true)
      new Thread("CoarseGrainedExecutorBackend-stop-executor") {
        override def run(): Unit = {
          // executor.stop() will call `SparkEnv.stop()` which waits until RpcEnv stops totally.
          // However, if `executor.stop()` runs in some thread of RpcEnv, RpcEnv won't be able to
          // stop until `executor.stop()` returns, which becomes a dead-lock (See SPARK-14180).
          // Therefore, we put this line in a new thread.
          executor.stop()
        }
      }.start()

    case UpdateDelegationTokens(tokenBytes) =>
      logInfo(s"Received tokens of ${tokenBytes.length} bytes")
      SparkHadoopUtil.get.addDelegationTokens(tokenBytes, env.conf)
  }

  override def onDisconnected(remoteAddress: RpcAddress): Unit = {
    if (stopping.get()) {
      logInfo(s"Driver from $remoteAddress disconnected during shutdown")
    } else if (driver.exists(_.address == remoteAddress)) {
      exitExecutor(1, s"Driver $remoteAddress disassociated! Shutting down.", null,
        notifyDriver = false)
    } else {
      logWarning(s"An unknown ($remoteAddress) driver disconnected.")
    }
  }

  override def statusUpdate(taskId: Long, state: TaskState, data: ByteBuffer) {
    val msg = StatusUpdate(executorId, taskId, state, data)
    driver match {
      case Some(driverRef) => driverRef.send(msg)
      case None => logWarning(s"Drop $msg because has not yet connected to driver")
    }
  }

  /**
   * This function can be overloaded by other child classes to handle
   * executor exits differently. For e.g. when an executor goes down,
   * back-end may not want to take the parent process down.
   */
  protected def exitExecutor(code: Int,
                             reason: String,
                             throwable: Throwable = null,
                             notifyDriver: Boolean = true) = {
    val message = "Executor self-exiting due to : " + reason
    if (throwable != null) {
      logError(message, throwable)
    } else {
      logError(message)
    }

    if (notifyDriver && driver.nonEmpty) {
      driver.get.send(RemoveExecutor(executorId, new ExecutorLossReason(reason)))
    }

    System.exit(code)
  }
}

// org.apache.spark.executor.CoarseGrainedExecutorBackend
// --driver-url spark://CoarseGrainedScheduler@sdc145.sefon.com:39341
// --executor-id 2
// --hostname sdc146.sefon.com --cores 1
// --app-id application_1587365236189_0087
// --user-class-path file:/hadoop/yarn/local/usercache/root/appcache/application_1587365236189_0087/container_e10_1587365236189_0087_01_000003/__app__.jar
private[spark] object CoarseGrainedExecutorBackend extends Logging {

  case class Arguments(
      driverUrl: String,
      executorId: String,
      hostname: String,
      cores: Int,
      appId: String,
      workerUrl: Option[String],
      userClassPath: mutable.ListBuffer[URL],
      resourcesFile: Option[String])

  def main(args: Array[String]): Unit = {
    val createFn: (RpcEnv, Arguments, SparkEnv) =>
      CoarseGrainedExecutorBackend = { case (rpcEnv, arguments, env) =>
      new CoarseGrainedExecutorBackend(rpcEnv, arguments.driverUrl, arguments.executorId,
        arguments.hostname, arguments.cores, arguments.userClassPath, env, arguments.resourcesFile)
    }
    run(parseArguments(args, this.getClass.getCanonicalName.stripSuffix("$")), createFn)
    System.exit(0)
  }

  def run(
      arguments: Arguments,
      backendCreateFn: (RpcEnv, Arguments, SparkEnv) => CoarseGrainedExecutorBackend): Unit = {

    Utils.initDaemon(log)

    SparkHadoopUtil.get.runAsSparkUser { () =>
      // Debug code
      Utils.checkHost(arguments.hostname)

      // Bootstrap to fetch the driver's Spark properties.
      val executorConf = new SparkConf
      val fetcher = RpcEnv.create(
        "driverPropsFetcher",
        arguments.hostname,
        -1,
        executorConf,
        new SecurityManager(executorConf),
        clientMode = true)
      val driver = fetcher.setupEndpointRefByURI(arguments.driverUrl)
      val cfg = driver.askSync[SparkAppConfig](RetrieveSparkAppConfig)
      val props = cfg.sparkProperties ++ Seq[(String, String)](("spark.app.id", arguments.appId))
      fetcher.shutdown()

      // Create SparkEnv using properties we fetched from the driver.
      val driverConf = new SparkConf()
      for ((key, value) <- props) {
        // this is required for SSL in standalone mode
        if (SparkConf.isExecutorStartupConf(key)) {
          driverConf.setIfMissing(key, value)
        } else {
          driverConf.set(key, value)
        }
      }

      cfg.hadoopDelegationCreds.foreach { tokens =>
        SparkHadoopUtil.get.addDelegationTokens(tokens, driverConf)
      }

      driverConf.set(EXECUTOR_ID, arguments.executorId)
      val env = SparkEnv.createExecutorEnv(driverConf, arguments.executorId, arguments.hostname,
        arguments.cores, cfg.ioEncryptionKey, isLocal = false)

      env.rpcEnv.setupEndpoint("Executor", backendCreateFn(env.rpcEnv, arguments, env))
      arguments.workerUrl.foreach { url =>
        env.rpcEnv.setupEndpoint("WorkerWatcher", new WorkerWatcher(env.rpcEnv, url))
      }
      env.rpcEnv.awaitTermination()
    }
  }

  def parseArguments(args: Array[String], classNameForEntry: String): Arguments = {
    var driverUrl: String = null
    var executorId: String = null
    var hostname: String = null
    var cores: Int = 0
    var resourcesFile: Option[String] = None
    var appId: String = null
    var workerUrl: Option[String] = None
    val userClassPath = new mutable.ListBuffer[URL]()

    var argv = args.toList
    while (!argv.isEmpty) {
      argv match {
        case ("--driver-url") :: value :: tail =>
          driverUrl = value
          argv = tail
        case ("--executor-id") :: value :: tail =>
          executorId = value
          argv = tail
        case ("--hostname") :: value :: tail =>
          hostname = value
          argv = tail
        case ("--cores") :: value :: tail =>
          cores = value.toInt
          argv = tail
        case ("--resourcesFile") :: value :: tail =>
          resourcesFile = Some(value)
          argv = tail
        case ("--app-id") :: value :: tail =>
          appId = value
          argv = tail
        case ("--worker-url") :: value :: tail =>
          // Worker url is used in spark standalone mode to enforce fate-sharing with worker
          workerUrl = Some(value)
          argv = tail
        case ("--user-class-path") :: value :: tail =>
          userClassPath += new URL(value)
          argv = tail
        case Nil =>
        case tail =>
          // scalastyle:off println
          System.err.println(s"Unrecognized options: ${tail.mkString(" ")}")
          // scalastyle:on println
          printUsageAndExit(classNameForEntry)
      }
    }

    if (driverUrl == null || executorId == null || hostname == null || cores <= 0 ||
      appId == null) {
      printUsageAndExit(classNameForEntry)
    }

    Arguments(driverUrl, executorId, hostname, cores, appId, workerUrl,
      userClassPath, resourcesFile)
  }

  private def printUsageAndExit(classNameForEntry: String): Unit = {
    // scalastyle:off println
    System.err.println(
      s"""
      |Usage: $classNameForEntry [options]
      |
      | Options are:
      |   --driver-url <driverUrl>
      |   --executor-id <executorId>
      |   --hostname <hostname>
      |   --cores <cores>
      |   --resourcesFile <fileWithJSONResourceInformation>
      |   --app-id <appid>
      |   --worker-url <workerUrl>
      |   --user-class-path <url>
      |""".stripMargin)
    // scalastyle:on println
    System.exit(1)
  }
}

// yarn client 模式 --executor-memory 1g --executor-cores 1 --num-executors 3

// 27623 org.apache.spark.deploy.yarn.ExecutorLauncher
// --arg sdc145.sefon.com:42355
// --properties-file /hadoop/yarn/local/usercache/root/appcache/application_1587365236189_0136/container_e10_1587365236189_0136_01_000001/__spark_conf__/__spark_conf__.properties


// 27675 org.apache.spark.executor.CoarseGrainedExecutorBackend
// --driver-url spark://CoarseGrainedScheduler@sdc145.sefon.com:42355
// --executor-id 1
// --hostname sdc142.sefon.com
// --cores 1
// --app-id application_1587365236189_0136
// --user-class-path file:/hadoop/yarn/local/usercache/root/appcache/application_1587365236189_0136/container_e10_1587365236189_0136_01_000002/__app__.jar

// 32243 org.apache.spark.executor.CoarseGrainedExecutorBackend
// --driver-url spark://CoarseGrainedScheduler@sdc145.sefon.com:42355
// --executor-id 2
// --hostname sdc146.sefon.com
// --cores 1
// --app-id application_1587365236189_0136
// --user-class-path file:/hadoop/yarn/local/usercache/root/appcache/application_1587365236189_0136/container_e10_1587365236189_0136_01_000003/__app__.jar

// 27354 org.apache.spark.executor.CoarseGrainedExecutorBackend
// --driver-url spark://CoarseGrainedScheduler@sdc145.sefon.com:42355
// --executor-id 3
// --hostname sdc143.sefon.com
// --cores 1
// --app-id application_1587365236189_0136
// --user-class-path file:/hadoop/yarn/local/usercache/root/appcache/application_1587365236189_0136/container_e10_1587365236189_0136_01_000004/__app__.jar
/*


SparkSubmit

yarn-client： new JavaMainApplication(mainClass(用户传入的主类))
yarn-cluster：org.apache.spark.deploy.yarn.YarnClusterApplication



在 spark2.X 版本，启动 executor 使用的 org.apache.spark.executor.CoarseGrainedExecutorBackend
但是在 Spark3.0 之后，使用的是 org.apache.spark.executor.YarnCoarseGrainedExecutorBackend 相关原因如下 issues
https://issues.apache.org/jira/browse/SPARK-26790


20/05/15 16:50:53 INFO ApplicationMaster:
===============================================================================
YARN executor launch context:
  env:
    CLASSPATH -> {{PWD}}<CPS>{{PWD}}/__spark_conf__<CPS>{{PWD}}/__spark_libs__/*<CPS>$HADOOP_CONF_DIR<CPS>/usr/hdp/current/hadoop-client/*<CPS>/usr/hdp/current/hadoop-client/lib/*<CPS>/usr/hdp/current/hadoop-hdfs-client/*<CPS>/usr/hdp/current/hadoop-hdfs-client/lib/*<CPS>/usr/hdp/current/hadoop-yarn-client/*<CPS>/usr/hdp/current/hadoop-yarn-client/lib/*<CPS>$PWD/mr-framework/hadoop/share/hadoop/mapreduce/*:$PWD/mr-framework/hadoop/share/hadoop/mapreduce/lib/*:$PWD/mr-framework/hadoop/share/hadoop/common/*:$PWD/mr-framework/hadoop/share/hadoop/common/lib/*:$PWD/mr-framework/hadoop/share/hadoop/yarn/*:$PWD/mr-framework/hadoop/share/hadoop/yarn/lib/*:$PWD/mr-framework/hadoop/share/hadoop/hdfs/*:$PWD/mr-framework/hadoop/share/hadoop/hdfs/lib/*:$PWD/mr-framework/hadoop/share/hadoop/tools/lib/*:/etc/hadoop/conf/secure<CPS>{{PWD}}/__spark_conf__/__hadoop_conf__
    SPARK_YARN_STAGING_DIR -> hdfs://sdc143.sefon.com:8020/user/root/.sparkStaging/application_1587365236189_0602
    SPARK_USER -> root

  command:
    LD_LIBRARY_PATH="/usr/sdp/current/hadoop-client/lib/native:/usr/sdp/current/hadoop-client/lib/native/Linux-amd64-64:$LD_LIBRARY_PATH" \
      {{JAVA_HOME}}/bin/java \
      -server \
      -Xmx3072m \
      '-XX:+UseNUMA' \
      -Djava.io.tmpdir={{PWD}}/tmp \
      '-Dspark.history.ui.port=18081' \
      -Dspark.yarn.app.container.log.dir=<LOG_DIR> \
      -XX:OnOutOfMemoryError='kill %p' \
      org.apache.spark.executor.CoarseGrainedExecutorBackend \
      --driver-url \
      spark://CoarseGrainedScheduler@sdc143.sefon.com:42840 \
      --executor-id \
      <executorId> \
      --hostname \
      <hostname> \
      --cores \
      1 \
      --app-id \
      application_1587365236189_0602 \
      --user-class-path \
      file:$PWD/__app__.jar \
      1><LOG_DIR>/stdout \
      2><LOG_DIR>/stderr

  resources:
    __app__.jar -> resource { scheme: "hdfs" host: "sdc143.sefon.com" port: 8020 file: "/user/root/.sparkStaging/application_1587365236189_0602/spark-examples_2.11-2.3.2.jar" } size: 1997550 timestamp: 1589532648693 type: FILE visibility: PRIVATE
    __spark_libs__ -> resource { scheme: "hdfs" host: "sdc143.sefon.com" port: 8020 file: "/user/root/.sparkStaging/application_1587365236189_0602/__spark_libs__2236303822718028422.zip" } size: 230921750 timestamp: 1589532648520 type: ARCHIVE visibility: PRIVATE
    __spark_conf__ -> resource { scheme: "hdfs" host: "sdc143.sefon.com" port: 8020 file: "/user/root/.sparkStaging/application_1587365236189_0602/__spark_conf__.zip" } size: 237172 timestamp: 1589532648833 type: ARCHIVE visibility: PRIVATE

===============================================================================
20/05/15 16:50:53 INFO RMProxy: Connecting to ResourceManager at sdc143.sefon.com/10.0.8.143:8030

*/
