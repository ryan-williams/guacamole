package org.hammerlab.guacamole.util.listener

import org.apache.spark.{Logging, Resubmitted, SparkContext, Success, TaskFailedReason}
import org.apache.spark.scheduler.{JobSucceeded, SparkListener, SparkListenerApplicationEnd, SparkListenerApplicationStart, SparkListenerBlockUpdated, SparkListenerExecutorMetricsUpdate, SparkListenerJobEnd, SparkListenerJobStart, SparkListenerStageCompleted, SparkListenerStageSubmitted, SparkListenerTaskEnd, SparkListenerTaskGettingResult, SparkListenerTaskStart, StageInfo}
import org.apache.spark.storage.RDDInfo
import org.hammerlab.guacamole.util.listener.TestSparkListener.{App, AppId, AppName, JobId, RddId, StageAttemptId, StageId, TaskAttemptId, TaskId, Time}
import org.hammerlab.guacamole.util.listener.metrics.Metrics

import scala.collection.mutable

//class StageMetrics
//class StageAttemptMetrics
//class JobMetrics
//class AppMetrics

class TestSparkListener extends HasStatus(Pending) with SparkListener with Temporal with Logging {

  var id: AppId = _
  var name: AppName = _

  var sc: SparkContext = _

  val jobs = mutable.HashMap[JobId, Job]()
  val stages = mutable.HashMap[StageId, Stage]()
  val rdds = mutable.HashMap[RddId, RDD]()

  override def onApplicationStart(applicationStart: SparkListenerApplicationStart): Unit = {
    id = applicationStart.appId.getOrElse("???")
    name = applicationStart.appName
    start = applicationStart.time
    status = Running

    // SparkContext should exist in this process by now… I think!
//    sc = SparkContext.getOrCreate()

    TestSparkListener.instance = this
  }

  override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {
    end = applicationEnd.time
  }

  override def onJobStart(jobStart: SparkListenerJobStart): Unit = {
    val job = jobs.getOrElseUpdate(jobStart.jobId, Job(jobStart.jobId, jobStart.time))
    for { stageAttemptInfo <- jobStart.stageInfos } {
      val stageAttempt = getStageAttemptFromInfo(stageAttemptInfo)

      val stage = stageAttempt.stage
      job.stages.getOrElseUpdate(stage.id, stage)
      stage.jobId = job.id
    }
  }

  override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = {
    val job = jobs(jobEnd.jobId)
    job.end = jobEnd.time
    job.status = jobEnd.jobResult match {
      case JobSucceeded => Succeeded
      case status => Failed(status.toString)
    }
  }

  def getStageAttemptFromInfo(stageAttemptInfo: StageInfo): StageAttempt = {
    val stageId = stageAttemptInfo.stageId

    val stage = stages.getOrElseUpdate(stageId, Stage(this, stageId, stageAttemptInfo.name, stageAttemptInfo.details))

    stage.getAttemptFromInfo(stageAttemptInfo)
  }

  def getStageAttempt(taskStartEvent: SparkListenerTaskStart): StageAttempt =
    getStageAttempt(taskStartEvent.stageId, taskStartEvent.stageAttemptId)

  def getStageAttempt(taskEndEvent: SparkListenerTaskEnd): StageAttempt =
    getStageAttempt(taskEndEvent.stageId, taskEndEvent.stageAttemptId)

  def getStageAttempt(stageId: StageId, attemptId: StageAttemptId): StageAttempt =
    stages(stageId).attempts(attemptId)

  override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted): Unit = {
    val stageAttemptInfo = stageSubmitted.stageInfo
    val stageAttempt = getStageAttemptFromInfo(stageAttemptInfo)
    stageAttemptInfo.submissionTime match {
      case Some(submissionTime) =>
        stageAttempt.start = submissionTime
        stageAttempt.status = Running
      case None =>
        //throw new Exception(
//        log.warn(
//          s"Missing submission time for stage attempt: $stageAttempt; $stageAttemptInfo"
//        )
    }
  }

  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
    val stageAttemptInfo = stageCompleted.stageInfo
    val stageAttempt = getStageAttemptFromInfo(stageAttemptInfo)

    stageAttemptInfo.submissionTime match {
      case Some(submissionTime) =>
        stageAttempt.start = submissionTime
        stageAttempt.status = Running
      case None =>
        throw new Exception(
          s"Missing submission time for stage attempt: $stageAttempt; $stageAttemptInfo"
        )
    }

    stageAttemptInfo.completionTime match {
      case Some(completionTime) =>
        stageAttempt.end = completionTime
        stageAttempt.status = stageAttemptInfo.failureReason match {
          case Some(failureReason) => Failed(failureReason)
          case None => Succeeded
        }
      case None =>
        throw new Exception(
          s"No completionTime for stage: $stageAttempt; $stageAttemptInfo, ${stageAttemptInfo.failureReason}"
        )
    }
  }

  def handleRDDInfo(rddInfo: RDDInfo, stage: Stage): RDD = {
    val rddId = rddInfo.id
    val rdd =
      rdds.getOrElseUpdate(
        rddId,
        RDD(rddId, rddInfo.name, rddInfo.numPartitions, rddInfo.callSite, rddInfo.parentIds)
      )
    rdds.getOrElseUpdate(rddId, rdd)
    stage.rdds.getOrElseUpdate(rddId, rdd)
  }

  override def onTaskStart(taskStart: SparkListenerTaskStart): Unit = {
    val taskAttempt = getStageAttempt(taskStart).getTaskAttempt(taskStart.taskInfo)
    taskAttempt.status = Running
  }

//  override def onTaskGettingResult(taskGettingResult: SparkListenerTaskGettingResult): Unit = {
//
//  }

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
    val stageAttempt = getStageAttempt(taskEnd)
    val taskAttempt = stageAttempt.getTaskAttempt(taskEnd.taskInfo)

    taskEnd.reason match {
      case Success =>
        taskAttempt.status = Succeeded
      case reason: TaskFailedReason =>
        taskAttempt.status = Failed(reason.toErrorString)
    }

    taskAttempt.updateMetrics(taskEnd.taskMetrics)
  }

  var metrics = Metrics()
  def updateMetrics(delta: Metrics) = {
    metrics += delta
  }

//  override def onExecutorMetricsUpdate(executorMetricsUpdate: SparkListenerExecutorMetricsUpdate): Unit = {
//
//  }
//
//  override def onBlockUpdated(blockUpdated: SparkListenerBlockUpdated): Unit = {
//
//  }

//  override def onEnvironmentUpdate(environmentUpdate: SparkListenerEnvironmentUpdate): Unit = {
//
//  }

//  override def onBlockManagerAdded(blockManagerAdded: SparkListenerBlockManagerAdded): Unit = {
//
//  }

//  override def onBlockManagerRemoved(blockManagerRemoved: SparkListenerBlockManagerRemoved): Unit = {
//
//  }

//  override def onUnpersistRDD(unpersistRDD: SparkListenerUnpersistRDD): Unit = {
//
//  }

//  override def onExecutorAdded(executorAdded: SparkListenerExecutorAdded): Unit = {
//
//  }

//  override def onExecutorRemoved(executorRemoved: SparkListenerExecutorRemoved): Unit = {
//
//  }
}

object TestSparkListener {

  var instance: TestSparkListener = _

  def apply(): TestSparkListener = instance

  type AppId = String
  type App = TestSparkListener

  type AppName = String

  type JobId = Int

  type StageId = Int
  type StageAttemptId = Int

  type TaskIndex = Int
  type TaskId = Int
  type TaskAttemptId = Long
  type TaskAttemptNum = Int

  type RddId = Int

  type Time = Long
}
