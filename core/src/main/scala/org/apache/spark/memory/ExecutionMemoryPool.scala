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

package org.apache.spark.memory

import javax.annotation.concurrent.GuardedBy

import scala.collection.mutable

import org.apache.spark.internal.Logging

/**
 * Implements policies and bookkeeping for sharing an adjustable-sized pool of memory between tasks.
 *
 * Tries to ensure that each task gets a reasonable share of memory, instead of some task ramping up
 * to a large amount first and then causing others to spill to disk repeatedly.
 *
 * If there are N tasks, it ensures that each task can acquire at least 1 / 2N of the memory
 * before it has to spill, and at most 1 / N. Because N varies dynamically, we keep track of the
 * set of active tasks and redo the calculations of 1 / 2N and 1 / N in waiting tasks whenever this
 * set changes. This is all done by synchronizing access to mutable state and using wait() and
 * notifyAll() to signal changes to callers. Prior to Spark 1.6, this arbitration of memory across
 * tasks was performed by the ShuffleMemoryManager.
 *
 * @param lock a [[MemoryManager]] instance to synchronize on
 * @param memoryMode the type of memory tracked by this pool (on- or off-heap)
 */
private[memory] class ExecutionMemoryPool(
    lock: Object,
    memoryMode: MemoryMode
  ) extends MemoryPool(lock) with Logging {

  private[this] val poolName: String = memoryMode match {
    case MemoryMode.ON_HEAP => "on-heap execution"
    case MemoryMode.OFF_HEAP => "off-heap execution"
  }

  /**
   * 记录task 的使用量，taskid -> 内存使用量
   * Map from taskAttemptId -> memory consumption in bytes
   */
  @GuardedBy("lock")
  private val memoryForTask = new mutable.HashMap[Long, Long]()

  override def memoryUsed: Long = lock.synchronized {
    memoryForTask.values.sum
  }

  /**
   * Returns the memory consumption, in bytes, for the given task.
   */
  def getMemoryUsageForTask(taskAttemptId: Long): Long = lock.synchronized {
    memoryForTask.getOrElse(taskAttemptId, 0L)
  }

  /**
   * Try to acquire up to `numBytes` of memory for the given task and return the number of bytes
   * obtained, or 0 if none can be allocated.
   *
   * This call may block until there is enough free memory in some situations, to make sure each
   * task has a chance to ramp up to at least 1 / 2N of the total memory pool (where N is the # of
   * active tasks) before it is forced to spill. This can happen if the number of tasks increase
   * but an older task had a lot of memory already.
   *
   * @param numBytes number of bytes to acquire
   * @param taskAttemptId the task attempt acquiring memory
   * @param maybeGrowPool a callback that potentially grows the size of this pool. It takes in
   *                      one parameter (Long) that represents the desired amount of memory by
   *                      which this pool should be expanded.
   * @param computeMaxPoolSize a callback that returns the maximum allowable size of this pool
   *                           at this given moment. This is not a field because the max pool
   *                           size is variable in certain cases. For instance, in unified
   *                           memory management, the execution pool can be expanded by evicting
   *                           cached blocks, thereby shrinking the storage pool.
   *
   * @return the number of bytes granted to the task.
   */
  private[memory] def acquireMemory(
      numBytes: Long,
      taskAttemptId: Long,
      maybeGrowPool: Long => Unit = (additionalSpaceNeeded: Long) => Unit,
      computeMaxPoolSize: () => Long = () => poolSize): Long = lock.synchronized {
    assert(numBytes > 0, s"invalid number of bytes requested: $numBytes")

    // TODO: clean up this clunky method signature

    // Add this task to the taskMemory map just so we can keep an accurate count of the number
    // of active tasks, to let other tasks ramp down their memory in calls to `acquireMemory`
    if (!memoryForTask.contains(taskAttemptId)) {
      memoryForTask(taskAttemptId) = 0L
      // This will later cause waiting tasks to wake up and check numTasks again
      lock.notifyAll()
    }

    // Keep looping until we're either sure that we don't want to grant this request (because this
    // task would have more than 1 / numActiveTasks of the memory) or we have enough free
    // memory to give it (we always let each task get at least 1 / (2 * numActiveTasks)).
    // TODO: simplify this to limit each task to its own slot
    while (true) {
      // 获取当前存在的 task 的个数
      val numActiveTasks = memoryForTask.keys.size
      // 获取当前 task 已占用的内存大小
      val curMem = memoryForTask(taskAttemptId)

      // In every iteration of this loop, we should first try to reclaim any borrowed execution
      // space from storage. This is necessary because of the potential race condition where new
      // storage blocks may steal the free execution memory that this task was waiting for.
      // 在每次循环时, 都会去从 storage 中借用当前申请的内存大小减去当前 executor 的空闲内存大小
      maybeGrowPool(numBytes - memoryFree)

      // Maximum size the pool would have after potentially growing the pool.
      // This is used to compute the upper bound of how much memory each task can occupy. This
      // must take into account potential free memory as well as the amount this pool currently
      // occupies. Otherwise, we may run into SPARK-12155 where, in unified memory management,
      // we did not take into account space that could have been freed by evicting cached blocks.
      // executor pool 最大内存大小
      val maxPoolSize = computeMaxPoolSize()
      // 为当前每个 task 可分配的平均的最大内存大小 maxPoolSize 为可能像 storage 借用了内存后的大小
      val maxMemoryPerTask = maxPoolSize / numActiveTasks
      // 为当前每个 task 可分配的评价的最小内存大小 poolSize 为 storage 的初始大小
      val minMemoryPerTask = poolSize / (2 * numActiveTasks)

      // How much we can grant this task; keep its share within 0 <= X <= 1 / numActiveTasks
      // 最大可分配给当前 task 的 executor 内存大小, maxMemoryPerTask - curMem = 为每个 task 评价分配的最大内存大小 - 该 task 已经使用了的内存大小 （能照顾到每个 task）
      val maxToGrant = math.min(numBytes, math.max(0, maxMemoryPerTask - curMem))
      // Only give it as much memory as is free, which might be none if it reached 1 / numTasks
      // 真正能分配的内存大小是最大的可分配的大小和空闲内存的最小值
      val toGrant = math.min(maxToGrant, memoryFree)

      // We want to let each task get at least 1 / (2 * numActiveTasks) before blocking;
      // if we can't give it this much now, wait for other tasks to free up memory
      // (this happens if older tasks allocated lots of memory before N grew)
      // 如果真正分配的内存大小小于申请的大小并且当前 task 占用的内存大小加上真正分配的大小小于 minMemoryPerTask 则 该 task 线程等待
      if (toGrant < numBytes && curMem + toGrant < minMemoryPerTask) {
        logInfo(s"TID $taskAttemptId waiting for at least 1/2N of $poolName pool to be free")
        lock.wait()
      } else {
        // 记录当前 task 占用内存
        memoryForTask(taskAttemptId) += toGrant
        return toGrant
      }
    }
    0L  // Never reached
  }

  /**
   * Release `numBytes` of memory acquired by the given task.
   */
  def releaseMemory(numBytes: Long, taskAttemptId: Long): Unit = lock.synchronized {
    val curMem = memoryForTask.getOrElse(taskAttemptId, 0L)
    var memoryToFree = if (curMem < numBytes) {
      logWarning(
        s"Internal error: release called on $numBytes bytes but task only has $curMem bytes " +
          s"of memory from the $poolName pool")
      curMem
    } else {
      numBytes
    }
    if (memoryForTask.contains(taskAttemptId)) {
      memoryForTask(taskAttemptId) -= memoryToFree
      if (memoryForTask(taskAttemptId) <= 0) {
        memoryForTask.remove(taskAttemptId)
      }
    }
    lock.notifyAll() // Notify waiters in acquireMemory() that memory has been freed
  }

  /**
   * Release all memory for the given task and mark it as inactive (e.g. when a task ends).
   * @return the number of bytes freed.
   */
  def releaseAllMemoryForTask(taskAttemptId: Long): Long = lock.synchronized {
    val numBytesToFree = getMemoryUsageForTask(taskAttemptId)
    releaseMemory(numBytesToFree, taskAttemptId)
    numBytesToFree
  }

}
