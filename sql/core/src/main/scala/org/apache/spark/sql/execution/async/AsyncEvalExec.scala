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

package org.apache.spark.sql.execution.async

import java.io.File
import java.util.concurrent.{CancellationException, CompletableFuture, ConcurrentHashMap, ExecutionException, TimeoutException, TimeUnit}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger, AtomicLong}

import scala.collection.mutable.ArrayBuffer
import scala.util.control.NonFatal

import org.apache.spark.{SparkEnv, TaskContext, TaskKilledException}
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{ApplyAsyncFunctionExpression, Attribute, AttributeSet, BoundReference, GenericInternalRow, JoinedRow, SortOrder, UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.connector.catalog.functions.AsyncScalarFunction
import org.apache.spark.sql.execution.{SparkPlan, UnaryExecNode}
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.execution.python.HybridRowQueue
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.util.Utils

/**
 * Physical operator for [[ApplyAsyncFunctionExpression]]. It keeps a bounded number of source
 * futures in flight per task and emits exactly one output row per input row, in input order.
 *
 * The active-call bound and submitted-not-emitted reorder bound are independent. The second bound
 * prevents a stalled first call from allowing completed results to accumulate without limit.
 */
case class AsyncEvalExec(
    functions: Seq[ApplyAsyncFunctionExpression],
    resultAttrs: Seq[Attribute],
    child: SparkPlan)
  extends UnaryExecNode {

  override def output: Seq[Attribute] = child.output ++ resultAttrs
  override def producedAttributes: AttributeSet = AttributeSet(resultAttrs)
  override def outputPartitioning: Partitioning = child.outputPartitioning
  override def outputOrdering: Seq[SortOrder] = child.outputOrdering

  override lazy val metrics = Map(
    "numInputRows" -> SQLMetrics.createMetric(sparkContext, "number of input rows"),
    "numCalls" -> SQLMetrics.createMetric(sparkContext, "number of calls issued"),
    "numFailedCalls" -> SQLMetrics.createMetric(sparkContext, "number of failed calls"),
    "remoteWaitTime" -> SQLMetrics.createTimingMetric(sparkContext, "time waiting on calls"),
    "peakInFlight" -> SQLMetrics.createMetric(sparkContext, "peak calls in flight"))

  private val maxInFlightCeiling =
    conf.getConf(SQLConf.ASYNC_FUNCTION_MAX_IN_FLIGHT_CEILING)
  private val reorderWindowConf = conf.getConf(SQLConf.ASYNC_FUNCTION_REORDER_WINDOW)
  private val pollMillis = conf.getConf(SQLConf.ASYNC_FUNCTION_POLL_INTERVAL_MS).toLong

  override protected def doExecute(): RDD[InternalRow] = {
    val childOutput = child.output
    val outputAttrs = output
    val funcs = functions
    require(funcs.nonEmpty, "AsyncEvalExec requires at least one function")

    val inputProjFactory = () => UnsafeProjection.create(childOutput, childOutput)
    val argProjFactories = functions.map { f =>
      () => UnsafeProjection.create(f.children, childOutput)
    }
    val localCeiling = maxInFlightCeiling
    val localReorder = reorderWindowConf
    val localPoll = pollMillis
    val numInputRows = longMetric("numInputRows")
    val numCalls = longMetric("numCalls")
    val numFailed = longMetric("numFailedCalls")
    val remoteWaitTime = longMetric("remoteWaitTime")
    val peakInFlight = longMetric("peakInFlight")

    child.execute().mapPartitionsWithIndexInternal { (_, iter) =>
      AsyncEvalExec.evalPartition(
        TaskContext.get(), iter, childOutput.length, funcs, outputAttrs,
        inputProjFactory(), argProjFactories.map(_()), localCeiling, localReorder, localPoll,
        numInputRows, numCalls, numFailed, remoteWaitTime, peakInFlight)
    }
  }

  override protected def withNewChildInternal(newChild: SparkPlan): AsyncEvalExec =
    copy(child = newChild)
}

object AsyncEvalExec extends Logging {

  // scalastyle:off
  private[async] def evalPartition(
      context: TaskContext,
      input: Iterator[InternalRow],
      childWidth: Int,
      funcs: Seq[ApplyAsyncFunctionExpression],
      outputAttrs: Seq[Attribute],
      inputProj: UnsafeProjection,
      argProjs: Seq[UnsafeProjection],
      maxInFlightCeiling: Int,
      reorderWindowConf: Int,
      pollMillis: Long,
      numInputRows: SQLMetric,
      numCalls: SQLMetric,
      numFailed: SQLMetric,
      remoteWaitTime: SQLMetric,
      peakInFlightMetric: SQLMetric): Iterator[InternalRow] = {
    // scalastyle:on

    require(funcs.nonEmpty, "async evaluation requires at least one function")
    require(maxInFlightCeiling > 0, "maxInFlightCeiling must be positive")
    require(pollMillis > 0, "pollMillis must be positive")
    require(reorderWindowConf >= 0, "reorderWindow must be non-negative")

    val providerIndexByIdentity =
      new java.util.IdentityHashMap[AsyncScalarFunction[_], java.lang.Integer]()
    val providers = new ArrayBuffer[AsyncScalarFunction[_]]()
    val providerLimits = new ArrayBuffer[Int]()
    val functionProviderIndex = funcs.map { expression =>
      val provider = expression.function
      val existing = providerIndexByIdentity.get(provider)
      if (existing != null) {
        existing.intValue()
      } else {
        val limit = provider.maxInFlightPerTask()
        require(limit > 0, s"${provider.name()} maxInFlightPerTask must be positive")
        val index = providers.length
        providers += provider
        providerLimits += limit
        providerIndexByIdentity.put(provider, java.lang.Integer.valueOf(index))
        index
      }
    }
    val maxInFlight = maxInFlightCeiling
    val reorderWindow = if (reorderWindowConf > 0) reorderWindowConf else 2 * maxInFlight

    val queue = HybridRowQueue(
      context.taskMemoryManager(),
      new File(Utils.getLocalDir(SparkEnv.get.conf)),
      childWidth)

    // Only the task thread mutates pending and queue. The interrupt listener touches only these
    // concurrent source-future and flag structures, so it cannot race queue close or map mutation.
    val pending = new java.util.TreeMap[java.lang.Long, Array[CompletableFuture[AnyRef]]]()
    val sourceFutures = new ConcurrentHashMap[CompletableFuture[AnyRef], java.lang.Boolean]()
    val activeFutures = new AtomicInteger(0)
    val activeByProvider = providerLimits.map(_ => new AtomicInteger(0))
    val failedFutures = new AtomicLong(0L)
    val cancelRequested = new AtomicBoolean(false)
    val cleaned = new AtomicBoolean(false)
    var nextSubmit = 0L
    var nextEmit = 0L

    def submittedNotEmitted: Long = nextSubmit - nextEmit

    def requestCancel(): Unit = {
      cancelRequested.set(true)
      sourceFutures.keySet().forEach((f: CompletableFuture[AnyRef]) => f.cancel(true))
    }

    def cleanup(): Unit = if (cleaned.compareAndSet(false, true)) {
      val outstanding = sourceFutures.keySet()
        .toArray(new Array[CompletableFuture[AnyRef]](sourceFutures.size()))
      requestCancel()
      if (outstanding.nonEmpty) {
        try {
          CompletableFuture.allOf(outstanding: _*)
            .get(math.min(1000L, pollMillis * 2L), TimeUnit.MILLISECONDS)
        } catch {
          case _: CancellationException | _: ExecutionException | _: TimeoutException =>
          case _: InterruptedException => Thread.currentThread().interrupt()
        }
      }
      numFailed.set(failedFutures.get())
      val uniqueFunctions = new java.util.IdentityHashMap[AsyncScalarFunction[_], java.lang.Boolean]()
      funcs.foreach { f =>
        val function = f.function
        if (uniqueFunctions.put(function, java.lang.Boolean.TRUE) == null) {
          try function.close() catch {
            case NonFatal(e) => logWarning("Async function close failed", e)
          }
        }
      }
      try queue.close() catch {
        case NonFatal(e) => logWarning("Async input queue close failed", e)
      }
      pending.clear()
    }

    context.addTaskCompletionListener[Unit](_ => cleanup())
    context.addTaskInterruptListener((_, _) => requestCancel())

    def checkInterrupted(): Unit = {
      if (cancelRequested.get() || context.isInterrupted()) {
        requestCancel()
        throw new TaskKilledException()
      }
    }

    def awaitProviderSlot(providerIndex: Int): Unit = {
      while (activeFutures.get() >= maxInFlight ||
          activeByProvider(providerIndex).get() >= providerLimits(providerIndex)) {
        checkInterrupted()
        try {
          Thread.sleep(math.min(10L, pollMillis))
        } catch {
          case _: InterruptedException =>
            Thread.currentThread().interrupt()
            requestCancel()
            throw new TaskKilledException()
        }
      }
    }

    def submitOne(row: UnsafeRow): Unit = {
      checkInterrupted()
      queue.add(row)
      val ordinal = nextSubmit
      val futures = new Array[CompletableFuture[AnyRef]](funcs.length)
      var i = 0
      try {
        while (i < funcs.length) {
          val functionIndex = i
          val providerIndex = functionProviderIndex(functionIndex)
          awaitProviderSlot(providerIndex)
          val args = argProjs(functionIndex)(row).copy()
          val provider = funcs(functionIndex).function
            .asInstanceOf[AsyncScalarFunction[AnyRef]]
          activeFutures.incrementAndGet()
          activeByProvider(providerIndex).incrementAndGet()
          peakInFlightMetric.set(math.max(peakInFlightMetric.value, activeFutures.get()))
          numCalls.add(1)

          val source = provider.produceResultAsync(args)
          if (source == null) {
            throw new IllegalStateException(s"async function ${provider.name()} returned null")
          }
          sourceFutures.put(source, java.lang.Boolean.TRUE)
          val tracked = source.whenComplete { (_, err) =>
            sourceFutures.remove(source)
            activeFutures.decrementAndGet()
            activeByProvider(providerIndex).decrementAndGet()
            if (err != null && !err.isInstanceOf[CancellationException]) {
              failedFutures.incrementAndGet()
            }
          }
          // Order and emission wait for the accounting stage, while cancellation always targets
          // the source future stored in sourceFutures.
          futures(functionIndex) = tracked
          if (cancelRequested.get()) source.cancel(true)
          i += 1
        }
        pending.put(ordinal, futures)
        nextSubmit += 1
      } catch {
        case NonFatal(t) =>
          // If produceResultAsync threw before installing its completion callback, restore the
          // counter for that slot. Earlier source futures are visible to requestCancel.
          if (i < funcs.length && futures(i) == null) {
            activeFutures.decrementAndGet()
            activeByProvider(functionProviderIndex(i)).decrementAndGet()
          }
          requestCancel()
          throw t
      }
    }

    def headReady: Boolean = {
      val futures = pending.get(nextEmit)
      futures != null && futures.forall(_.isDone)
    }

    def waitHead(): Unit = {
      val start = System.nanoTime()
      try {
        while (!headReady) {
          checkInterrupted()
          val futures = pending.get(nextEmit)
          if (futures != null) {
            try {
              CompletableFuture.allOf(futures: _*)
                .get(pollMillis, TimeUnit.MILLISECONDS)
            } catch {
              case _: TimeoutException => // poll again to observe task interruption
              case _: ExecutionException => return // the source error is rethrown by join below
              case _: InterruptedException =>
                Thread.currentThread().interrupt()
                requestCancel()
                throw new TaskKilledException()
            }
          }
        }
      } finally {
        remoteWaitTime.add((System.nanoTime() - start) / 1000000L)
      }
    }

    val joined = new JoinedRow
    val resultProj = UnsafeProjection.create(
      outputAttrs.indices.map { i =>
        BoundReference(i, outputAttrs(i).dataType, outputAttrs(i).nullable)
      })

    def canSubmitRow: Boolean = {
      activeFutures.get() < maxInFlight &&
        providers.indices.forall(i => activeByProvider(i).get() < providerLimits(i))
    }

    new Iterator[InternalRow] {
      private def fill(): Unit = {
        while (input.hasNext && submittedNotEmitted < reorderWindow && canSubmitRow) {
          checkInterrupted()
          val ownedRow = inputProj(input.next()).copy()
          numInputRows.add(1)
          submitOne(ownedRow)
        }
      }

      override def hasNext: Boolean = input.hasNext || submittedNotEmitted > 0

      override def next(): InternalRow = {
        fill()
        if (submittedNotEmitted == 0) throw new NoSuchElementException("next on empty iterator")
        waitHead()
        numFailed.set(failedFutures.get())
        val futures = pending.remove(nextEmit)
        nextEmit += 1
        val resultRow = new GenericInternalRow(funcs.length)
        var i = 0
        while (i < futures.length) {
          resultRow.update(i, futures(i).join())
          i += 1
        }
        resultProj(joined(queue.remove(), resultRow))
      }
    }
  }
}
