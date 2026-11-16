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

import java.util.Collections
import java.util.concurrent.{CompletableFuture, CountDownLatch, TimeUnit}
import java.util.concurrent.atomic.AtomicInteger

import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import scala.util.control.NonFatal

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.DatasourceV2SQLBase
import org.apache.spark.sql.connector.catalog.{Identifier, InMemoryCatalog, SupportsNamespaces}
import org.apache.spark.sql.connector.catalog.functions.{AsyncScalarFunction, BoundFunction, UnboundFunction}
import org.apache.spark.sql.types.{DataType, LongType, StringType, StructType}
import org.apache.spark.unsafe.types.UTF8String

/** Focused safety and optimizer tests for the native async execution path. */
class AsyncFunctionSafetySuite extends DatasourceV2SQLBase {

  private val emptyProps: java.util.Map[String, String] = Collections.emptyMap[String, String]

  private def addFunction(name: String, function: AsyncScalarFunction[UTF8String]): Unit = {
    addUnbound(name, new SafetyUnbound(name, function))
  }

  private def addUnbound(name: String, function: UnboundFunction): Unit = {
    val ns = catalog("testcat").asInstanceOf[SupportsNamespaces]
    if (!ns.namespaceExists(Array("safe"))) {
      ns.createNamespace(Array("safe"), emptyProps)
    }
    catalog("testcat").asInstanceOf[InMemoryCatalog].createFunction(
      Identifier.of(Array("safe"), name), function)
  }

  test("LIMIT is pushed below async evaluation and prevents extra calls") {
    SafetyState.reset()
    addFunction("counted", new CountingFunction("counted", deterministic = true))
    spark.range(0, 100).repartition(1).selectExpr("cast(id as string) AS value")
      .createOrReplaceTempView("safe_limit_src")
    val rows = sql("SELECT testcat.safe.counted(value) FROM safe_limit_src LIMIT 3").collect()
    assert(rows.length == 3)
    assert(SafetyState.calls.get() == 3)
  }

  test("child-only predicate runs below async evaluation and avoids calls") {
    SafetyState.reset()
    addFunction("counted", new CountingFunction("counted", deterministic = true))
    spark.range(0, 20).repartition(1)
      .selectExpr("cast(id as string) AS value", "id % 2 = 0 AS keep")
      .createOrReplaceTempView("safe_filter_src")
    val df = sql("SELECT out FROM (SELECT keep, testcat.safe.counted(value) AS out " +
      "FROM safe_filter_src) WHERE keep")
    val optimized = df.queryExecution.optimizedPlan.treeString
    assert(optimized.contains("EvalAsyncFunctions"))
    assert(df.collect().length == 10)
    assert(SafetyState.calls.get() == 10)
  }

  test("nested async calls are extracted in dependency order") {
    SafetyState.reset()
    addFunction("nested", new CountingFunction("nested", deterministic = true))
    val rows = sql(
      "SELECT testcat.safe.nested(testcat.safe.nested('a'))").collect()
    assert(rows.toSeq == Seq(Row("A")))
    assert(SafetyState.calls.get() == 2)
  }

  test("async function over an aggregate result runs after the aggregate") {
    SafetyState.reset()
    addUnbound("after_sum", new LongToStringUnbound)
    val rows = sql("SELECT testcat.safe.after_sum(sum(id)) FROM range(5)").collect()
    assert(rows.toSeq == Seq(Row("10")))
    assert(SafetyState.calls.get() == 1)
  }

  test("nondeterministic identical calls are not deduplicated") {
    SafetyState.reset()
    addFunction("varying", new CountingFunction("varying", deterministic = false))
    val df = sql("SELECT testcat.safe.varying('x'), testcat.safe.varying('x')")
    val nodes = df.queryExecution.executedPlan.collect { case a: AsyncEvalExec => a }
    assert(nodes.map(_.functions.size).sum == 2)
    assert(df.collect().length == 1)
    assert(SafetyState.calls.get() == 2)
  }

  test("invalid engine limits fail promptly instead of deadlocking") {
    val error = intercept[Throwable] {
      withSQLConf("spark.sql.asyncFunction.pollIntervalMs" -> "0") {
        fail("invalid configuration should be rejected before query execution")
      }
    }
    assert(exceptionText(error).contains("pollIntervalMs"))
  }

  test("a session cannot raise the static active-call ceiling") {
    val error = intercept[Throwable] {
      spark.conf.set("spark.sql.asyncFunction.maxInFlightCeiling", "128")
    }
    assert(exceptionText(error).contains("CANNOT_MODIFY_STATIC_CONFIG"))
  }

  test("synchronous provider exception fails promptly") {
    addFunction("sync_fail", new SynchronousFailureFunction("sync_fail", returnNull = false))
    val error = intercept[Throwable] {
      sql("SELECT testcat.safe.sync_fail('x')").collect()
    }
    assert(exceptionText(error).contains("synthetic synchronous failure"))
  }

  test("null future fails promptly") {
    addFunction("null_future", new SynchronousFailureFunction("null_future", returnNull = true))
    val error = intercept[Throwable] {
      sql("SELECT testcat.safe.null_future('x')").collect()
    }
    assert(exceptionText(error).contains("returned null"))
  }

  test("multiple functions never exceed the global active-call ceiling") {
    SafetyState.reset()
    addFunction("slow_a", new SlowCountingFunction("slow_a"))
    addFunction("slow_b", new SlowCountingFunction("slow_b"))
    spark.range(0, 12).repartition(1).selectExpr("cast(id as string) AS value")
      .createOrReplaceTempView("safe_multi_src")
    val rows = sql("SELECT testcat.safe.slow_a(value), " +
      "testcat.safe.slow_b(value) FROM safe_multi_src").collect()
    assert(rows.length == 12)
    assert(SafetyState.peak.get() <= 2)
  }

  test("a shared bound-function instance is closed once per task") {
    SafetyState.reset()
    addFunction("shared_close", new CloseCountingFunction("shared_close"))
    val rows = sql("SELECT testcat.safe.shared_close('a'), " +
      "testcat.safe.shared_close('b')").collect()
    assert(rows.toSeq == Seq(Row("a", "b")))
    assert(SafetyState.peak.get() <= 1)
    assert(SafetyState.closes.get() == 1)
  }

  test("task cancellation reaches the source future") {
    import scala.concurrent.ExecutionContext.Implicits.global

    SafetyState.reset()
    SafetyState.started = new CountDownLatch(1)
    addFunction("stall", new CancellableFunction("stall"))
    val group = "async-source-cancel"
    val action = Future {
      spark.sparkContext.setJobGroup(group, "cancel async source", interruptOnCancel = true)
      try sql("SELECT testcat.safe.stall('x')").collect()
      finally spark.sparkContext.clearJobGroup()
    }
    assert(SafetyState.started.await(10, TimeUnit.SECONDS), "provider call did not start")
    spark.sparkContext.cancelJobGroup(group)
    try Await.ready(action, 10.seconds) catch { case NonFatal(_) => }
    assert(action.isCompleted, "cancelled query did not complete promptly")
    assert(SafetyState.cancels.get() >= 1, "source CompletableFuture was not cancelled")
  }

  private def exceptionText(error: Throwable): String =
    org.apache.spark.util.Utils.exceptionString(error)
}

object SafetyState {
  val calls = new AtomicInteger(0)
  val active = new AtomicInteger(0)
  val peak = new AtomicInteger(0)
  val cancels = new AtomicInteger(0)
  val closes = new AtomicInteger(0)
  @volatile var started: CountDownLatch = new CountDownLatch(0)

  def reset(): Unit = {
    calls.set(0)
    active.set(0)
    peak.set(0)
    cancels.set(0)
    closes.set(0)
    started = new CountDownLatch(0)
  }

  def updatePeak(value: Int): Unit = {
    var previous = peak.get()
    while (value > previous && !peak.compareAndSet(previous, value)) {
      previous = peak.get()
    }
  }
}

class LongToStringUnbound extends UnboundFunction {
  override def name(): String = "after_sum"
  override def bind(inputType: StructType): BoundFunction = {
    if (inputType.fields.length != 1 || inputType.fields(0).dataType != LongType) {
      throw new UnsupportedOperationException("after_sum expects one long")
    }
    new LongToStringFunction
  }
  override def description(): String = "after_sum(long) -> string"
}

class LongToStringFunction extends AsyncScalarFunction[UTF8String] {
  override def name(): String = "after_sum"
  override def inputTypes(): Array[DataType] = Array(LongType)
  override def resultType(): DataType = StringType
  override def isDeterministic(): Boolean = true
  override def canonicalName(): String = "spark61672.safety.after_sum"
  override def produceResultAsync(input: InternalRow): CompletableFuture[UTF8String] = {
    SafetyState.calls.incrementAndGet()
    CompletableFuture.completedFuture(UTF8String.fromString(input.getLong(0).toString))
  }
}

class SafetyUnbound(functionName: String, function: AsyncScalarFunction[UTF8String])
  extends UnboundFunction {
  override def name(): String = functionName
  override def bind(inputType: StructType): BoundFunction = {
    if (inputType.fields.length != 1 || inputType.fields(0).dataType != StringType) {
      throw new UnsupportedOperationException(s"$functionName expects one string")
    }
    function
  }
  override def description(): String = s"$functionName(string) -> string"
}

class CountingFunction(functionName: String, deterministic: Boolean)
  extends AsyncScalarFunction[UTF8String] {
  override def name(): String = functionName
  override def inputTypes(): Array[DataType] = Array(StringType)
  override def resultType(): DataType = StringType
  override def isDeterministic(): Boolean = deterministic
  override def canonicalName(): String = s"spark61672.safety.$functionName"

  override def produceResultAsync(input: InternalRow): CompletableFuture[UTF8String] = {
    SafetyState.calls.incrementAndGet()
    if (input.isNullAt(0)) CompletableFuture.completedFuture(null.asInstanceOf[UTF8String])
    else CompletableFuture.completedFuture(
      UTF8String.fromString(input.getUTF8String(0).toString.toUpperCase(java.util.Locale.ROOT)))
  }
}

class SynchronousFailureFunction(functionName: String, returnNull: Boolean)
  extends AsyncScalarFunction[UTF8String] {
  override def name(): String = functionName
  override def inputTypes(): Array[DataType] = Array(StringType)
  override def resultType(): DataType = StringType
  override def isDeterministic(): Boolean = true
  override def canonicalName(): String = s"spark61672.safety.$functionName"
  override def produceResultAsync(input: InternalRow): CompletableFuture[UTF8String] = {
    if (returnNull) null
    else throw new IllegalStateException("synthetic synchronous failure")
  }
}

class SlowCountingFunction(functionName: String) extends AsyncScalarFunction[UTF8String] {
  override def name(): String = functionName
  override def inputTypes(): Array[DataType] = Array(StringType)
  override def resultType(): DataType = StringType
  override def isDeterministic(): Boolean = true
  override def maxInFlightPerTask(): Int = 1
  override def canonicalName(): String = s"spark61672.safety.$functionName"

  override def produceResultAsync(input: InternalRow): CompletableFuture[UTF8String] = {
    SafetyState.calls.incrementAndGet()
    val now = SafetyState.active.incrementAndGet()
    SafetyState.updatePeak(now)
    CompletableFuture.supplyAsync(() => {
      try {
        Thread.sleep(20)
        UTF8String.fromString(input.getUTF8String(0).toString)
      } finally {
        SafetyState.active.decrementAndGet()
      }
    })
  }
}

class CloseCountingFunction(functionName: String) extends AsyncScalarFunction[UTF8String] {
  override def name(): String = functionName
  override def inputTypes(): Array[DataType] = Array(StringType)
  override def resultType(): DataType = StringType
  override def isDeterministic(): Boolean = false
  override def maxInFlightPerTask(): Int = 1
  override def canonicalName(): String = s"spark61672.safety.$functionName"
  override def produceResultAsync(input: InternalRow): CompletableFuture[UTF8String] = {
    val now = SafetyState.active.incrementAndGet()
    SafetyState.updatePeak(now)
    CompletableFuture.supplyAsync(() => {
      try {
        Thread.sleep(20)
        UTF8String.fromString(input.getUTF8String(0).toString)
      } finally {
        SafetyState.active.decrementAndGet()
      }
    })
  }
  override def close(): Unit = SafetyState.closes.incrementAndGet()
}

class CancellableFunction(functionName: String) extends AsyncScalarFunction[UTF8String] {
  override def name(): String = functionName
  override def inputTypes(): Array[DataType] = Array(StringType)
  override def resultType(): DataType = StringType
  override def isDeterministic(): Boolean = false
  override def maxInFlightPerTask(): Int = 1
  override def canonicalName(): String = s"spark61672.safety.$functionName"

  override def produceResultAsync(input: InternalRow): CompletableFuture[UTF8String] = {
    SafetyState.started.countDown()
    new CompletableFuture[UTF8String]() {
      override def cancel(mayInterruptIfRunning: Boolean): Boolean = {
        SafetyState.cancels.incrementAndGet()
        super.cancel(mayInterruptIfRunning)
      }
    }
  }
}
