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

import java.net.{InetSocketAddress, URI}
import java.net.http.{HttpClient, HttpRequest, HttpResponse}
import java.nio.charset.StandardCharsets
import java.util.Collections
import java.util.Locale
import java.util.concurrent.{CompletableFuture, ConcurrentLinkedQueue, ExecutorService, Executors, ThreadLocalRandom}

import scala.jdk.CollectionConverters._

import com.sun.net.httpserver.HttpServer

import org.apache.spark.SparkException
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.DatasourceV2SQLBase
import org.apache.spark.sql.connector.catalog.{Identifier, InMemoryCatalog, SupportsNamespaces}
import org.apache.spark.sql.connector.catalog.functions.{AsyncScalarFunction, BoundFunction, UnboundFunction}
import org.apache.spark.sql.types.{DataType, StringType, StructType}
import org.apache.spark.unsafe.types.UTF8String

/**
 * SPARK-61672: end-to-end test that runs `SELECT <catalog>.<ns>.generate(col)` through the real
 * Spark analyzer, optimizer (ExtractAsyncFunctions), planner (AsyncFunctionEvals) and physical
 * operator (AsyncEvalExec), where the async "model" call is a real asynchronous HTTP request to a
 * loopback server started by the test. This exercises the whole path, not a mock of it.
 */
class AsyncFunctionEndToEndSuite extends DatasourceV2SQLBase {

  private val emptyProps: java.util.Map[String, String] = Collections.emptyMap[String, String]

  @transient private var server: HttpServer = _
  @transient private var serverExecutor: ExecutorService = _
  @transient private val completionOrder = new ConcurrentLinkedQueue[Int]()
  private var port: Int = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0)
    // /gen uppercases the request body after a small random delay, so completion order differs
    // from request order and the operator's in-order emission is actually tested.
    server.createContext("/gen", (exchange: com.sun.net.httpserver.HttpExchange) => {
      val body = new String(exchange.getRequestBody.readAllBytes(), StandardCharsets.UTF_8)
      Thread.sleep(ThreadLocalRandom.current().nextInt(5, 40).toLong)
      val resp = body.toUpperCase(Locale.ROOT).getBytes(StandardCharsets.UTF_8)
      exchange.sendResponseHeaders(200, resp.length.toLong)
      val os = exchange.getResponseBody
      try os.write(resp) finally os.close()
    })
    server.createContext("/reverse", (exchange: com.sun.net.httpserver.HttpExchange) => {
      val body = new String(exchange.getRequestBody.readAllBytes(), StandardCharsets.UTF_8)
      val ordinal = body.toInt
      Thread.sleep(if (ordinal == 0) 200L else if (ordinal == 1) 10L else 40L)
      completionOrder.add(ordinal)
      val resp = body.getBytes(StandardCharsets.UTF_8)
      exchange.sendResponseHeaders(200, resp.length.toLong)
      val os = exchange.getResponseBody
      try os.write(resp) finally os.close()
    })
    // /boom always fails, to exercise the strict (task-failing) error mode.
    server.createContext("/boom", (exchange: com.sun.net.httpserver.HttpExchange) => {
      val resp = "boom".getBytes(StandardCharsets.UTF_8)
      exchange.sendResponseHeaders(500, resp.length.toLong)
      val os = exchange.getResponseBody
      try os.write(resp) finally os.close()
    })
    serverExecutor = Executors.newFixedThreadPool(8)
    server.setExecutor(serverExecutor)
    server.start()
    port = server.getAddress.getPort
  }

  override def afterAll(): Unit = {
    try {
      if (server != null) server.stop(0)
      if (serverExecutor != null) serverExecutor.shutdownNow()
    } finally {
      super.afterAll()
    }
  }

  private def addFunction(ns: String, name: String, fn: UnboundFunction): Unit = {
    catalog("testcat").asInstanceOf[SupportsNamespaces]
      .createNamespace(Array(ns), emptyProps)
    catalog("testcat").asInstanceOf[InMemoryCatalog]
      .createFunction(Identifier.of(Array(ns), name), fn)
  }

  private def hasAsyncEvalExec(df: org.apache.spark.sql.DataFrame): Boolean =
    df.queryExecution.executedPlan.exists(_.isInstanceOf[AsyncEvalExec])

  test("async scalar function: single row returns the async HTTP result") {
    addFunction("ns", "generate", new GenerateUnbound(port, "/gen"))
    val df = sql("SELECT testcat.ns.generate('hello') AS out")
    assert(hasAsyncEvalExec(df), "query must plan to AsyncEvalExec")
    checkAnswer(df, Row("HELLO") :: Nil)
  }

  test("async scalar function: per-row results are correct and in order over many rows") {
    addFunction("ns", "generate", new GenerateUnbound(port, "/gen"))
    val words = (0 until 200).map(i => s"w$i")
    spark.createDataFrame(words.map(Tuple1(_))).toDF("data")
      .createOrReplaceTempView("async_src")
    val df = sql("SELECT data, testcat.ns.generate(data) AS out FROM async_src")
    assert(hasAsyncEvalExec(df))
    // A direct sequence assertion proves both row/result association and output order.
    assert(df.collect().toSeq ==
      words.map(w => Row(w, w.toUpperCase(Locale.ROOT))))
  }

  test("async scalar function: forced reverse completion still emits input order") {
    completionOrder.clear()
    addFunction("ns", "generate", new GenerateUnbound(port, "/reverse"))
    val values = (0 until 6).map(_.toString)
    spark.createDataFrame(values.map(Tuple1(_))).toDF("data")
      .createOrReplaceTempView("async_reverse_src")
    val rows = sql("SELECT data, testcat.ns.generate(data) FROM async_reverse_src").collect()
    assert(rows.toSeq == values.map(v => Row(v, v)))
    val completed = completionOrder.iterator().asScala.toSeq
    assert(completed.indexOf(1) < completed.indexOf(0),
      s"expected row 1 to complete before row 0, got $completed")
  }

  test("async scalar function: NULL input is passed through as NULL without a call") {
    addFunction("ns", "generate", new GenerateUnbound(port, "/gen"))
    spark.createDataFrame(Seq(Tuple1("a"), Tuple1[String](null), Tuple1("c")))
      .toDF("data").createOrReplaceTempView("async_null_src")
    val df = sql("SELECT testcat.ns.generate(data) AS out FROM async_null_src")
    assert(hasAsyncEvalExec(df))
    checkAnswer(df, Row("A") :: Row(null) :: Row("C") :: Nil)
  }

  test("async scalar function: a failing call fails the query (strict error mode)") {
    addFunction("ns", "generate_fail", new GenerateUnbound(port, "/boom"))
    val df = sql("SELECT testcat.ns.generate_fail('x') AS out")
    val e = intercept[SparkException] {
      df.collect()
    }
    assert(org.apache.spark.util.Utils.exceptionString(e).contains("HTTP 500"),
      s"expected the HTTP 500 failure to surface, got: ${e.getMessage}")
  }

  test("async scalar function: identical calls are evaluated once (dedup)") {
    addFunction("ns", "generate", new GenerateUnbound(port, "/gen"))
    val df = sql("SELECT testcat.ns.generate('dup') AS a, testcat.ns.generate('dup') AS b")
    assert(hasAsyncEvalExec(df))
    val asyncNodes = df.queryExecution.executedPlan.collect {
      case a: AsyncEvalExec => a
    }
    assert(asyncNodes.size == 1, "two identical calls should share one AsyncEvalExec")
    assert(asyncNodes.head.functions.size == 1,
      "two identical calls should collapse to a single evaluated function")
    checkAnswer(df, Row("DUP", "DUP") :: Nil)
  }
}

/**
 * Binds `generate(string)` to an [[AsyncScalarFunction]] that calls the loopback server at the
 * given path. The bound function is serialized to executors, so it is [[Serializable]] and builds
 * its [[HttpClient]] lazily on first use on the executor.
 */
class GenerateUnbound(port: Int, path: String) extends UnboundFunction {
  override def name(): String = "generate"
  override def bind(inputType: StructType): BoundFunction = {
    if (inputType.fields.length != 1 || inputType.fields(0).dataType != StringType) {
      throw new UnsupportedOperationException("generate expects exactly one string argument")
    }
    new HttpGenerateFunction(port, path)
  }
  override def description(): String = "generate(string) -> string"
}

class HttpGenerateFunction(port: Int, path: String)
  extends AsyncScalarFunction[UTF8String] with Serializable {

  override def name(): String = "generate"
  override def inputTypes(): Array[DataType] = Array(StringType)
  override def resultType(): DataType = StringType
  override def isDeterministic(): Boolean = true
  override def canonicalName(): String = s"spark61672.generate.$port.$path"

  @transient private lazy val client: HttpClient = HttpClient.newHttpClient()

  override def produceResultAsync(input: InternalRow): CompletableFuture[UTF8String] = {
    if (input.isNullAt(0)) {
      return CompletableFuture.completedFuture(null.asInstanceOf[UTF8String])
    }
    val text = input.getUTF8String(0).toString
    val request = HttpRequest.newBuilder()
      .uri(URI.create(s"http://127.0.0.1:$port$path"))
      .POST(HttpRequest.BodyPublishers.ofString(text, StandardCharsets.UTF_8))
      .build()
    client.sendAsync(request, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8))
      .thenApply { response =>
        if (response.statusCode() != 200) {
          throw new RuntimeException(s"HTTP ${response.statusCode()}")
        }
        UTF8String.fromString(response.body())
      }
  }
}
