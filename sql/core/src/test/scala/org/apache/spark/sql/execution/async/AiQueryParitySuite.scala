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
import java.util.concurrent.{CompletableFuture, ExecutorService, Executors, ThreadLocalRandom}
import java.util.concurrent.atomic.AtomicInteger

import com.fasterxml.jackson.databind.ObjectMapper
import com.sun.net.httpserver.{HttpExchange, HttpServer}

import org.apache.spark.SparkException
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.DatasourceV2SQLBase
import org.apache.spark.sql.connector.catalog.{Identifier, InMemoryCatalog, SupportsNamespaces}
import org.apache.spark.sql.connector.catalog.functions.{AsyncScalarFunction, BoundFunction, UnboundFunction}
import org.apache.spark.sql.types.{DataType, StringType, StructType}
import org.apache.spark.unsafe.types.UTF8String

/**
 * SPARK-61672: parity test against the SPARK-61404 `ai_query` prototype (PR #60311).
 *
 * The `ai_query` prototype is a PySpark Pandas UDF that calls Databricks Model Serving
 * (OpenAI chat-completions envelope) and Amazon Bedrock Converse. This suite runs the same
 * workloads on our native async engine, calling loopback servers that return the exact same
 * provider response envelopes, from a SQL function instead of a Pandas UDF. It mirrors the
 * `ai_query` behaviors: provider-specific request and response shapes, null-input passthrough,
 * an `on_error = null` tolerant mode, strict failure, and transient 429 retry, while keeping
 * our in-order emission and planning-deterministic operator (`AsyncEvalExec`).
 */
class AiQueryParitySuite extends DatasourceV2SQLBase {

  private val emptyProps: java.util.Map[String, String] = Collections.emptyMap[String, String]

  @transient private var server: HttpServer = _
  @transient private var serverExecutor: ExecutorService = _
  private var port: Int = _
  @transient private val retryHits = new AtomicInteger(0)

  private def readBody(ex: HttpExchange): String =
    new String(ex.getRequestBody.readAllBytes(), StandardCharsets.UTF_8)

  private def send(ex: HttpExchange, status: Int, body: String): Unit = {
    val bytes = body.getBytes(StandardCharsets.UTF_8)
    ex.sendResponseHeaders(status, bytes.length.toLong)
    val os = ex.getResponseBody
    try os.write(bytes) finally os.close()
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    val mapper = new ObjectMapper()
    server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0)

    // Databricks Model Serving envelope: read messages[0].content, reply choices[0].message.content
    def databricksOk(ex: HttpExchange): Unit = {
      val prompt = mapper.readTree(readBody(ex)).path("messages").path(0).path("content").asText()
      val up = prompt.toUpperCase(Locale.ROOT)
      Thread.sleep(ThreadLocalRandom.current().nextInt(3, 25).toLong)
      send(ex, 200, s"""{"choices":[{"message":{"role":"assistant","content":${mapper
        .writeValueAsString(up)}}}]}""")
    }
    server.createContext("/db/ok", (ex: HttpExchange) => databricksOk(ex))

    // 429 on the first call, then the Databricks success envelope.
    server.createContext("/db/retry", (ex: HttpExchange) => {
      if (retryHits.getAndIncrement() == 0) {
        send(ex, 429, """{"error":"rate limited"}""")
      } else {
        databricksOk(ex)
      }
    })

    // Always 400, which is a nullable condition under the null policy.
    server.createContext("/db/bad", (ex: HttpExchange) => send(ex, 400, """{"error":"bad"}"""))

    // Bedrock Converse envelope: read messages[0].content[0].text, reply output.message.content[].text
    server.createContext("/br/ok", (ex: HttpExchange) => {
      val prompt = mapper.readTree(readBody(ex))
        .path("messages").path(0).path("content").path(0).path("text").asText()
      val up = prompt.toUpperCase(Locale.ROOT)
      Thread.sleep(ThreadLocalRandom.current().nextInt(3, 25).toLong)
      send(ex, 200, s"""{"output":{"message":{"role":"assistant","content":[{"text":${mapper
        .writeValueAsString(up)}}]}}}""")
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

  private def addFunction(name: String, fn: UnboundFunction): Unit = {
    val ns = catalog("testcat").asInstanceOf[SupportsNamespaces]
    if (!ns.namespaceExists(Array("aq"))) {
      ns.createNamespace(Array("aq"), emptyProps)
    }
    catalog("testcat").asInstanceOf[InMemoryCatalog]
      .createFunction(Identifier.of(Array("aq"), name), fn)
  }

  private def hasAsyncEvalExec(df: org.apache.spark.sql.DataFrame): Boolean =
    df.queryExecution.executedPlan.exists(_.isInstanceOf[AsyncEvalExec])

  test("ai_query parity: Databricks chat-completions envelope") {
    addFunction("databricks", new AiQueryUnbound("databricks", port, "/db/ok", onErrorNull = false))
    val df = sql("SELECT testcat.aq.databricks('hello') AS out")
    assert(hasAsyncEvalExec(df), "must plan to the native AsyncEvalExec, not a UDF")
    checkAnswer(df, Row("HELLO") :: Nil)
  }

  test("ai_query parity: Bedrock Converse envelope") {
    addFunction("bedrock", new AiQueryUnbound("bedrock", port, "/br/ok", onErrorNull = false))
    val df = sql("SELECT testcat.aq.bedrock('world') AS out")
    assert(hasAsyncEvalExec(df))
    checkAnswer(df, Row("WORLD") :: Nil)
  }

  test("ai_query parity: null request returns null with no call") {
    addFunction("databricks", new AiQueryUnbound("databricks", port, "/db/ok", onErrorNull = false))
    spark.createDataFrame(Seq(Tuple1("a"), Tuple1[String](null), Tuple1("c")))
      .toDF("req").createOrReplaceTempView("aq_null_src")
    val df = sql("SELECT testcat.aq.databricks(req) AS out FROM aq_null_src")
    checkAnswer(df, Row("A") :: Row(null) :: Row("C") :: Nil)
  }

  test("ai_query parity: on_error=null turns a 400 into null, neighbors succeed") {
    addFunction("databricks", new AiQueryUnbound("databricks", port, "/db/ok", onErrorNull = false))
    addFunction("databricks_bad_try",
      new AiQueryUnbound("databricks", port, "/db/bad", onErrorNull = true))
    val df = sql("SELECT testcat.aq.databricks_bad_try('x') AS out")
    checkAnswer(df, Row(null) :: Nil)
    // a good neighbor call still succeeds
    checkAnswer(sql("SELECT testcat.aq.databricks('ok') AS out"), Row("OK") :: Nil)
  }

  test("ai_query parity: strict mode fails the query on a 400") {
    addFunction("databricks_bad", new AiQueryUnbound("databricks", port, "/db/bad", onErrorNull = false))
    val df = sql("SELECT testcat.aq.databricks_bad('x') AS out")
    val e = intercept[SparkException] {
      df.collect()
    }
    assert(org.apache.spark.util.Utils.exceptionString(e).contains("HTTP 400"))
  }

  test("ai_query parity: a transient 429 is retried and then succeeds") {
    retryHits.set(0)
    addFunction("databricks_retry",
      new AiQueryUnbound("databricks", port, "/db/retry", onErrorNull = false))
    val df = sql("SELECT testcat.aq.databricks_retry('again') AS out")
    checkAnswer(df, Row("AGAIN") :: Nil)
    assert(retryHits.get() >= 2, "the route should have been hit at least twice (429 then 200)")
  }

  test("ai_query parity: results stay in input order over many rows") {
    addFunction("databricks", new AiQueryUnbound("databricks", port, "/db/ok", onErrorNull = false))
    val words = (0 until 120).map(i => s"w$i")
    spark.createDataFrame(words.map(Tuple1(_))).toDF("req")
      .createOrReplaceTempView("aq_src")
    val df = sql("SELECT req, testcat.aq.databricks(req) AS out FROM aq_src")
    assert(hasAsyncEvalExec(df))
    assert(df.collect().toSeq ==
      words.map(w => Row(w, w.toUpperCase(Locale.ROOT))))
  }
}

/** Binds an `ai_query`-style function to the given provider envelope and error policy. */
class AiQueryUnbound(provider: String, port: Int, path: String, onErrorNull: Boolean)
  extends UnboundFunction {
  override def name(): String = "ai_query"
  override def bind(inputType: StructType): BoundFunction = {
    if (inputType.fields.length != 1 || inputType.fields(0).dataType != StringType) {
      throw new UnsupportedOperationException("ai_query expects exactly one string argument")
    }
    new AiQueryFunction(provider, port, path, onErrorNull)
  }
  override def description(): String = s"ai_query($provider) -> string"
}

/**
 * Our `AsyncScalarFunction` realization of `ai_query`: builds the provider-specific request,
 * issues a non-blocking call, parses the provider-specific response envelope, retries a transient
 * 429/5xx, and under the null policy returns null for a nullable failure.
 */
class AiQueryFunction(provider: String, port: Int, path: String, onErrorNull: Boolean)
  extends AsyncScalarFunction[UTF8String] with Serializable {

  private val maxRetries = 2

  override def name(): String = "ai_query"
  override def inputTypes(): Array[DataType] = Array(StringType)
  override def resultType(): DataType = StringType
  override def isDeterministic(): Boolean = false
  override def canonicalName(): String = s"spark61672.aiquery.$provider.$port.$path.$onErrorNull"

  @transient private lazy val client: HttpClient = HttpClient.newHttpClient()
  @transient private lazy val mapper: ObjectMapper = new ObjectMapper()

  private def requestBody(text: String): String = {
    val q = mapper.writeValueAsString(text)
    if (provider == "bedrock") {
      s"""{"messages":[{"role":"user","content":[{"text":$q}]}],""" +
        s""""inferenceConfig":{"maxTokens":256,"temperature":0}}"""
    } else {
      s"""{"messages":[{"role":"user","content":$q}],"max_tokens":256,"temperature":0}"""
    }
  }

  private def parse(body: String): UTF8String = {
    val root = mapper.readTree(body)
    val text = if (provider == "bedrock") {
      root.path("output").path("message").path("content").path(0).path("text").asText()
    } else {
      root.path("choices").path(0).path("message").path("content").asText()
    }
    UTF8String.fromString(text)
  }

  private def nullable(status: Int): Boolean =
    status == 400 || status == 408 || status == 413 || status == 422

  override def produceResultAsync(input: InternalRow): CompletableFuture[UTF8String] = {
    if (input.isNullAt(0)) {
      return CompletableFuture.completedFuture(null.asInstanceOf[UTF8String])
    }
    val body = requestBody(input.getUTF8String(0).toString)

    def attempt(n: Int): CompletableFuture[UTF8String] = {
      val request = HttpRequest.newBuilder()
        .uri(URI.create(s"http://127.0.0.1:$port$path"))
        .POST(HttpRequest.BodyPublishers.ofString(body, StandardCharsets.UTF_8))
        .build()
      client.sendAsync(request, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8))
        .thenCompose { response =>
          val sc = response.statusCode()
          if ((sc == 429 || sc >= 500) && n < maxRetries) {
            attempt(n + 1)
          } else if (sc == 200) {
            CompletableFuture.completedFuture(parse(response.body()))
          } else if (onErrorNull && (nullable(sc) || sc == 429 || sc >= 500)) {
            CompletableFuture.completedFuture(null.asInstanceOf[UTF8String])
          } else {
            CompletableFuture.failedFuture[UTF8String](
              new RuntimeException(s"HTTP $sc"))
          }
        }
    }
    attempt(0)
  }
}
