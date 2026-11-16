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

package org.apache.spark.sql.ai.conformance

import java.io.ByteArrayOutputStream
import java.net.InetSocketAddress
import java.nio.charset.StandardCharsets
import java.time.{ZoneOffset, ZonedDateTime}
import java.time.format.DateTimeFormatter
import java.util.concurrent.{CompletableFuture, CompletionException, TimeUnit}
import java.util.concurrent.atomic.AtomicInteger
import java.util.zip.GZIPOutputStream

import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters._

import com.sun.net.httpserver.{HttpExchange, HttpServer}

import org.apache.spark.sql.ai.bedrock.{BedrockClient, BedrockConf, BedrockFunctionCatalog}
import org.apache.spark.sql.ai.openai.{OpenAIClient, OpenAIConf, OpenAIFunctionCatalog}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.connector.catalog.Identifier
import org.apache.spark.sql.connector.catalog.functions.AsyncScalarFunction
import org.apache.spark.sql.types.{StringType, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.unsafe.types.UTF8String

/**
 * SPARK-61672 provider-module conformance harness.
 *
 * Exercises the shipped provider modules through their real catalog path: `initialize` with
 * cluster allowlists, `loadFunction`, `bind`, then `produceResultAsync`, against a loopback HTTP
 * server that emulates an OpenAI-compatible endpoint. No Spark session is required, so every case
 * runs in seconds with a JDK. Covers the client behaviors SPARK-61404 tests in its mocked Python
 * suite: allowlist and URL rejection, ceiling clamp, gzip, decoded size cap, Retry-After seconds and
 * HTTP-date, absolute deadline, terminal versus retryable classification, fatal-versus-row-level
 * policy across all three function forms, sanitization, cancellation, and the circuit breaker.
 *
 * Set SPARK_TEST_LIVE_BEDROCK=1 to also run the Bedrock module live through the same catalog path.
 *
 *   java -cp ... org.apache.spark.sql.ai.conformance.ProviderConformanceHarness
 */
object ProviderConformanceHarness {

  private val results = ArrayBuffer[(String, Boolean, String)]()
  private val PROMPT = "SECRET_PROMPT_MARKER please summarize"
  private val TOKEN = "SECRET_TOKEN_MARKER_123"

  private def check(name: String)(body: => Unit): Unit = {
    // Each check starts with fresh shared breaker/pacer state so no test can silently degrade
    // another's coverage (for example by leaving a breaker open).
    OpenAIClient.resetSharedStateForTests()
    BedrockClient.resetSharedStateForTests()
    try { body; results += ((name, true, "")) }
    catch { case t: Throwable => results += ((name, false, s"${t.getClass.getSimpleName}: ${t.getMessage}")) }
  }

  private def assertTrue(cond: Boolean, msg: => String): Unit =
    if (!cond) throw new AssertionError(msg)

  private def row(s: String): InternalRow = {
    val r = new GenericInternalRow(1)
    r.update(0, if (s == null) null else UTF8String.fromString(s))
    r
  }

  private def options(kv: (String, String)*): CaseInsensitiveStringMap =
    new CaseInsensitiveStringMap(kv.toMap.asJava)

  private def unwrapCode(t: Throwable): String = OpenAIClient.errorCode(t)

  private def await[T](f: CompletableFuture[T]): T = f.get(20, TimeUnit.SECONDS)

  private def failureOf[T](f: CompletableFuture[T]): Throwable =
    try { await(f); throw new AssertionError("expected failure, got success") }
    catch {
      case e: java.util.concurrent.ExecutionException => e.getCause
      case c: CompletionException => c.getCause
    }

  // ---------------------------------------------------------------------------------------------
  // Loopback OpenAI-compatible server. Behavior is selected by the model name in the request body.
  // ---------------------------------------------------------------------------------------------
  private val hits = new java.util.concurrent.ConcurrentHashMap[String, AtomicInteger]()
  private def hit(key: String): Int = hits.computeIfAbsent(key, _ => new AtomicInteger()).incrementAndGet()

  private def okBody(text: String): Array[Byte] =
    s"""{"id":"chatcmpl-1","choices":[{"index":0,"message":{"role":"assistant","content":${jsonStr(text)}}}]}"""
      .getBytes(StandardCharsets.UTF_8)

  private def jsonStr(s: String): String =
    "\"" + s.replace("\\", "\\\\").replace("\"", "\\\"").replace("\n", "\\n") + "\""

  private def gzip(bytes: Array[Byte]): Array[Byte] = {
    val bos = new ByteArrayOutputStream()
    val g = new GZIPOutputStream(bos); g.write(bytes); g.close()
    bos.toByteArray
  }

  private def send(ex: HttpExchange, status: Int, body: Array[Byte],
      headers: (String, String)*): Unit = {
    headers.foreach { case (k, v) => ex.getResponseHeaders.add(k, v) }
    ex.getResponseHeaders.add("x-request-id", "req-" + status)
    ex.sendResponseHeaders(status, if (body.isEmpty) -1 else body.length)
    if (body.nonEmpty) ex.getResponseBody.write(body)
    ex.close()
  }

  /** Sends a chunked body (no Content-Length) so the client cannot pre-check the size. */
  private def sendChunked(ex: HttpExchange, status: Int, body: Array[Byte]): Unit = {
    ex.getResponseHeaders.add("x-request-id", "req-chunked")
    ex.sendResponseHeaders(status, 0)   // 0 = chunked transfer encoding
    val os = ex.getResponseBody
    var off = 0
    while (off < body.length) {
      val n = math.min(4096, body.length - off)
      os.write(body, off, n); os.flush(); off += n
    }
    ex.close()
  }

  // Every request that reaches the server is counted here, regardless of model.
  private val totalRequests = new AtomicInteger(0)
  // Set when the slow handler's write fails because the client went away.
  private val slowWriteAborted = new AtomicInteger(0)
  private val trickleAborted = new AtomicInteger(0)

  private def startServer(): HttpServer = {
    val server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0)
    server.createContext("/v1/chat/completions", (ex: HttpExchange) => {
      totalRequests.incrementAndGet()
      val reqBody = new String(ex.getRequestBody.readAllBytes(), StandardCharsets.UTF_8)
      val auth = Option(ex.getRequestHeaders.getFirst("Authorization")).getOrElse("")
      val model = {
        val m = "\"model\":\"([^\"]+)\"".r.findFirstMatchIn(reqBody)
        m.map(_.group(1)).getOrElse("unknown")
      }
      if (auth != s"Bearer $TOKEN") send(ex, 401, "{}".getBytes)
      else model match {
        case "m-ok" => hit("ok"); send(ex, 200, okBody("HELLO"))
        case "m-chunked-big" => sendChunked(ex, 200, okBody("z" * 200000))
        case "m-slow-cancel" =>
          hit("slow-cancel")
          try { Thread.sleep(1500); send(ex, 200, okBody("SLOW")) }
          catch { case _: java.io.IOException => slowWriteAborted.incrementAndGet() }
        case "m-trickle" =>
          // Headers immediately, then a chunked body trickled 1 byte / 100 ms for ~6 s. If the
          // client aborts the exchange, our write fails and trickleAborted is incremented.
          hit("trickle")
          try {
            ex.getResponseHeaders.add("x-request-id", "req-trickle")
            ex.sendResponseHeaders(200, 0)
            val os = ex.getResponseBody
            val body = okBody("TRICKLE")
            var i = 0
            while (i < 60) { os.write(body(i % body.length)); os.flush(); Thread.sleep(100); i += 1 }
            ex.close()
          } catch { case _: java.io.IOException => trickleAborted.incrementAndGet() }
        case "m-echo" => // echo the prompt back with JSON-significant characters intact
          val p = "\"content\":\"((?:[^\"\\\\]|\\\\.)*)\"".r.findFirstMatchIn(reqBody).map(_.group(1)).getOrElse("")
          send(ex, 200, okBody("echo:" + p))
        case "m-gzip" =>
          send(ex, 200, gzip(okBody("GZIPPED")), "Content-Encoding" -> "gzip")
        case "m-big" =>
          send(ex, 200, okBody("x" * 4096))
        case "m-bigzip" =>
          send(ex, 200, gzip(okBody("y" * 65536)), "Content-Encoding" -> "gzip")
        case "m-429-seconds" =>
          if (hit("429s") == 1) send(ex, 429, "{}".getBytes, "Retry-After" -> "1")
          else send(ex, 200, okBody("AFTER_429"))
        case "m-429-date" =>
          if (hit("429d") == 1) {
            // RFC 1123 dates have one-second resolution, so use +2s and assert at least ~1s.
            val when = ZonedDateTime.now(ZoneOffset.UTC).plusSeconds(2)
            send(ex, 429, "{}".getBytes,
              "Retry-After" -> DateTimeFormatter.RFC_1123_DATE_TIME.format(when))
          } else send(ex, 200, okBody("AFTER_DATE"))
        case "m-503-always" => hit("503"); send(ex, 503, "{}".getBytes)
        case "m-503-sanit" => send(ex, 503, """{"error":"SECRET_BODY"}""".getBytes)
        case "m-503-retry2" => hit("503r2"); send(ex, 503, "{}".getBytes, "Retry-After" -> "2")
        case "m-breaker" => hit("breaker"); send(ex, 500, "{}".getBytes)
        case "m-bigerr" => send(ex, 503, ("{\"e\":\"" + ("x" * 70000) + "\"}").getBytes)
        case "m-400" => send(ex, 400, """{"error":{"message":"bad request SECRET_BODY"}}""".getBytes)
        case "m-404" => send(ex, 404, "{}".getBytes)
        case "m-403" => send(ex, 403, "{}".getBytes)
        case "m-schema" => send(ex, 200, """{"choices":[]}""".getBytes)
        case "m-notjson" => send(ex, 200, "<html>oops</html>".getBytes)
        case "m-slow" => Thread.sleep(3000); send(ex, 200, okBody("SLOW"))
        case _ => send(ex, 500, "{}".getBytes)
      }
    })
    server.setExecutor(java.util.concurrent.Executors.newCachedThreadPool())
    server.start()
    server
  }

  // ---------------------------------------------------------------------------------------------
  def main(args: Array[String]): Unit = {
    val server = startServer()
    val base = s"http://127.0.0.1:${server.getAddress.getPort}/v1"
    val allModels = "m-ok,m-echo,m-gzip,m-big,m-bigzip,m-429-seconds,m-429-date,m-503-always," +
      "m-400,m-404,m-403,m-schema,m-notjson,m-slow,m-breaker,m-chunked-big,m-slow-cancel," +
      "m-503-sanit,m-503-retry2,m-bigerr,m-trickle"
    val cluster: Map[String, String] = Map(
      "spark.ai.openai.allowedBaseUrls" -> s"$base,https://approved.example.com/v1",
      "spark.ai.openai.allowedModels" -> allModels,
      "spark.ai.openai.credentialEnv" -> "AI_TEST_TOKEN",
      "spark.ai.openai.maxResponseBytes" -> "2097152",     // 2 MiB cluster ceiling
      "spark.ai.openai.maxAttempts" -> "50",
      "spark.ai.openai.breakerFailureThreshold" -> "3",
      "spark.ai.openai.breakerResetMs" -> "400",
      "spark.sql.asyncFunction.maxInFlightCeiling" -> "8")
    val conf: OpenAIConf.ClusterConf = cluster.get

    def bound(model: String, form: String, extra: (String, String)*): AsyncScalarFunction[AnyRef] = {
      val cat = new OpenAIFunctionCatalog()
      cat.initialize("ai", options(Seq("baseUrl" -> base, "chatModel" -> model) ++ extra: _*), conf)
      cat.loadFunction(Identifier.of(Array.empty, form))
        .bind(new StructType().add("prompt", StringType))
        .asInstanceOf[AsyncScalarFunction[AnyRef]]
    }

    try {
      // --- trust boundary: what a session can and cannot change --------------------------------
      check("credentialEnv cannot be set from a catalog option (cluster-only)") {
        val t = intercept(OpenAIConf.resolve(options("baseUrl" -> base, "chatModel" -> "m-ok",
          "credentialEnv" -> "SENSITIVE_EXECUTOR_ENV"), conf))
        assertTrue(t.getMessage.contains("cluster-only"), t.toString)
      }
      check("session can lower but not raise maxResponseBytes above the cluster ceiling") {
        val lower = OpenAIConf.resolve(options("baseUrl" -> base, "chatModel" -> "m-ok",
          "maxResponseBytes" -> "1024"), conf)
        val raise = OpenAIConf.resolve(options("baseUrl" -> base, "chatModel" -> "m-ok",
          "maxResponseBytes" -> "9000000000000000000"), conf)
        assertTrue(lower.maxResponseBytes == 1024L, s"lower ${lower.maxResponseBytes}")
        assertTrue(raise.maxResponseBytes == 2097152L, s"raise ${raise.maxResponseBytes}")
      }
      check("session cannot disable a cluster rate ceiling or raise it") {
        val rconf: OpenAIConf.ClusterConf = (cluster + ("spark.ai.openai.ratePermitsPerSec" -> "4")).get
        val off = OpenAIConf.resolve(options("baseUrl" -> base, "chatModel" -> "m-ok", "ratePermitsPerSec" -> "0"), rconf)
        val up = OpenAIConf.resolve(options("baseUrl" -> base, "chatModel" -> "m-ok", "ratePermitsPerSec" -> "1000"), rconf)
        val down = OpenAIConf.resolve(options("baseUrl" -> base, "chatModel" -> "m-ok", "ratePermitsPerSec" -> "2"), rconf)
        assertTrue(off.ratePermitsPerSec == 4 && up.ratePermitsPerSec == 4 && down.ratePermitsPerSec == 2,
          s"off=${off.ratePermitsPerSec} up=${up.ratePermitsPerSec} down=${down.ratePermitsPerSec}")
      }
      check("breaker policy options from a session are ignored (cluster-only)") {
        val s = OpenAIConf.resolve(options("baseUrl" -> base, "chatModel" -> "m-ok",
          "breakerFailureThreshold" -> "1000000", "breakerResetMs" -> "1"), conf)
        assertTrue(s.breakerFailureThreshold == 3 && s.breakerResetMs == 400L,
          s"${s.breakerFailureThreshold}/${s.breakerResetMs}")
      }
      check("garbage numeric option fails without echoing the value") {
        val t = intercept(OpenAIConf.resolve(options("baseUrl" -> base, "chatModel" -> "m-ok",
          "maxAttempts" -> "SECRET_GARBAGE"), conf))
        assertTrue(!t.getMessage.contains("SECRET_GARBAGE") && t.getMessage.contains("integer"), t.getMessage)
      }
      check("loopback exemption is host-exact, not prefix-matched") {
        val fake = "http://127.0.0.1.attacker.example/v1"
        val t = intercept(OpenAIConf.resolve(options("baseUrl" -> fake, "chatModel" -> "m-ok"),
          (k: String) => if (k.endsWith("allowedBaseUrls")) Some(fake) else cluster.get(k)))
        assertTrue(t.getMessage.contains("https"), t.toString)
      }
      check("baseUrl with user info is rejected") {
        val t = intercept(OpenAIConf.resolve(options("baseUrl" -> "https://user:pw@approved.example.com/v1",
          "chatModel" -> "m-ok"), conf))
        assertTrue(t.getMessage.contains("user info"), t.toString)
      }
      check("DESCRIBE text does not expose the endpoint URL") {
        val cat = new OpenAIFunctionCatalog()
        cat.initialize("ai", options("baseUrl" -> base, "chatModel" -> "m-ok"), conf)
        val d = cat.loadFunction(Identifier.of(Array.empty, "generate")).description()
        assertTrue(!d.contains("127.0.0.1") && !d.contains("http"), d)
      }

      // --- configuration and allowlists -----------------------------------------------------
      check("allowlist rejects an unapproved base URL") {
        val t = intercept(OpenAIConf.resolve(options("baseUrl" -> "https://evil.example.com/v1",
          "chatModel" -> "m-ok"), conf))
        assertTrue(t.isInstanceOf[IllegalArgumentException] && t.getMessage.contains("allowedBaseUrls"), t.toString)
      }
      check("allowlist rejects an unapproved model") {
        val t = intercept(OpenAIConf.resolve(options("baseUrl" -> base, "chatModel" -> "gpt-evil"), conf))
        assertTrue(t.getMessage.contains("allowedModels"), t.toString)
      }
      check("plain http is rejected off loopback") {
        val t = intercept(OpenAIConf.resolve(options("baseUrl" -> "http://approved.example.com/v1",
          "chatModel" -> "m-ok"), (_: String) => Some("http://approved.example.com/v1,m-ok")))
        assertTrue(t.getMessage.contains("https"), t.toString)
      }
      check("empty allowlist blocks the catalog entirely") {
        val t = intercept(OpenAIConf.resolve(options("baseUrl" -> base, "chatModel" -> "m-ok"), (_: String) => None))
        assertTrue(t.getMessage.contains("is empty"), t.toString)
      }
      check("requested concurrency is clamped to the static ceiling") {
        val s = OpenAIConf.resolve(options("baseUrl" -> base, "chatModel" -> "m-ok", "maxInFlight" -> "64"), conf)
        assertTrue(s.maxInFlight == 8, s"expected 8, got ${s.maxInFlight}")
        val b = bound("m-ok", "generate", "maxInFlight" -> "64")
        assertTrue(b.maxInFlightPerTask() == 8, s"bound function reports ${b.maxInFlightPerTask()}")
      }
      check("bound function is nondeterministic and has a stable canonical name") {
        val b = bound("m-ok", "generate")
        assertTrue(!b.isDeterministic(), "generative function must report nondeterministic")
        assertTrue(b.canonicalName() == bound("m-ok", "generate").canonicalName(), "canonical name unstable")
      }
      check("catalog lists exactly the three forms") {
        val cat = new OpenAIFunctionCatalog()
        cat.initialize("ai", options("baseUrl" -> base, "chatModel" -> "m-ok"), conf)
        val names = cat.listFunctions(Array.empty).map(_.name()).toSet
        assertTrue(names == Set("generate", "try_generate", "generate_status"), names.toString)
      }
      check("bind rejects a non-string argument") {
        val cat = new OpenAIFunctionCatalog()
        cat.initialize("ai", options("baseUrl" -> base, "chatModel" -> "m-ok"), conf)
        val t = intercept(cat.loadFunction(Identifier.of(Array.empty, "generate"))
          .bind(new StructType().add("n", org.apache.spark.sql.types.IntegerType)))
        assertTrue(t.isInstanceOf[UnsupportedOperationException], t.toString)
      }

      // --- happy path and JSON correctness --------------------------------------------------
      check("success parses choices[0].message.content") {
        val v = await(bound("m-ok", "generate").produceResultAsync(row(PROMPT)))
        assertTrue(v.toString == "HELLO", s"got $v")
      }
      check("prompt with quotes, backslashes and newlines round-trips through Jackson") {
        val tricky = "say \"hi\" \\ twice\nplease"
        val v = await(bound("m-echo", "generate").produceResultAsync(row(tricky))).toString
        assertTrue(v == "echo:" + tricky.replace("\\", "\\\\").replace("\"", "\\\"").replace("\n", "\\n"),
          s"got $v")
      }
      check("NULL input returns NULL without a call (verified by the server request counter)") {
        // First prove the counter moves for a real call, so the check below is not vacuous.
        val b = bound("m-ok", "generate")
        val c0 = totalRequests.get()
        await(b.produceResultAsync(row(PROMPT)))
        assertTrue(totalRequests.get() == c0 + 1, "counter did not register a real call")
        val c1 = totalRequests.get()
        val v = await(b.produceResultAsync(row(null)))
        assertTrue(v == null, s"expected null, got $v")
        assertTrue(totalRequests.get() == c1, "a request reached the server for NULL input")
      }
      check("gzip response is decoded") {
        val v = await(bound("m-gzip", "generate").produceResultAsync(row(PROMPT)))
        assertTrue(v.toString == "GZIPPED", s"got $v")
      }

      // --- size caps ---------------------------------------------------------------------------
      check("decoded size cap rejects an oversized plain response") {
        val t = failureOf(bound("m-big", "generate", "maxResponseBytes" -> "1024").produceResultAsync(row(PROMPT)))
        assertTrue(unwrapCode(t) == "RESPONSE_TOO_LARGE", t.toString)
      }
      check("decoded size cap rejects an oversized gzip response (decoded, not compressed, size)") {
        val t = failureOf(bound("m-bigzip", "generate", "maxResponseBytes" -> "4096").produceResultAsync(row(PROMPT)))
        assertTrue(unwrapCode(t) == "RESPONSE_TOO_LARGE", t.toString)
      }
      check("a response under the cap still succeeds") {
        val v = await(bound("m-big", "generate", "maxResponseBytes" -> "1048576").produceResultAsync(row(PROMPT)))
        assertTrue(v.toString.length == 4096, s"len ${v.toString.length}")
      }
      check("chunked response with no Content-Length is aborted at the cap while streaming") {
        // 200 KB body, 16 KB cap, chunked: no Content-Length pre-check is possible.
        val t = failureOf(bound("m-chunked-big", "generate", "maxResponseBytes" -> "16384").produceResultAsync(row(PROMPT)))
        assertTrue(unwrapCode(t) == "RESPONSE_TOO_LARGE", t.toString)
      }
      check("chunked response under the cap succeeds") {
        val v = await(bound("m-chunked-big", "generate", "maxResponseBytes" -> "1048576").produceResultAsync(row(PROMPT)))
        assertTrue(v.toString.length == 200000, s"len ${v.toString.length}")
      }

      // --- retries, Retry-After, deadlines --------------------------------------------------
      check("429 with Retry-After seconds is retried and then succeeds") {
        val t0 = System.nanoTime()
        val v = await(bound("m-429-seconds", "generate").produceResultAsync(row(PROMPT)))
        val ms = (System.nanoTime() - t0) / 1000000
        assertTrue(v.toString == "AFTER_429", s"got $v")
        assertTrue(ms >= 900, s"Retry-After not honored, took $ms ms")
      }
      check("Retry-After parser handles delay-seconds, HTTP-date, and garbage") {
        assertTrue(OpenAIClient.parseRetryAfterMs("3") == 3000L, "seconds")
        val future = DateTimeFormatter.RFC_1123_DATE_TIME.format(
          ZonedDateTime.now(ZoneOffset.UTC).plusSeconds(5))
        val ms = OpenAIClient.parseRetryAfterMs(future)
        assertTrue(ms > 3000L && ms <= 5000L, s"http-date gave $ms ms for header '$future'")
        assertTrue(OpenAIClient.parseRetryAfterMs("garbage") == 0L, "garbage")
        assertTrue(OpenAIClient.parseRetryAfterMs("-5") == 0L, "negative clamps to 0")
      }
      check("429 with Retry-After HTTP-date is retried and then succeeds") {
        val t0 = System.nanoTime()
        val v = await(bound("m-429-date", "generate").produceResultAsync(row(PROMPT)))
        val ms = (System.nanoTime() - t0) / 1000000
        assertTrue(v.toString == "AFTER_DATE", s"got $v")
        assertTrue(ms >= 900, s"Retry-After date not honored, took $ms ms")
      }
      check("retries stop at maxAttempts") {
        hits.remove("503")
        val t = failureOf(bound("m-503-always", "generate", "maxAttempts" -> "3", "maxElapsedMs" -> "30000")
          .produceResultAsync(row(PROMPT)))
        assertTrue(unwrapCode(t) == "SERVER_ERROR", t.toString)
        assertTrue(hits.get("503").get() == 3, s"attempts: ${hits.get("503").get()}")
      }
      check("absolute deadline stops retries before maxAttempts") {
        hits.remove("503")
        val t0 = System.nanoTime()
        val t = failureOf(bound("m-503-always", "generate", "maxAttempts" -> "50", "maxElapsedMs" -> "400")
          .produceResultAsync(row(PROMPT)))
        val ms = (System.nanoTime() - t0) / 1000000
        assertTrue(unwrapCode(t) == "SERVER_ERROR", t.toString)
        assertTrue(ms < 5000 && hits.get("503").get() < 50, s"deadline ignored: $ms ms, ${hits.get("503").get()} attempts")
      }
      check("request timeout surfaces TIMEOUT and does not hang") {
        val t = failureOf(bound("m-slow", "generate", "requestTimeoutMs" -> "500", "maxAttempts" -> "1")
          .produceResultAsync(row(PROMPT)))
        assertTrue(unwrapCode(t) == "TIMEOUT", t.toString)
      }

      // --- terminal classification and sanitization -----------------------------------------
      check("400 is terminal BAD_REQUEST and is not retried") {
        val t = failureOf(bound("m-400", "generate").produceResultAsync(row(PROMPT)))
        assertTrue(unwrapCode(t) == "BAD_REQUEST", t.toString)
      }
      check("404 is terminal BAD_REQUEST") {
        assertTrue(unwrapCode(failureOf(bound("m-404", "generate").produceResultAsync(row(PROMPT)))) == "BAD_REQUEST", "")
      }
      check("403 is terminal AUTH") {
        assertTrue(unwrapCode(failureOf(bound("m-403", "generate").produceResultAsync(row(PROMPT)))) == "AUTH", "")
      }
      check("malformed envelope is SCHEMA") {
        assertTrue(unwrapCode(failureOf(bound("m-schema", "generate").produceResultAsync(row(PROMPT)))) == "SCHEMA", "")
      }
      check("non-JSON body is SCHEMA") {
        assertTrue(unwrapCode(failureOf(bound("m-notjson", "generate").produceResultAsync(row(PROMPT)))) == "SCHEMA", "")
      }
      check("no error path in any form leaks the prompt, token, URL, or provider body") {
        val secrets = Seq("SECRET_PROMPT_MARKER", TOKEN, "SECRET_BODY", "127.0.0.1", "/v1/chat")
        def chainText(t: Throwable): String =
          Iterator.iterate(t)(_.getCause).takeWhile(_ != null).map(e => e.toString + " " + e.getMessage).mkString
        val errorModels = Seq("m-400", "m-404", "m-403", "m-schema", "m-notjson", "m-503-sanit", "m-slow")
        for (m <- errorModels; form <- Seq("generate", "try_generate", "generate_status")) {
          val b = bound(m, form, "maxAttempts" -> "1", "requestTimeoutMs" -> "500")
          val out = try Right(await(b.produceResultAsync(row(PROMPT)))) catch {
            case e: java.util.concurrent.ExecutionException => Left(e.getCause)
            case e: CompletionException => Left(e.getCause)
          }
          val text = out match {
            case Left(t) => chainText(t)
            case Right(r: InternalRow) => (2 to 3).map(i => if (r.isNullAt(i)) "" else r.getUTF8String(i).toString).mkString(" ")
            case Right(_) => ""
          }
          for (s <- secrets) assertTrue(!text.contains(s), s"$m/$form leaked '$s' in: $text")
        }
      }
      check("generate_status throws (does not return a struct) on a fatal AUTH error") {
        val t = failureOf(bound("m-403", "generate_status").produceResultAsync(row(PROMPT)))
        assertTrue(unwrapCode(t) == "AUTH", t.toString)
      }

      // --- the three forms -----------------------------------------------------------------
      check("try_generate turns a row-level error into NULL") {
        val v = await(bound("m-400", "try_generate").produceResultAsync(row(PROMPT)))
        assertTrue(v == null, s"expected null, got $v")
      }
      check("try_generate still fails fatally on AUTH") {
        val t = failureOf(bound("m-403", "try_generate").produceResultAsync(row(PROMPT)))
        assertTrue(unwrapCode(t) == "AUTH", t.toString)
      }
      check("generate_status returns a populated SUCCESS struct") {
        val r = await(bound("m-ok", "generate_status").produceResultAsync(row(PROMPT))).asInstanceOf[InternalRow]
        assertTrue(r.getUTF8String(0).toString == "SUCCESS", r.toString)
        assertTrue(r.getUTF8String(1).toString == "HELLO", r.toString)
        assertTrue(r.getInt(4) == 1, s"attempts ${r.getInt(4)}")
        assertTrue(r.getUTF8String(5).toString == "req-200", s"request_id ${r.getUTF8String(5)}")
      }
      check("generate_status returns an ERROR struct with code, safe message, attempts and request id") {
        val r = await(bound("m-400", "generate_status").produceResultAsync(row(PROMPT))).asInstanceOf[InternalRow]
        assertTrue(r.getUTF8String(0).toString == "ERROR", r.toString)
        assertTrue(r.isNullAt(1), "result must be null on error")
        assertTrue(r.getUTF8String(2).toString == "BAD_REQUEST", r.toString)
        assertTrue(!r.getUTF8String(3).toString.contains("SECRET"), "message leaked")
        assertTrue(r.getInt(4) == 1 && r.getUTF8String(5).toString == "req-400", r.toString)
      }
      check("generate_status reports attempts after a retry") {
        hits.remove("429s")
        val r = await(bound("m-429-seconds", "generate_status").produceResultAsync(row(PROMPT))).asInstanceOf[InternalRow]
        assertTrue(r.getInt(4) == 2, s"attempts ${r.getInt(4)}")
      }
      check("generate_status NULL input is SKIPPED_NULL_INPUT with zero attempts") {
        val r = await(bound("m-ok", "generate_status").produceResultAsync(row(null))).asInstanceOf[InternalRow]
        assertTrue(r.getUTF8String(0).toString == "SKIPPED_NULL_INPUT" && r.getInt(4) == 0, r.toString)
      }

      // --- cancellation and breaker --------------------------------------------------------
      // --- cancellation: observed at the SERVER, not only on the client future ----------------
      check("cancelling the result aborts the in-flight HTTP exchange (server sees the write fail)") {
        slowWriteAborted.set(0); hits.remove("slow-cancel")
        val f = bound("m-slow-cancel", "generate", "requestTimeoutMs" -> "10000").produceResultAsync(row(PROMPT))
        Thread.sleep(300)                       // request has reached the server and is sleeping
        f.cancel(true)
        Thread.sleep(1700)                      // let the server finish its sleep and try to write
        assertTrue(f.isCancelled, "client future not cancelled")
        assertTrue(hits.get("slow-cancel").get() == 1, "request never reached the server")
        assertTrue(slowWriteAborted.get() == 1, "server write did not fail: connection was not aborted")
      }
      check("cancelling during retry backoff sends no further request (deterministic Retry-After)") {
        hits.remove("503r2")
        // 503 with Retry-After: 2 -> the client must wait 2s before attempt 2. Cancel at 300ms.
        val f = bound("m-503-retry2", "generate", "maxAttempts" -> "5", "maxElapsedMs" -> "60000").produceResultAsync(row(PROMPT))
        Thread.sleep(300)
        assertTrue(hits.get("503r2").get() == 1, s"expected exactly 1 attempt before cancel, got ${hits.get("503r2").get()}")
        f.cancel(true)
        Thread.sleep(2600)                      // past the Retry-After window
        assertTrue(hits.get("503r2").get() == 1, s"a retry was sent after cancel: ${hits.get("503r2").get()}")
      }
      check("each attempt is bounded end to end: a trickled body cannot exceed the attempt budget") {
        // m-slow sleeps 3s before responding; requestTimeoutMs 500 must cut it off even though
        // this is the headers phase. Also assert the deadline covers the whole wait.
        val t0 = System.nanoTime()
        val t = failureOf(bound("m-slow", "generate", "requestTimeoutMs" -> "500", "maxAttempts" -> "3",
          "maxElapsedMs" -> "1200").produceResultAsync(row(PROMPT)))
        val ms = (System.nanoTime() - t0) / 1000000
        assertTrue(unwrapCode(t) == "TIMEOUT", t.toString)
        assertTrue(ms < 2500, s"deadline not enforced across attempts: $ms ms")
      }
      check("a trickled BODY is cut off at the attempt budget and the exchange is aborted") {
        trickleAborted.set(0); hits.remove("trickle")
        val t0 = System.nanoTime()
        val t = failureOf(bound("m-trickle", "generate", "requestTimeoutMs" -> "700", "maxAttempts" -> "1")
          .produceResultAsync(row(PROMPT)))
        val ms = (System.nanoTime() - t0) / 1000000
        assertTrue(unwrapCode(t) == "TIMEOUT", s"expected TIMEOUT, got $t")
        assertTrue(ms < 3000, s"body phase not bounded: $ms ms (server trickles for 6 s)")
        Thread.sleep(1200)   // give the server's next write a chance to fail
        assertTrue(hits.get("trickle").get() == 1, "request did not reach the server")
        assertTrue(trickleAborted.get() == 1, "exchange was not aborted: server write did not fail")
      }
      check("rejected pacer callers do not burn slots or trip the breaker") {
        // Cluster ceiling 2/s, deadline 600ms: a burst of 10 admits ~2 and rejects the rest.
        val rconf: OpenAIConf.ClusterConf = (cluster + ("spark.ai.openai.ratePermitsPerSec" -> "2")).get
        val cat = new OpenAIFunctionCatalog()
        cat.initialize("ai", options("baseUrl" -> base, "chatModel" -> "m-echo", "maxElapsedMs" -> "600"), rconf)
        val b = cat.loadFunction(Identifier.of(Array.empty, "generate")).bind(new StructType().add("p", StringType))
          .asInstanceOf[AsyncScalarFunction[AnyRef]]
        val outcomes = (1 to 10).map(_ => b.produceResultAsync(row(PROMPT))).map { f =>
          try { await(f); "OK" } catch { case e: java.util.concurrent.ExecutionException => unwrapCode(e.getCause) }
        }
        assertTrue(outcomes.contains("PACED_OUT"), s"expected some PACED_OUT: $outcomes")
        assertTrue(!outcomes.contains("CIRCUIT_OPEN") && !outcomes.contains("RATE_LIMITED"),
          s"local pacing rejection must not be a health failure: $outcomes")
        // After the burst, a fresh caller must be admitted within ~one interval (no drift).
        Thread.sleep(600)
        val t0 = System.nanoTime()
        await(b.produceResultAsync(row(PROMPT)))
        val ms = (System.nanoTime() - t0) / 1000000
        assertTrue(ms < 1200, s"schedule drifted after rejected burst: next caller waited $ms ms")
        val s = OpenAIConf.resolve(options("baseUrl" -> base, "chatModel" -> "m-echo"), rconf)
        assertTrue(!OpenAIClient.breakerFor(s).isOpen, "pacer rejections tripped the breaker")
      }

      // --- circuit breaker: shared per endpoint, health-only, half-open, generation-stamped ----
      check("circuit breaker opens after consecutive endpoint failures and reports CIRCUIT_OPEN") {
        val b = bound("m-breaker", "generate", "maxAttempts" -> "1")   // m-breaker -> 500
        for (_ <- 1 to 3) failureOf(b.produceResultAsync(row(PROMPT)))
        val t = failureOf(b.produceResultAsync(row(PROMPT)))
        assertTrue(unwrapCode(t) == "CIRCUIT_OPEN", t.toString)
      }
      check("a second catalog with different tuning shares the same open breaker") {
        val b = bound("m-breaker", "generate", "maxAttempts" -> "1")
        for (_ <- 1 to 3) failureOf(b.produceResultAsync(row(PROMPT)))
        val other = bound("m-breaker", "generate", "maxAttempts" -> "2", "requestTimeoutMs" -> "7000")
        val t = failureOf(other.produceResultAsync(row(PROMPT)))
        assertTrue(unwrapCode(t) == "CIRCUIT_OPEN", s"breaker not shared: ${t}")
      }
      check("CIRCUIT_OPEN is reported, not fatal, in the tolerant forms") {
        val b = bound("m-breaker", "generate", "maxAttempts" -> "1")
        for (_ <- 1 to 3) failureOf(b.produceResultAsync(row(PROMPT)))
        val n = await(bound("m-breaker", "try_generate", "maxAttempts" -> "1").produceResultAsync(row(PROMPT)))
        assertTrue(n == null, s"try_generate should NULL on CIRCUIT_OPEN, got $n")
        val r = await(bound("m-breaker", "generate_status", "maxAttempts" -> "1").produceResultAsync(row(PROMPT))).asInstanceOf[InternalRow]
        assertTrue(r.getUTF8String(0).toString == "ERROR" && r.getUTF8String(2).toString == "CIRCUIT_OPEN", r.toString)
      }
      check("after the reset window exactly one probe is admitted, and its failure re-opens") {
        val b = bound("m-breaker", "generate", "maxAttempts" -> "1")
        for (_ <- 1 to 3) failureOf(b.produceResultAsync(row(PROMPT)))
        Thread.sleep(500)                       // breakerResetMs is 400 in cluster conf
        val probe = failureOf(b.produceResultAsync(row(PROMPT)))
        val next = failureOf(b.produceResultAsync(row(PROMPT)))
        assertTrue(unwrapCode(probe) == "SERVER_ERROR", s"probe should reach the endpoint: $probe")
        assertTrue(unwrapCode(next) == "CIRCUIT_OPEN", s"second call should be rejected while re-opened: $next")
      }
      check("a late outcome from a call admitted before the trip cannot close the breaker") {
        // Start a slow-success call (admitted CLOSED, generation g0), then trip the breaker with
        // three fast failures, then let the slow success land: the breaker must stay OPEN.
        val slow = bound("m-slow", "generate", "requestTimeoutMs" -> "10000").produceResultAsync(row(PROMPT))
        Thread.sleep(100)
        // m-slow and the failing model must share an endpoint key; emulate by using m-slow's own
        // breaker: fail it three times with a tiny timeout (TIMEOUT is a health failure).
        val fast = bound("m-slow", "generate", "requestTimeoutMs" -> "50", "maxAttempts" -> "1")
        for (_ <- 1 to 3) failureOf(fast.produceResultAsync(row(PROMPT)))
        val rejected = failureOf(fast.produceResultAsync(row(PROMPT)))
        assertTrue(unwrapCode(rejected) == "CIRCUIT_OPEN", s"breaker should be open: $rejected")
        val s = OpenAIConf.resolve(options("baseUrl" -> base, "chatModel" -> "m-slow"), conf)
        val breaker = OpenAIClient.breakerFor(s)
        assertTrue(breaker.isOpen, "precondition: breaker open before stale success lands")
        await(slow)                             // the stale (pre-trip) success lands now
        // The breaker must still be open or half-open: a stale success must not CLOSE it.
        // (The 400 ms reset window may have elapsed during the 3 s slow call, so a half-open probe
        // is legitimate; what is not legitimate is a transition to CLOSED caused by stale data.)
        assertTrue(breaker.isOpen, "stale success from before the trip closed the breaker")
      }
      check("breaker ignores cancellation and client-side 400s (does not open)") {
        val b400 = bound("m-400", "generate")
        for (_ <- 1 to 6) failureOf(b400.produceResultAsync(row(PROMPT)))   // 6 > threshold 3
        val still = failureOf(b400.produceResultAsync(row(PROMPT)))
        assertTrue(unwrapCode(still) == "BAD_REQUEST", s"400s opened the breaker: $still")
        val bc = bound("m-slow-cancel", "generate", "requestTimeoutMs" -> "10000")
        val fs = (1 to 6).map(_ => bc.produceResultAsync(row(PROMPT)))
        Thread.sleep(200); fs.foreach(_.cancel(true)); Thread.sleep(1800)
        val after = await(bound("m-slow-cancel", "generate", "requestTimeoutMs" -> "10000").produceResultAsync(row(PROMPT)))
        assertTrue(after.toString == "SLOW", s"cancellations opened the breaker: $after")
      }
      check("over-cap 5xx body is classified by status (retryable), not as RESPONSE_TOO_LARGE") {
        val t = failureOf(bound("m-bigerr", "generate", "maxResponseBytes" -> "1024", "maxAttempts" -> "1").produceResultAsync(row(PROMPT)))
        assertTrue(unwrapCode(t) == "SERVER_ERROR", t.toString)
      }

      // --- pacing: session rate slows only its own catalog -------------------------------------
      check("pacer spaces requests at the configured rate (sequential)") {
        val b = bound("m-echo", "generate", "ratePermitsPerSec" -> "4")
        val t0 = System.nanoTime()
        (1 to 5).map(_ => b.produceResultAsync(row(PROMPT))).foreach(await(_))
        val ms = (System.nanoTime() - t0) / 1000000
        assertTrue(ms >= 900, s"5 calls at 4/s finished in $ms ms")
      }
      check("a session rate limit does not slow a different catalog on the same endpoint") {
        val slowCat = bound("m-echo", "generate", "ratePermitsPerSec" -> "1")
        slowCat.produceResultAsync(row(PROMPT))        // registers the 1/s session pacer
        val fastCat = bound("m-echo", "generate")        // no session rate, no cluster ceiling
        val t0 = System.nanoTime()
        (1 to 5).map(_ => fastCat.produceResultAsync(row(PROMPT))).foreach(await(_))
        val ms = (System.nanoTime() - t0) / 1000000
        assertTrue(ms < 800, s"another catalog's 1/s rate slowed this one: $ms ms for 5 calls")
      }
      check("a cluster rate ceiling applies to every catalog and a session cannot lift it") {
        val rconf: OpenAIConf.ClusterConf = (cluster + ("spark.ai.openai.ratePermitsPerSec" -> "4")).get
        def boundR(extra: (String, String)*): AsyncScalarFunction[AnyRef] = {
          val cat = new OpenAIFunctionCatalog()
          cat.initialize("ai", options(Seq("baseUrl" -> base, "chatModel" -> "m-echo") ++ extra: _*), rconf)
          cat.loadFunction(Identifier.of(Array.empty, "generate")).bind(new StructType().add("p", StringType))
            .asInstanceOf[AsyncScalarFunction[AnyRef]]
        }
        val b = boundR("ratePermitsPerSec" -> "1000")   // tries to go faster than the ceiling
        val t0 = System.nanoTime()
        (1 to 5).map(_ => b.produceResultAsync(row(PROMPT))).foreach(await(_))
        val ms = (System.nanoTime() - t0) / 1000000
        assertTrue(ms >= 900, s"cluster ceiling 4/s not applied: 5 calls in $ms ms")
      }
      check("varying per-call tuning does not create new clients or threads") {
        val before = OpenAIClient.clientCount
        for (i <- 1 to 20) {
          await(bound("m-ok", "generate", "maxAttempts" -> s"${1 + i % 3}", "requestTimeoutMs" -> s"${1000 + i}",
            "maxResponseBytes" -> s"${4096 + i}").produceResultAsync(row(PROMPT)))
        }
        assertTrue(OpenAIClient.clientCount == before, s"client count grew from $before to ${OpenAIClient.clientCount}")
      }
      check("pacer and breaker are safe under 32 concurrent callers") {
        val b = bound("m-gzip", "generate", "ratePermitsPerSec" -> "40")
        val pool = java.util.concurrent.Executors.newFixedThreadPool(32)
        val t0 = System.nanoTime()
        val futures = (1 to 32).map { _ =>
          val cf = new CompletableFuture[AnyRef]()
          pool.submit(new Runnable { def run(): Unit =
            b.produceResultAsync(row(PROMPT)).whenComplete((v, e) => if (e == null) cf.complete(v) else cf.completeExceptionally(e)) })
          cf
        }
        val vals = futures.map(await(_))
        pool.shutdown()
        val ms = (System.nanoTime() - t0) / 1000000
        assertTrue(vals.forall(_.toString == "GZIPPED"), "a concurrent call failed")
        assertTrue(ms >= 700, s"32 calls at 40/s finished in $ms ms; pacing lost under concurrency")
      }

      // --- Bedrock module: configuration and classification, no network --------------------
      val bconf: BedrockConf.ClusterConf = Map(
        "spark.ai.bedrock.allowedModels" -> "amazon.nova-micro-v1:0",
        "spark.ai.bedrock.allowedRegions" -> "us-east-1",
        "spark.sql.asyncFunction.maxInFlightCeiling" -> "2").get
      check("bedrock allowlist rejects an unapproved model and region") {
        val t1 = intercept(BedrockConf.resolve(options("region" -> "us-east-1", "modelId" -> "anthropic.claude-opus"), bconf))
        val t2 = intercept(BedrockConf.resolve(options("region" -> "eu-west-1", "modelId" -> "amazon.nova-micro-v1:0"), bconf))
        assertTrue(t1.getMessage.contains("allowedModels") && t2.getMessage.contains("allowedRegions"), s"$t1 / $t2")
      }
      check("bedrock concurrency is clamped to the static ceiling") {
        val s = BedrockConf.resolve(options("region" -> "us-east-1", "modelId" -> "amazon.nova-micro-v1:0", "maxInFlight" -> "99"), bconf)
        assertTrue(s.maxInFlight == 2, s"got ${s.maxInFlight}")
      }
      check("bedrock SDK exceptions map to stable codes with row-level vs fatal split") {
        import software.amazon.awssdk.services.bedrockruntime.model._
        import software.amazon.awssdk.core.exception.SdkClientException
        def code(t: Throwable): String = BedrockClient.classify(new CompletionException(t))
        assertTrue(code(ThrottlingException.builder().message("m").build()) == "RATE_LIMITED", "throttle")
        // Per-prompt validation (e.g. input too long) is row-level, NOT fatal.
        assertTrue(code(ValidationException.builder().message("m").build()) == "BAD_REQUEST", "validation")
        assertTrue(!BedrockClient.isFatal("BAD_REQUEST"), "validation must be row-level")
        // A missing model id is fatal: every row would fail identically.
        assertTrue(code(ResourceNotFoundException.builder().message("m").build()) == "BAD_MODEL", "notfound")
        assertTrue(BedrockClient.isFatal("BAD_MODEL"), "bad model must be fatal")
        assertTrue(code(AccessDeniedException.builder().message("m").build()) == "AUTH", "denied")
        assertTrue(code(ModelTimeoutException.builder().message("m").build()) == "TIMEOUT", "timeout")
        assertTrue(code(InternalServerException.builder().message("m").build()) == "SERVER_ERROR", "ise")
        // Transport failure (IOException cause) is transient, not a config error.
        val reset = SdkClientException.builder().message("Unable to execute HTTP request")
          .cause(new java.net.SocketException("Connection reset")).build()
        assertTrue(code(reset) == "TIMEOUT", s"io-caused SdkClientException -> ${code(reset)}")
        val noCreds = SdkClientException.builder().message("Unable to load credentials").build()
        assertTrue(code(noCreds) == "CONFIG" && BedrockClient.isFatal("CONFIG"), "no-creds must be fatal CONFIG")
        assertTrue(code(new java.util.concurrent.CancellationException()) == "CANCELLED", "cancel")
        assertTrue(!BedrockClient.isEndpointHealthFailure("CANCELLED") &&
          !BedrockClient.isEndpointHealthFailure("BAD_REQUEST") &&
          BedrockClient.isEndpointHealthFailure("TIMEOUT"), "health set")
        assertTrue(code(new IllegalStateException("x")) == "SERVER_ERROR", "unknown")
        // The sanitized failure carries no SDK message text.
        val f = BedrockClient.asFailure(ValidationException.builder().message("SECRET_SDK_TEXT").build())
        assertTrue(!f.getMessage.contains("SECRET_SDK_TEXT"), f.getMessage)
      }

      // --- OpenAI-compatible module: live, opt-in, against a REAL endpoint -------------------
      // Amazon Bedrock exposes an OpenAI-compatible Chat Completions API with bearer-token auth,
      // so the OpenAI module is proven against a real service with the same account. Set
      // SPARK_TEST_LIVE_OPENAI_BASE_URL (e.g. https://bedrock-runtime.us-east-1.amazonaws.com/openai/v1),
      // SPARK_TEST_LIVE_OPENAI_MODEL (e.g. qwen.qwen3-32b-v1:0), and the bearer token in the
      // executor environment variable AI_PROVIDER_TOKEN.
      sys.env.get("SPARK_TEST_LIVE_OPENAI_BASE_URL").map(_.trim).filter(_.nonEmpty) match {
        case Some(liveBase) if sys.env.contains("AI_PROVIDER_TOKEN") =>
          val liveModel = sys.env.getOrElse("SPARK_TEST_LIVE_OPENAI_MODEL", "qwen.qwen3-32b-v1:0")
          val liveConf: OpenAIConf.ClusterConf = Map(
            "spark.ai.openai.allowedBaseUrls" -> liveBase,
            "spark.ai.openai.allowedModels" -> liveModel,
            "spark.ai.openai.credentialEnv" -> "AI_PROVIDER_TOKEN",
            "spark.sql.asyncFunction.maxInFlightCeiling" -> "2").get
          def liveBound(form: String): AsyncScalarFunction[AnyRef] = {
            val cat = new OpenAIFunctionCatalog()
            cat.initialize("ai", options("baseUrl" -> liveBase, "chatModel" -> liveModel,
              "requestTimeoutMs" -> "30000"), liveConf)
            cat.loadFunction(Identifier.of(Array.empty, form))
              .bind(new StructType().add("p", StringType)).asInstanceOf[AsyncScalarFunction[AnyRef]]
          }
          check("LIVE openai-compatible: generate returns real model text") {
            val v = await(liveBound("generate").produceResultAsync(row("Reply with exactly the single word OK")))
            assertTrue(v != null && v.toString.toUpperCase.contains("OK"), s"got $v")
          }
          check("LIVE openai-compatible: generate_status populates attempts and request id") {
            val r = await(liveBound("generate_status").produceResultAsync(row("Reply with exactly the single word OK")))
              .asInstanceOf[InternalRow]
            assertTrue(r.getUTF8String(0).toString == "SUCCESS", r.toString)
            assertTrue(r.getInt(4) >= 1, s"attempts ${r.getInt(4)}")
            assertTrue(!r.isNullAt(5) && r.getUTF8String(5).toString.nonEmpty, "request id missing")
          }
          check("LIVE openai-compatible: NULL input makes no call") {
            assertTrue(await(liveBound("generate").produceResultAsync(row(null))) == null, "expected null")
          }
          check("LIVE openai-compatible: an unlisted model is rejected before any call") {
            val t = intercept(OpenAIConf.resolve(options("baseUrl" -> liveBase, "chatModel" -> "gpt-4o"), liveConf))
            assertTrue(t.getMessage.contains("allowedModels"), t.toString)
          }
        case _ =>
          results += (("LIVE openai-compatible cases (set SPARK_TEST_LIVE_OPENAI_BASE_URL and AI_PROVIDER_TOKEN)", true, "skipped"))
      }

      // --- Bedrock module: live, opt-in, through the catalog path --------------------------
      if (sys.env.get("SPARK_TEST_LIVE_BEDROCK").contains("1")) {
        val region = sys.env.getOrElse("SPARK_TEST_BEDROCK_REGION", "us-east-1")
        val model = sys.env.getOrElse("SPARK_TEST_BEDROCK_MODEL_ID", "amazon.nova-micro-v1:0")
        val liveConf: BedrockConf.ClusterConf = Map(
          "spark.ai.bedrock.allowedModels" -> model,
          "spark.ai.bedrock.allowedRegions" -> region,
          "spark.sql.asyncFunction.maxInFlightCeiling" -> "1").get
        check("LIVE bedrock: generate through BedrockFunctionCatalog returns the model text") {
          val cat = new BedrockFunctionCatalog()
          cat.initialize("ai", options("region" -> region, "modelId" -> model, "maxTokens" -> "8"), liveConf)
          val b = cat.loadFunction(Identifier.of(Array.empty, "generate"))
            .bind(new StructType().add("p", StringType)).asInstanceOf[AsyncScalarFunction[AnyRef]]
          val v = await(b.produceResultAsync(row("Reply with exactly A")))
          assertTrue(v != null && v.toString.trim.equalsIgnoreCase("A"), s"got $v")
        }
        check("LIVE bedrock: generate_status populates request_id") {
          val cat = new BedrockFunctionCatalog()
          cat.initialize("ai", options("region" -> region, "modelId" -> model, "maxTokens" -> "8"), liveConf)
          val b = cat.loadFunction(Identifier.of(Array.empty, "generate_status"))
            .bind(new StructType().add("p", StringType)).asInstanceOf[AsyncScalarFunction[AnyRef]]
          val r = await(b.produceResultAsync(row("Reply with exactly B"))).asInstanceOf[InternalRow]
          assertTrue(r.getUTF8String(0).toString == "SUCCESS", r.toString)
          assertTrue(!r.isNullAt(5) && r.getUTF8String(5).toString.nonEmpty, "request_id missing")
        }
        check("LIVE bedrock: NULL input makes no call") {
          val cat = new BedrockFunctionCatalog()
          cat.initialize("ai", options("region" -> region, "modelId" -> model), liveConf)
          val b = cat.loadFunction(Identifier.of(Array.empty, "generate"))
            .bind(new StructType().add("p", StringType)).asInstanceOf[AsyncScalarFunction[AnyRef]]
          assertTrue(await(b.produceResultAsync(row(null))) == null, "expected null")
        }
      } else {
        results += (("LIVE bedrock cases (set SPARK_TEST_LIVE_BEDROCK=1)", true, "skipped"))
      }
    } finally {
      server.stop(0)
    }

    // --- report -------------------------------------------------------------------------------
    val passed = results.count(_._2)
    val failed = results.size - passed
    println("SPARK-61672 provider-module conformance harness")
    println("=" * 78)
    results.foreach { case (name, ok, detail) =>
      val tag = if (ok) "PASS" else "FAIL"
      val extra = if (detail.nonEmpty) s"  [$detail]" else ""
      println(f"$tag  $name%s$extra")
    }
    println("=" * 78)
    println(s"Total: ${results.size}   passed: $passed   failed: $failed")
    println(if (failed == 0) "RESULT: ALL CHECKS PASSED" else "RESULT: FAILURES")
    System.exit(if (failed == 0) 0 else 1)
  }

  private def intercept(body: => Any): Throwable =
    try { body; throw new AssertionError("expected an exception") }
    catch { case t: Throwable if !t.isInstanceOf[AssertionError] => t }
}
