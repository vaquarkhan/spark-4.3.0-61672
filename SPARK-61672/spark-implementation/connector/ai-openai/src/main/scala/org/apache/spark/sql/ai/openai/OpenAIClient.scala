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

package org.apache.spark.sql.ai.openai

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, IOException}
import java.net.URI
import java.net.http.{HttpClient, HttpRequest, HttpResponse, HttpTimeoutException}
import java.nio.ByteBuffer
import java.time.{Duration, ZonedDateTime}
import java.time.format.DateTimeFormatter
import java.util.concurrent.{CompletableFuture, CompletionException, ConcurrentHashMap, Flow, TimeUnit}
import java.util.concurrent.atomic.{AtomicInteger, AtomicLong, AtomicReference}
import java.util.zip.GZIPInputStream

import scala.util.control.NonFatal

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.fasterxml.jackson.databind.node.ObjectNode

/**
 * SPARK-61672 OpenAI-compatible client.
 *
 * One client per connection identity (base URL plus credential source) for the executor
 * lifetime. All per-call tuning (timeouts, retry budget, size cap, model) is passed with each
 * call, so a session cannot cause new clients or threads to be created by varying options.
 *
 * Shared endpoint state is keyed by base URL plus model:
 *  - The circuit breaker. Its policy is cluster-only. Only endpoint-health failures (timeouts,
 *    5xx, rate limiting) count; cancellation and per-row errors are neutral. Admissions are
 *    stamped with a generation so a late outcome from before a trip cannot change state.
 *  - The cluster pacer, driven only by the cluster rate ceiling. A session may request a lower
 *    rate, which is applied in a per-catalog pacer layered on top, so one session can slow its
 *    own queries but never anyone else's.
 *
 * Request and response bodies use Jackson. Responses may be gzip-encoded; the size cap is
 * enforced while the body streams in, and each attempt is time-bounded end to end (headers and
 * body). `Retry-After` is honored in delay-seconds and HTTP-date forms. Retries stop at
 * `maxAttempts` or the absolute `maxElapsedMs` deadline, which includes pacer wait. Cancelling the
 * returned future cancels the in-flight request and stops a pending retry or pacing wait.
 *
 * Only sanitized error codes are surfaced; prompts, tokens, provider bodies, and the endpoint are
 * never placed in a row-level error, log line, or plan by this module.
 */
class OpenAIClient private (baseUrl: String, credentialEnv: String) {

  private val http: HttpClient = HttpClient.newBuilder()
    .version(HttpClient.Version.HTTP_1_1)
    .followRedirects(HttpClient.Redirect.NEVER)
    .connectTimeout(Duration.ofMillis(OpenAIClient.CONNECT_TIMEOUT_MS))
    .build()

  private val token: String = sys.env.getOrElse(credentialEnv, throw new OpenAIException("CONFIG"))

  private val endpoint: URI = URI.create(baseUrl + "/chat/completions")

  /** Issues a chat completion under the given per-call policy. */
  def chatCompletion(prompt: String, s: OpenAISettings): CompletableFuture[ChatResult] = {
    val result = new CompletableFuture[ChatResult]()
    val breaker = OpenAIClient.breakerFor(s)
    val generation = breaker.tryAcquire()
    if (generation < 0) {
      result.completeExceptionally(new OpenAIException("CIRCUIT_OPEN"))
      return result
    }
    val body = OpenAIClient.buildBody(s.chatModel, prompt)
    val inFlight = new AtomicReference[CompletableFuture[_]]()
    val startNanos = System.nanoTime()   // deadline includes pacing

    val chain = OpenAIClient.pace(s, inFlight, s.maxElapsedMs).thenCompose[ChatResult] { _ =>
      if (result.isCancelled) OpenAIClient.failedFuture(new OpenAIException("CANCELLED"))
      else sendWithRetry(body, s, attempt = 1, startNanos, inFlight, result)
    }
    chain.whenComplete { (res, err) =>
      if (err == null) {
        breaker.recordSuccess(generation)
        result.complete(res)
      } else {
        val failure = OpenAIClient.asFailure(err)
        if (OpenAIClient.isEndpointHealthFailure(failure.code)) breaker.recordFailure(generation)
        else breaker.recordNeutral(generation)
        result.completeExceptionally(failure)
      }
    }
    // Propagate cancellation to whichever stage is active: pacing wait, HTTP exchange, or backoff.
    result.whenComplete { (_, _) =>
      if (result.isCancelled) {
        val f = inFlight.get()
        if (f != null) f.cancel(true)
      }
    }
    result
  }

  private def sendWithRetry(
      body: Array[Byte],
      s: OpenAISettings,
      attempt: Int,
      startNanos: Long,
      inFlight: AtomicReference[CompletableFuture[_]],
      result: CompletableFuture[ChatResult]): CompletableFuture[ChatResult] = {
    if (result.isCancelled) {
      return OpenAIClient.failedFuture(new OpenAIException("CANCELLED", attempt))
    }
    val remainingMs = s.maxElapsedMs - OpenAIClient.elapsedMs(startNanos)
    if (remainingMs <= 0) {
      // Budget exhausted before this attempt. If nothing was ever sent this is a local
      // condition (pacing ate the budget), not endpoint ill-health.
      val code = if (attempt == 1) "PACED_OUT" else "TIMEOUT"
      return OpenAIClient.failedFuture(new OpenAIException(code, attempt - 1))
    }
    // Each attempt is bounded end to end (headers AND body) by the smaller of the per-request
    // timeout and the time left under the absolute deadline.
    val attemptBudgetMs = math.min(s.requestTimeoutMs, remainingMs)
    val request = HttpRequest.newBuilder(endpoint)
      .timeout(Duration.ofMillis(attemptBudgetMs))
      .header("Authorization", s"Bearer $token")
      .header("Content-Type", "application/json")
      .header("Accept", "application/json")
      .header("Accept-Encoding", "gzip")
      .POST(HttpRequest.BodyPublishers.ofByteArray(body))
      .build()

    val httpFuture = http.sendAsync(request, OpenAIClient.boundedBodyHandler(s.maxResponseBytes))
    // HttpRequest.timeout only covers time-to-headers. To bound the BODY phase too, and actually
    // abort the exchange (the JDK aborts only on cancel(), not on an external exceptional
    // completion), a timer CANCELS the send future when the attempt budget expires.
    val timedOut = new java.util.concurrent.atomic.AtomicBoolean(false)
    val timer = OpenAIClient.scheduler.schedule(new Runnable {
      def run(): Unit = { timedOut.set(true); httpFuture.cancel(true) }
    }, attemptBudgetMs, TimeUnit.MILLISECONDS)
    httpFuture.whenComplete { (_, _) => timer.cancel(false) }
    inFlight.set(httpFuture)
    if (result.isCancelled) httpFuture.cancel(true)   // close the pre-send window

    httpFuture
      .handle[CompletableFuture[ChatResult]] { (resp, err) =>
        val requestId = if (resp == null) null else OpenAIClient.requestIdOf(resp)
        val cls = OpenAIClient.classify(resp, err) match {
          // A cancel we issued for the attempt budget is a timeout, not a user cancellation.
          case Classification.Terminal("CANCELLED") if timedOut.get() =>
            Classification.Retryable("TIMEOUT", 0L)
          case other => other
        }
        cls match {
          case Classification.Ok =>
            try {
              val decoded = OpenAIClient.decode(resp, s.maxResponseBytes)
              val text = OpenAIClient.parseText(decoded)
              CompletableFuture.completedFuture(ChatResult(text, attempt, requestId))
            } catch {
              case e: OpenAIException =>
                OpenAIClient.failedFuture[ChatResult](
                  new OpenAIException(e.code, attempt, requestId))
              case NonFatal(_) =>
                OpenAIClient.failedFuture[ChatResult](
                  new OpenAIException("SCHEMA", attempt, requestId))
            }
          case Classification.Retryable(code, retryAfterMs) =>
            val delay = OpenAIClient.backoffMs(attempt, retryAfterMs)
            val elapsed = OpenAIClient.elapsedMs(startNanos)
            if (attempt < s.maxAttempts && elapsed + delay < s.maxElapsedMs) {
              val wait = OpenAIClient.delayed(delay)
              inFlight.set(wait)   // so a cancel during backoff stops the next attempt
              if (result.isCancelled) wait.cancel(true)
              wait.thenCompose[ChatResult] { _ =>
                sendWithRetry(body, s, attempt + 1, startNanos, inFlight, result)
              }
            } else {
              OpenAIClient.failedFuture[ChatResult](new OpenAIException(code, attempt, requestId))
            }
          case Classification.Terminal(code) =>
            OpenAIClient.failedFuture[ChatResult](new OpenAIException(code, attempt, requestId))
        }
      }.thenCompose(x => x)
  }
}

object OpenAIClient {
  private[openai] val CONNECT_TIMEOUT_MS = 15000L

  private val clients = new ConcurrentHashMap[String, OpenAIClient]()
  private val clusterPacers = new ConcurrentHashMap[String, Pacer]()
  private val sessionPacers = new ConcurrentHashMap[String, Pacer]()
  private val breakers = new ConcurrentHashMap[String, CircuitBreaker]()
  private[openai] val scheduler =
    java.util.concurrent.Executors.newScheduledThreadPool(4, (r: Runnable) => {
      val t = new Thread(r, "ai-openai-retry"); t.setDaemon(true); t
    })
  private val mapper = new ObjectMapper()

  /** Connection identity only. Nothing a session can vary appears here. */
  private def connectionKey(s: OpenAISettings): String = s"${s.baseUrl}|${s.credentialEnv}"

  /** Endpoint key for shared breaker and cluster pacer. */
  private def endpointKey(s: OpenAISettings): String = s"${s.baseUrl}|${s.chatModel}"

  def forSettings(s: OpenAISettings): OpenAIClient =
    clients.computeIfAbsent(connectionKey(s), _ => new OpenAIClient(s.baseUrl, s.credentialEnv))

  /** Number of live clients; for tests and metrics. */
  def clientCount: Int = clients.size()

  /** Clears shared breaker and pacer state. For tests only; clients are kept. */
  def resetSharedStateForTests(): Unit = {
    breakers.clear(); clusterPacers.clear(); sessionPacers.clear()
  }

  /**
   * Pacing in two layers. The cluster pacer is shared per endpoint and set ONLY from the cluster
   * rate ceiling, so no session can change it. If the session asked for a lower rate, a
   * per-catalog pacer (keyed by endpoint plus the requested rate) is applied as well; it slows
   * only the catalogs that asked for it.
   */
  /** Upper bound on distinct session pacers per JVM; beyond it, requests are not session-paced. */
  private val MAX_SESSION_PACERS = 1024

  private[openai] def pace(s: OpenAISettings, inFlight: AtomicReference[CompletableFuture[_]],
      maxWaitMs: Long): CompletableFuture[Void] = {
    val start = System.nanoTime()
    val cluster = if (s.clusterRatePermitsPerSec > 0) {
      clusterPacers.computeIfAbsent(endpointKey(s), _ => new Pacer(s.clusterRatePermitsPerSec))
    } else null
    val session = if (s.ratePermitsPerSec > 0 &&
        (s.clusterRatePermitsPerSec <= 0 || s.ratePermitsPerSec < s.clusterRatePermitsPerSec)) {
      val key = endpointKey(s) + "|" + s.ratePermitsPerSec
      val existing = sessionPacers.get(key)
      if (existing != null) existing
      else if (sessionPacers.size() < MAX_SESSION_PACERS) {
        sessionPacers.computeIfAbsent(key, _ => new Pacer(s.ratePermitsPerSec))
      } else null   // map bounded: a session cannot grow it without limit
    } else null
    val first = if (cluster != null) cluster.acquire(inFlight, maxWaitMs)
      else CompletableFuture.completedFuture[Void](null)
    if (session == null) first
    else first.thenCompose[Void] { _ =>
      // The second layer only gets what is left of the budget.
      val remaining = maxWaitMs - (System.nanoTime() - start) / 1000000L
      session.acquire(inFlight, math.max(0L, remaining))
    }
  }

  /** The breaker for an endpoint; the stricter cluster policy wins if two differ. */
  def breakerFor(s: OpenAISettings): CircuitBreaker = {
    val b = breakers.computeIfAbsent(endpointKey(s), _ => new CircuitBreaker())
    b.tightenTo(s.breakerFailureThreshold, s.breakerResetMs)
    b
  }

  private[openai] def delayed(ms: Long): CompletableFuture[Void] = {
    if (ms <= 0) return CompletableFuture.completedFuture(null)
    val f = new CompletableFuture[Void]()
    val task = scheduler.schedule(
      new Runnable { def run(): Unit = f.complete(null) }, ms, TimeUnit.MILLISECONDS)
    f.whenComplete { (_, _) => if (f.isCancelled) task.cancel(false) }
    f
  }

  /**
   * A fixed-interval request pacer. `acquire` never blocks the task thread: it returns a future
   * that completes when the caller's slot arrives, registers that future so a cancel during the
   * wait is honored, and fails fast with RATE_LIMITED if the wait alone would exceed the deadline.
   */
  private[openai] final class Pacer(permitsPerSec: Int) {
    private val intervalNanos = 1000000000L / math.max(1, permitsPerSec)
    private val nextNanos = new AtomicLong(System.nanoTime())

    /**
     * Reserves the next slot only if its wait fits the budget. A rejected caller does not
     * consume a slot, so a burst cannot push the schedule into the future and starve later
     * callers while the endpoint sits idle. Rejection uses PACED_OUT, a local condition that is
     * not an endpoint-health signal.
     */
    def acquire(inFlight: AtomicReference[CompletableFuture[_]],
        maxWaitMs: Long): CompletableFuture[Void] = {
      while (true) {
        val now = System.nanoTime()
        val prev = nextNanos.get()
        val slot = math.max(now, prev)
        val waitMs = math.max(0L, (slot - now) / 1000000L)
        if (waitMs > maxWaitMs) return failedFuture[Void](new OpenAIException("PACED_OUT"))
        if (nextNanos.compareAndSet(prev, slot + intervalNanos)) {
          if (waitMs == 0L) return CompletableFuture.completedFuture(null)
          val f = delayed(waitMs + 1L)
          inFlight.set(f)
          return f
        }
        // lost the race; recompute from the new schedule
      }
      throw new IllegalStateException("unreachable")
    }
  }

  /**
   * A circuit breaker shared per endpoint with generation-stamped admissions.
   *
   * CLOSED: calls flow; `threshold` consecutive endpoint-health failures open it.
   * OPEN: calls are rejected until the reset window elapses.
   * HALF_OPEN: exactly one probe is admitted (CAS). Its success closes the breaker; its failure
   * re-opens it. Other callers are rejected while the probe is out.
   *
   * Every admission receives the current generation. A trip, and the OPEN to HALF_OPEN move,
   * each start a new generation, so outcomes of calls admitted earlier are ignored and cannot
   * close, extend, or re-probe the breaker.
   */
  final class CircuitBreaker {
    private val CLOSED = 0; private val OPEN = 1; private val HALF_OPEN = 2
    private val state = new AtomicInteger(CLOSED)
    private val generation = new AtomicLong(0L)
    private val consecutiveFailures = new AtomicInteger(0)
    private val openUntilNanos = new AtomicLong(0L)
    private val threshold = new AtomicInteger(Int.MaxValue)
    private val resetNanos = new AtomicLong(Long.MaxValue)

    def tightenTo(failureThreshold: Int, resetMs: Long): Unit = {
      threshold.updateAndGet(cur => math.min(cur, math.max(1, failureThreshold)))
      resetNanos.updateAndGet(cur => math.min(cur, math.max(1L, resetMs) * 1000000L))
    }

    /**
     * Returns the admission generation, or -1 if the caller is rejected. The generation is read
     * BEFORE the state so a trip that lands between the two reads leaves this caller holding the
     * pre-trip generation, which the record methods then ignore.
     */
    def tryAcquire(): Long = {
      val gen = generation.get()
      state.get() match {
        case CLOSED => gen
        case OPEN =>
          if (System.nanoTime() >= openUntilNanos.get() && state.compareAndSet(OPEN, HALF_OPEN)) {
            generation.incrementAndGet()   // the probe is the only member of this generation
          } else -1L
        case _ => -1L
      }
    }

    private def current(gen: Long): Boolean = gen == generation.get()

    def recordSuccess(gen: Long): Unit = synchronized {
      if (!current(gen)) return
      consecutiveFailures.set(0)
      state.set(CLOSED)
    }

    /** A client-side or cancelled outcome: no health signal. A neutral probe re-arms a probe. */
    def recordNeutral(gen: Long): Unit = synchronized {
      if (!current(gen)) return
      if (state.get() == HALF_OPEN) {
        state.set(OPEN)
        openUntilNanos.set(System.nanoTime())   // immediately eligible for a new probe
      }
    }

    def recordFailure(gen: Long): Unit = synchronized {
      if (!current(gen)) return
      if (state.get() == HALF_OPEN) trip()
      else if (consecutiveFailures.incrementAndGet() >= threshold.get()) trip()
    }

    private def trip(): Unit = {
      consecutiveFailures.set(0)
      generation.incrementAndGet()
      openUntilNanos.set(System.nanoTime() + resetNanos.get())
      state.set(OPEN)
    }

    def isOpen: Boolean = state.get() != CLOSED
  }

  /**
   * Codes that must fail the task even in the tolerant forms: not row-level conditions.
   * CIRCUIT_OPEN is deliberately NOT fatal: `generate` fails on it anyway, `generate_status`
   * reports it in `error_code` (the column that exists for this), and `try_generate` returns NULL
   * rather than turning a short outage into a task-failure storm across the executor.
   */
  private val fatalCodes = Set("AUTH", "CONFIG", "TLS")

  /** Codes that indicate endpoint ill-health and therefore count toward opening the breaker. */
  private val healthCodes = Set("TIMEOUT", "SERVER_ERROR", "RATE_LIMITED")

  def isFatal(code: String): Boolean = fatalCodes.contains(code)

  def isEndpointHealthFailure(code: String): Boolean = healthCodes.contains(code)

  def failedFuture[T](e: Throwable): CompletableFuture[T] = {
    val f = new CompletableFuture[T]()
    f.completeExceptionally(e)
    f
  }

  /** Builds the OpenAI-compatible chat payload with Jackson, so any prompt is escaped correctly. */
  private[openai] def buildBody(model: String, prompt: String): Array[Byte] = {
    val root: ObjectNode = mapper.createObjectNode()
    root.put("model", model)
    val messages = root.putArray("messages")
    val msg = messages.addObject()
    msg.put("role", "user")
    msg.put("content", prompt)
    mapper.writeValueAsBytes(root)
  }

  /** Extracts choices[0].message.content; SCHEMA if the shape is wrong. */
  private[openai] def parseText(body: Array[Byte]): String = {
    val root: JsonNode = try mapper.readTree(body) catch {
      case NonFatal(_) => throw new OpenAIException("SCHEMA")
    }
    if (root == null) throw new OpenAIException("SCHEMA")
    val content = root.path("choices").path(0).path("message").path("content")
    if (content.isMissingNode || content.isNull || !content.isTextual) {
      throw new OpenAIException("SCHEMA")
    }
    content.asText()
  }

  /**
   * A body handler that enforces the size cap while the body streams in. If Content-Length
   * already exceeds the cap the subscription is cancelled before any body byte is requested.
   * Otherwise bytes are counted as they arrive and the subscription is cancelled past the cap.
   * For gzip bodies this bounds the compressed size; `decode` then bounds the decoded size.
   */
  private[openai] def boundedBodyHandler(maxBytes: Long): HttpResponse.BodyHandler[Array[Byte]] =
    (info: HttpResponse.ResponseInfo) => {
      val declared = try info.headers().firstValueAsLong("Content-Length") catch {
        case _: NumberFormatException => java.util.OptionalLong.empty()
      }
      if (declared.isPresent && declared.getAsLong > maxBytes) new RejectingSubscriber()
      else new BoundedSubscriber(maxBytes)
    }

  /** Marker body for an over-cap response. */
  private[openai] val TOO_LARGE_SENTINEL: Array[Byte] = Array[Byte](0x7f, 'T', 'L')

  /** Cancels the subscription immediately; no body bytes are requested or read. */
  private final class RejectingSubscriber extends HttpResponse.BodySubscriber[Array[Byte]] {
    private val done = CompletableFuture.completedFuture(TOO_LARGE_SENTINEL)
    override def getBody: java.util.concurrent.CompletionStage[Array[Byte]] = done
    override def onSubscribe(s: Flow.Subscription): Unit = s.cancel()
    override def onNext(items: java.util.List[ByteBuffer]): Unit = {}
    override def onError(t: Throwable): Unit = {}
    override def onComplete(): Unit = {}
  }

  private final class BoundedSubscriber(maxBytes: Long)
    extends HttpResponse.BodySubscriber[Array[Byte]] {
    private val done = new CompletableFuture[Array[Byte]]()
    private val buf = new ByteArrayOutputStream()
    private var total = 0L                        // accessed only from the subscriber's thread
    @volatile private var subscription: Flow.Subscription = _

    override def getBody: java.util.concurrent.CompletionStage[Array[Byte]] = done
    override def onSubscribe(s: Flow.Subscription): Unit = {
      subscription = s
      s.request(1)                                // one buffer at a time: bounded backpressure
    }
    override def onNext(items: java.util.List[ByteBuffer]): Unit = {
      if (done.isDone) return
      val it = items.iterator()
      while (it.hasNext) {
        val b = it.next()
        total += b.remaining()
        if (total > maxBytes) {
          done.complete(TOO_LARGE_SENTINEL)
          val s = subscription
          if (s != null) s.cancel()
          return
        }
        val arr = new Array[Byte](b.remaining())
        b.get(arr)
        buf.write(arr)
      }
      val s = subscription
      if (s != null) s.request(1)
    }
    override def onError(t: Throwable): Unit = done.completeExceptionally(t)
    override def onComplete(): Unit = if (!done.isDone) done.complete(buf.toByteArray)
  }

  /** Decodes the body, honoring gzip, and enforces the decoded size cap. */
  private[openai] def decode(resp: HttpResponse[Array[Byte]], maxBytes: Long): Array[Byte] = {
    val raw = resp.body()
    if (raw eq TOO_LARGE_SENTINEL) throw new OpenAIException("RESPONSE_TOO_LARGE")
    val encoding = resp.headers().firstValue("Content-Encoding").orElse("").toLowerCase
    if (encoding.contains("gzip")) {
      val in = new GZIPInputStream(new ByteArrayInputStream(raw))
      val out = new ByteArrayOutputStream()
      val chunk = new Array[Byte](8192)
      var total = 0L
      try {
        var n = in.read(chunk)
        while (n >= 0) {
          total += n
          if (total > maxBytes) throw new OpenAIException("RESPONSE_TOO_LARGE")
          out.write(chunk, 0, n)
          n = in.read(chunk)
        }
      } catch {
        case e: OpenAIException => throw e
        case _: IOException => throw new OpenAIException("SCHEMA")
      } finally in.close()
      out.toByteArray
    } else {
      if (raw.length > maxBytes) throw new OpenAIException("RESPONSE_TOO_LARGE")
      raw
    }
  }

  private[openai] def requestIdOf(resp: HttpResponse[Array[Byte]]): String = {
    val h = resp.headers()
    val id = h.firstValue("x-request-id").orElse(h.firstValue("request-id").orElse(null))
    // Untrusted header: bound its length before it becomes query output.
    if (id != null && id.length > 128) id.substring(0, 128) else id
  }

  private[openai] def classify(resp: HttpResponse[Array[Byte]], err: Throwable): Classification = {
    if (err != null) {
      val root = err match { case c: CompletionException if c.getCause != null => c.getCause
        case e => e }
      root match {
        case _: HttpTimeoutException => Classification.Retryable("TIMEOUT", 0L)
        case _: java.util.concurrent.TimeoutException => Classification.Retryable("TIMEOUT", 0L)
        case _: java.util.concurrent.CancellationException => Classification.Terminal("CANCELLED")
        case _: javax.net.ssl.SSLException => Classification.Terminal("TLS")   // fatal
        case _ => Classification.Retryable("TIMEOUT", 0L) // connect/read failure
      }
    } else resp.statusCode() match {
      // Status first: an over-cap error body is still an error of that status.
      case s if s >= 200 && s < 300 =>
        if (resp.body() eq TOO_LARGE_SENTINEL) Classification.Terminal("RESPONSE_TOO_LARGE")
        else Classification.Ok
      case 429 => Classification.Retryable("RATE_LIMITED", retryAfterMs(resp))
      case 408 => Classification.Retryable("TIMEOUT", retryAfterMs(resp))
      case 500 | 502 | 503 | 504 => Classification.Retryable("SERVER_ERROR", retryAfterMs(resp))
      case 400 | 404 | 413 | 422 => Classification.Terminal("BAD_REQUEST")
      case 401 | 403 => Classification.Terminal("AUTH")
      case _ => Classification.Terminal("SERVER_ERROR")
    }
  }

  /** Reads Retry-After from the response; 0 when absent or invalid. */
  private[openai] def retryAfterMs(resp: HttpResponse[Array[Byte]]): Long = {
    val h = resp.headers().firstValue("Retry-After")
    if (h.isPresent) parseRetryAfterMs(h.get()) else 0L
  }

  /**
   * Parses a Retry-After value as delay-seconds or an RFC 1123 HTTP-date (RFC 9110 section
   * 10.2.3). Returns 0 for a value in the past, a negative delay, or anything unparseable, so a
   * bad header degrades to the normal jittered backoff rather than a stall.
   */
  def parseRetryAfterMs(value: String): Long = {
    val v = if (value == null) "" else value.trim
    if (v.isEmpty) return 0L
    try {
      math.max(0L, java.lang.Long.parseLong(v) * 1000L)
    } catch {
      case _: NumberFormatException =>
        try {
          val at = ZonedDateTime.parse(v, DateTimeFormatter.RFC_1123_DATE_TIME).toInstant
          math.max(0L, Duration.between(java.time.Instant.now(), at).toMillis)
        } catch {
          case NonFatal(_) => 0L
        }
    }
  }

  /** Equal jitter: at least half the base so a retry is never immediate. */
  private[openai] def backoffMs(attempt: Int, retryAfterMs: Long): Long = {
    if (retryAfterMs > 0) retryAfterMs
    else {
      val base = math.min(30000L, (1L << math.min(attempt, 10)) * 100L)
      base / 2 + (base / 2 * math.random()).toLong
    }
  }

  private[openai] def elapsedMs(startNanos: Long): Long =
    (System.nanoTime() - startNanos) / 1000000L

  /** Unwraps any wrapper and returns the sanitized failure, defaulting to SERVER_ERROR. */
  def asFailure(err: Throwable): OpenAIException = err match {
    case e: OpenAIException => e
    case _: java.util.concurrent.CancellationException => new OpenAIException("CANCELLED")
    case c if c.getCause != null && (c.getCause ne c) => asFailure(c.getCause)
    case _ => new OpenAIException("SERVER_ERROR")
  }

  def errorCode(err: Throwable): String = asFailure(err).code

  def sanitizedMessage(code: String): String = code match {
    case "TIMEOUT" => "the request timed out"
    case "RATE_LIMITED" => "the endpoint rate-limited the request"
    case "SERVER_ERROR" => "the endpoint returned a server error"
    case "BAD_REQUEST" => "the endpoint rejected the request"
    case "AUTH" => "the endpoint rejected the credentials"
    case "CONFIG" => "the provider credential environment variable is not set on the executor"
    case "TLS" => "the TLS handshake with the endpoint failed"
    case "SCHEMA" => "the response could not be parsed"
    case "RESPONSE_TOO_LARGE" => "the response exceeded the configured size cap"
    case "CIRCUIT_OPEN" => "the endpoint circuit is open"
    case "PACED_OUT" => "the local rate limit could not admit the request within the deadline"
    case "CANCELLED" => "the request was cancelled"
    case _ => "the request failed"
  }
}

sealed trait Classification
object Classification {
  case object Ok extends Classification
  case class Retryable(code: String, retryAfterMs: Long) extends Classification
  case class Terminal(code: String) extends Classification
}
