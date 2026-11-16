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

package org.apache.spark.sql.ai.bedrock

import java.io.IOException
import java.time.Duration
import java.util.concurrent.{CompletableFuture, CompletionException, ConcurrentHashMap}
import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}

import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal

import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider
import software.amazon.awssdk.awscore.AwsRequestOverrideConfiguration
import software.amazon.awssdk.awscore.exception.AwsServiceException
import software.amazon.awssdk.awscore.retry.AwsRetryStrategy
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.bedrockruntime.BedrockRuntimeAsyncClient
import software.amazon.awssdk.services.bedrockruntime.model.{ContentBlock, ConversationRole, ConverseRequest, InferenceConfiguration, Message}

/**
 * SPARK-61672 Amazon Bedrock client.
 *
 * One SDK client per REGION for the executor lifetime; per-call tuning (model, token cap,
 * temperature, timeouts) is applied per request through the SDK's request override, so a session
 * cannot cause new clients or Netty event loops to be created by varying options.
 *
 * One circuit breaker per endpoint (region plus model), shared across all tuning, with
 * generation-stamped admissions so a late outcome from before a trip cannot change state. Only
 * endpoint-health failures open it. Credentials come from the AWS default chain on the executor.
 *
 * Only sanitized error codes are surfaced: a prompt, credential, or raw provider body never
 * appears in a row-level error or plan produced by this module. Cancelling the returned future
 * cancels the in-flight SDK call.
 */
class BedrockClient private (region: String) {

  private val client: BedrockRuntimeAsyncClient = {
    val overrideConfig = ClientOverrideConfiguration.builder()
      .retryStrategy(AwsRetryStrategy.standardRetryStrategy())
      .build()
    // One client per region is shared by every task and model on the executor, so the HTTP
    // concurrency must cover maxInFlight across all task slots, not Netty's default of 50.
    val httpBuilder = software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient.builder()
      .maxConcurrency(BedrockClient.MAX_HTTP_CONCURRENCY)
    BedrockRuntimeAsyncClient.builder()
      .region(Region.of(region))
      .credentialsProvider(DefaultCredentialsProvider.builder().build())
      .overrideConfiguration(overrideConfig)
      .httpClientBuilder(httpBuilder)
      .build()
  }

  /** Issues one Converse call under the given per-call policy. */
  def converse(prompt: String, s: BedrockSettings): CompletableFuture[ConverseResult] = {
    val result = new CompletableFuture[ConverseResult]()
    val breaker = BedrockClient.breakerFor(s)
    val generation = breaker.tryAcquire()
    if (generation < 0) {
      result.completeExceptionally(new BedrockException("CIRCUIT_OPEN"))
      return result
    }
    val message = Message.builder()
      .role(ConversationRole.USER)
      .content(ContentBlock.builder().text(prompt).build())
      .build()
    val inference = InferenceConfiguration.builder()
      .maxTokens(s.maxTokens)
      .temperature(s.temperature)
      .build()
    val perCall = AwsRequestOverrideConfiguration.builder()
      .apiCallTimeout(Duration.ofMillis(s.apiCallTimeoutMs))
      .apiCallAttemptTimeout(Duration.ofMillis(s.apiCallAttemptTimeoutMs))
      .build()
    val request = ConverseRequest.builder()
      .modelId(s.modelId)
      .messages(message)
      .inferenceConfig(inference)
      .overrideConfiguration(perCall)
      .build()

    val sdkFuture = client.converse(request)
    sdkFuture.whenComplete { (response, error) =>
      if (error != null) {
        val failure = BedrockClient.asFailure(error)
        if (BedrockClient.isEndpointHealthFailure(failure.code)) breaker.recordFailure(generation)
        else breaker.recordNeutral(generation)
        result.completeExceptionally(failure)
      } else {
        val requestId = try response.responseMetadata().requestId() catch {
          case NonFatal(_) => null
        }
        try {
          val text = Option(response.output()).flatMap(o => Option(o.message()))
            .toSeq.flatMap(_.content().asScala)
            .flatMap(block => Option(block.text()))
            .mkString
          if (text.trim.isEmpty) {
            breaker.recordNeutral(generation)
            result.completeExceptionally(new BedrockException("EMPTY", requestId))
          } else {
            breaker.recordSuccess(generation)
            result.complete(ConverseResult(text, requestId))
          }
        } catch {
          case NonFatal(_) =>
            breaker.recordNeutral(generation)
            result.completeExceptionally(new BedrockException("SCHEMA", requestId))
        }
      }
    }
    // Propagate engine cancellation to the in-flight SDK call.
    result.whenComplete { (_, _) => if (result.isCancelled) sdkFuture.cancel(true) }
    result
  }
}

object BedrockClient {
  /** HTTP concurrency for the per-region client: the engine hard ceiling times a slot allowance. */
  private[bedrock] val MAX_HTTP_CONCURRENCY = 1024

  private val clients = new ConcurrentHashMap[String, BedrockClient]()
  private val breakers = new ConcurrentHashMap[String, CircuitBreaker]()

  private def endpointKey(s: BedrockSettings): String = s"${s.region}|${s.modelId}"

  /** One client per region; nothing a session can vary is part of the key. */
  def forSettings(s: BedrockSettings): BedrockClient =
    clients.computeIfAbsent(s.region, r => new BedrockClient(r))

  def clientCount: Int = clients.size()

  def resetSharedStateForTests(): Unit = breakers.clear()

  /** The breaker for an endpoint; the stricter cluster policy wins if two differ. */
  private[bedrock] def breakerFor(s: BedrockSettings): CircuitBreaker = {
    val b = breakers.computeIfAbsent(endpointKey(s), _ => new CircuitBreaker())
    b.tightenTo(s.breakerFailureThreshold, s.breakerResetMs)
    b
  }

  /** Generation-stamped half-open breaker; see the OpenAI module for the state description. */
  private[bedrock] final class CircuitBreaker {
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

    /** Generation is read before state so a trip between the reads leaves a stale generation. */
    def tryAcquire(): Long = {
      val gen = generation.get()
      state.get() match {
        case CLOSED => gen
        case OPEN =>
          if (System.nanoTime() >= openUntilNanos.get() && state.compareAndSet(OPEN, HALF_OPEN)) {
            generation.incrementAndGet()
          } else -1L
        case _ => -1L
      }
    }

    private def current(gen: Long): Boolean = gen == generation.get()

    def recordSuccess(gen: Long): Unit = synchronized {
      if (current(gen)) { consecutiveFailures.set(0); state.set(CLOSED) }
    }

    def recordNeutral(gen: Long): Unit = synchronized {
      if (current(gen) && state.get() == HALF_OPEN) {
        state.set(OPEN); openUntilNanos.set(System.nanoTime())
      }
    }

    def recordFailure(gen: Long): Unit = synchronized {
      if (!current(gen)) return
      if (state.get() == HALF_OPEN) trip()
      else if (consecutiveFailures.incrementAndGet() >= threshold.get()) trip()
    }

    private def trip(): Unit = {
      consecutiveFailures.set(0); generation.incrementAndGet()
      openUntilNanos.set(System.nanoTime() + resetNanos.get()); state.set(OPEN)
    }

    def isOpen: Boolean = state.get() != CLOSED
  }

  /**
   * Codes that must fail the task even in the tolerant forms. AUTH and CONFIG are not row-level.
   * BAD_MODEL (model id absent in this account/region) is fatal because every row would fail
   * identically and silently NULL out. CIRCUIT_OPEN is not fatal: `generate` fails on it anyway,
   * `generate_status` reports it, and `try_generate` returns NULL instead of causing a
   * task-failure storm for a short outage.
   */
  private val fatalCodes = Set("AUTH", "CONFIG", "BAD_MODEL")

  /** Codes that indicate endpoint ill-health and therefore count toward opening the breaker. */
  private val healthCodes = Set("TIMEOUT", "SERVER_ERROR", "RATE_LIMITED")

  def isFatal(code: String): Boolean = fatalCodes.contains(code)

  def isEndpointHealthFailure(code: String): Boolean = healthCodes.contains(code)

  private def unwrap(error: Throwable): Throwable = error match {
    case c: CompletionException if c.getCause != null => unwrap(c.getCause)
    case e => e
  }

  private def hasCause[T](t: Throwable, pred: Throwable => Boolean): Boolean = {
    var cur = t; var depth = 0
    while (cur != null && depth < 16) {
      if (pred(cur)) return true
      cur = cur.getCause; depth += 1
    }
    false
  }

  /**
   * Maps an SDK throwable to a stable, non-sensitive error code using only exception class names
   * (never message text).
   *
   *  - ValidationException is a per-request condition (for example, input exceeds the model
   *    context window) and is row-level BAD_REQUEST, not fatal.
   *  - ResourceNotFoundException means the model id does not exist here: fatal BAD_MODEL.
   *  - SdkClientException with a TLS cause is fatal CONFIG (a certificate problem is a security
   *    signal); with another IOException cause it is a transient transport failure, retryable
   *    TIMEOUT; otherwise (credentials, region) it is fatal CONFIG.
   */
  def classify(error: Throwable): String = {
    val root = unwrap(error)
    root.getClass.getSimpleName match {
      case "CancellationException" => "CANCELLED"
      case "ThrottlingException" | "ServiceQuotaExceededException" => "RATE_LIMITED"
      case "ModelTimeoutException" | "ApiCallTimeoutException" |
           "ApiCallAttemptTimeoutException" => "TIMEOUT"
      case "AccessDeniedException" | "UnauthorizedException" => "AUTH"
      case "ResourceNotFoundException" => "BAD_MODEL"
      case "ValidationException" | "ModelErrorException" => "BAD_REQUEST"
      case "ModelNotReadyException" | "ServiceUnavailableException" |
           "InternalServerException" => "SERVER_ERROR"
      case "SdkClientException" =>
        if (hasCause(root, _.isInstanceOf[javax.net.ssl.SSLException])) "CONFIG"
        else if (hasCause(root, _.isInstanceOf[IOException])) "TIMEOUT"
        else "CONFIG"
      case _ => "SERVER_ERROR"
    }
  }

  /** Extracts the AWS request id from an SDK service exception, if present. */
  def requestIdOf(error: Throwable): String = unwrap(error) match {
    case e: AwsServiceException => e.requestId()
    case _ => null
  }

  def asFailure(err: Throwable): BedrockException = unwrap(err) match {
    case e: BedrockException => e
    case other => new BedrockException(classify(other), requestIdOf(other))
  }

  def errorCode(err: Throwable): String = asFailure(err).code

  def sanitizedMessage(code: String): String = code match {
    case "TIMEOUT" => "the request timed out or the connection failed"
    case "RATE_LIMITED" => "Bedrock throttled the request"
    case "SERVER_ERROR" => "Bedrock returned a server error"
    case "BAD_REQUEST" => "the model rejected this request"
    case "BAD_MODEL" => "the model id does not exist in this account and region"
    case "AUTH" => "Bedrock rejected the credentials"
    case "CONFIG" => "the executor could not reach Bedrock with its credentials and region"
    case "SCHEMA" => "the response could not be parsed"
    case "EMPTY" => "Bedrock returned no text"
    case "CIRCUIT_OPEN" => "the Bedrock circuit is open"
    case "CANCELLED" => "the request was cancelled"
    case _ => "the request failed"
  }
}
