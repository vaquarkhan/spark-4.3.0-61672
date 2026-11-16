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

import java.util.concurrent.CompletableFuture

import org.apache.spark.SparkEnv
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.{NoSuchFunctionException, NoSuchNamespaceException}
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.connector.catalog.{FunctionCatalog, Identifier}
import org.apache.spark.sql.connector.catalog.functions.{AsyncScalarFunction, BoundFunction, UnboundFunction}
import org.apache.spark.sql.types.{DataType, DataTypes, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.unsafe.types.UTF8String

/**
 * SPARK-61672 OpenAI-compatible provider module. This is a separate artifact from Spark core.
 * Register it with:
 * {{{
 *   spark.sql.catalog.ai = org.apache.spark.sql.ai.openai.OpenAIFunctionCatalog
 *   spark.sql.catalog.ai.baseUrl = https://approved.example.com/v1
 *   spark.sql.catalog.ai.chatModel = approved-model
 *   spark.sql.catalog.ai.credentialEnv = AI_PROVIDER_TOKEN
 *   # Cluster-wide ceilings, set at launch, not from a session:
 *   spark.ai.openai.allowedBaseUrls = https://approved.example.com/v1
 *   spark.ai.openai.allowedModels = approved-model
 *   spark.sql.asyncFunction.maxInFlightCeiling = 16
 * }}}
 *
 * The catalog options (`spark.sql.catalog.ai.*`) are convenient defaults. The security ceilings
 * (`spark.ai.openai.*`) come from the immutable `SparkEnv` configuration, never from a
 * session-mutable `SQLConf`, so a Spark Connect client cannot widen them. Any endpoint or model
 * requested through catalog options is rejected unless it is on the cluster allowlist.
 */

/** Error modes mapped onto the three function forms. */
sealed trait ErrorMode
object ErrorMode {
  case object Fail extends ErrorMode     // ai.generate: future fails, task fails
  case object Null extends ErrorMode     // ai.try_generate: sentinel NULL
  case object Status extends ErrorMode   // ai.generate_status: struct row
}

/**
 * Immutable, administrator-controlled settings, resolved once on the driver and shipped to
 * executors with the bound function. No secrets are carried here; only the name of the
 * environment variable that holds the token, and that name is chosen by the cluster
 * configuration, never by the session.
 */
final case class OpenAISettings(
    baseUrl: String,
    chatModel: String,
    maxInFlight: Int,
    maxAttempts: Int,
    maxElapsedMs: Long,
    requestTimeoutMs: Long,
    maxResponseBytes: Long,
    ratePermitsPerSec: Int,
    clusterRatePermitsPerSec: Int,
    breakerFailureThreshold: Int,
    breakerResetMs: Long,
    credentialEnv: String)

/**
 * Resolves and validates [[OpenAISettings]] from the catalog options and the immutable cluster
 * configuration.
 *
 * Trust model. Catalog options (`spark.sql.catalog.<name>.*`) are session-settable, so they may
 * only TIGHTEN a limit. Every limit has a cluster ceiling under `spark.ai.openai.*`, read from
 * the immutable `SparkEnv` configuration; a catalog option above the ceiling is clamped down to
 * it. When the administrator sets no ceiling for a limit, the module DEFAULT acts as the ceiling
 * (for example `maxElapsedMs` defaults to 60 s and a session cannot exceed 60 s unless the cluster
 * raises `spark.ai.openai.maxElapsedMs`). The credential environment variable name, the
 * circuit-breaker policy, and the allowlists are cluster-only and cannot be set from a catalog
 * option at all. A session-requested rate limit slows only that catalog's own calls.
 */
object OpenAIConf {
  val DEFAULT_MAX_IN_FLIGHT = 16
  val DEFAULT_MAX_ATTEMPTS = 4
  val DEFAULT_MAX_ELAPSED_MS = 60000L
  val DEFAULT_REQUEST_TIMEOUT_MS = 30000L
  val DEFAULT_MAX_RESPONSE_BYTES = 16L * 1024 * 1024   // 16 MiB decoded
  val DEFAULT_RATE_PERMITS_PER_SEC = 0                 // 0 means no cluster rate ceiling
  val DEFAULT_BREAKER_FAILURES = 10
  val DEFAULT_BREAKER_RESET_MS = 30000L
  val DEFAULT_CREDENTIAL_ENV = "AI_PROVIDER_TOKEN"
  val CEILING_KEY = "spark.sql.asyncFunction.maxInFlightCeiling"
  val HARD_CEILING = 1024

  /** A lookup into the immutable cluster configuration. */
  type ClusterConf = String => Option[String]

  /** The production lookup: the immutable SparkEnv configuration, or nothing outside a JVM. */
  val sparkEnvConf: ClusterConf = key => {
    val env = SparkEnv.get
    if (env == null) None else env.conf.getOption(key)
  }

  private def allowed(conf: ClusterConf, key: String): Set[String] =
    conf(key).getOrElse("").split(",").map(_.trim).filter(_.nonEmpty).toSet

  private def requireAllowed(kind: String, value: String, allowlist: Set[String]): Unit = {
    if (allowlist.isEmpty) {
      throw new IllegalArgumentException(
        s"spark.ai.openai.allowed$kind is empty; the administrator must allowlist at least one " +
          s"$kind before the ai catalog can be used")
    }
    if (!allowlist.contains(value)) {
      // Configuration-time, administrator-facing message. The rejected value is configuration,
      // not row data, and is included so the administrator can correct it.
      throw new IllegalArgumentException(
        s"$kind '$value' is not on the cluster allowlist spark.ai.openai.allowed$kind")
    }
  }

  /** Parses a numeric configuration value without echoing garbage into the error message. */
  private def num(source: String, key: String, raw: String): Long =
    try raw.trim.toLong catch {
      case _: NumberFormatException =>
        throw new IllegalArgumentException(s"$source.$key must be an integer")
    }

  private def clusterLong(conf: ClusterConf, key: String, default: Long): Long =
    conf(s"spark.ai.openai.$key").map(num("spark.ai.openai", key, _)).getOrElse(default)

  private def optLong(options: CaseInsensitiveStringMap, key: String, default: Long): Long =
    if (options.containsKey(key)) num("spark.sql.catalog.<name>", key, options.get(key))
    else default

  /** A limit the session may lower but never raise above the cluster ceiling. */
  private def tighten(options: CaseInsensitiveStringMap, key: String, ceiling: Long,
      floor: Long = 1L): Long =
    math.max(floor, math.min(optLong(options, key, ceiling), ceiling))

  private def required(options: CaseInsensitiveStringMap, key: String): String =
    Option(options.get(key)).map(_.trim).filter(_.nonEmpty)
      .getOrElse(throw new IllegalArgumentException(s"spark.sql.catalog.<name>.$key is required"))

  private def isLoopback(url: String): Boolean = {
    val host = try new java.net.URI(url).getHost catch { case _: Exception => null }
    host != null && (host == "127.0.0.1" || host == "localhost" || host == "[::1]" || host == "::1")
  }

  /** Resolves settings on the driver, clamping to the immutable cluster ceilings. */
  def resolve(options: CaseInsensitiveStringMap): OpenAISettings = resolve(options, sparkEnvConf)

  /** Same as [[resolve(CaseInsensitiveStringMap)]] with an explicit cluster lookup, for tests. */
  def resolve(options: CaseInsensitiveStringMap, conf: ClusterConf): OpenAISettings = {
    val baseUrl = required(options, "baseUrl").stripSuffix("/")
    val chatModel = required(options, "chatModel")

    if (options.containsKey("credentialEnv")) {
      throw new IllegalArgumentException(
        "credentialEnv is cluster-only; set spark.ai.openai.credentialEnv at launch, not as a " +
          "catalog option")
    }
    val credentialEnv = conf("spark.ai.openai.credentialEnv").map(_.trim).filter(_.nonEmpty)
      .getOrElse(DEFAULT_CREDENTIAL_ENV)

    val uri = try new java.net.URI(baseUrl) catch {
      case _: Exception => throw new IllegalArgumentException("baseUrl is not a valid URI")
    }
    if (uri.getUserInfo != null) {
      throw new IllegalArgumentException("baseUrl must not carry user info")
    }
    if (uri.getScheme != "https" && !(uri.getScheme == "http" && isLoopback(baseUrl))) {
      throw new IllegalArgumentException("baseUrl must use https (plain http only on loopback)")
    }
    requireAllowed("BaseUrls", baseUrl,
      allowed(conf, "spark.ai.openai.allowedBaseUrls").map(_.stripSuffix("/")))
    requireAllowed("Models", chatModel, allowed(conf, "spark.ai.openai.allowedModels"))

    // Concurrency: the engine-wide static ceiling bounds this module too.
    val inFlightCeiling = math.max(1L, math.min(HARD_CEILING.toLong,
      conf(CEILING_KEY).map(num("spark.sql.asyncFunction", "maxInFlightCeiling", _))
        .getOrElse(DEFAULT_MAX_IN_FLIGHT.toLong)))

    // Rate: a cluster ceiling of N permits/sec may be lowered by the session but not raised or
    // disabled. A cluster value of 0 means the administrator set no rate ceiling.
    val rateCeiling = clusterLong(conf, "ratePermitsPerSec", DEFAULT_RATE_PERMITS_PER_SEC)
    val requestedRate = optLong(options, "ratePermitsPerSec", rateCeiling)
    val rate =
      if (rateCeiling <= 0) math.max(0L, requestedRate)
      else if (requestedRate <= 0) rateCeiling
      else math.min(requestedRate, rateCeiling)

    OpenAISettings(
      baseUrl = baseUrl,
      chatModel = chatModel,
      maxInFlight = tighten(options, "maxInFlight", inFlightCeiling).toInt,
      maxAttempts = tighten(options, "maxAttempts",
        clusterLong(conf, "maxAttempts", DEFAULT_MAX_ATTEMPTS)).toInt,
      maxElapsedMs = tighten(options, "maxElapsedMs",
        clusterLong(conf, "maxElapsedMs", DEFAULT_MAX_ELAPSED_MS)),
      requestTimeoutMs = tighten(options, "requestTimeoutMs",
        clusterLong(conf, "requestTimeoutMs", DEFAULT_REQUEST_TIMEOUT_MS)),
      maxResponseBytes = tighten(options, "maxResponseBytes",
        clusterLong(conf, "maxResponseBytes", DEFAULT_MAX_RESPONSE_BYTES)),
      ratePermitsPerSec = math.min(rate, Int.MaxValue.toLong).toInt,
      clusterRatePermitsPerSec = math.min(math.max(0L, rateCeiling), Int.MaxValue.toLong).toInt,
      // Breaker policy is cluster-only so a session cannot weaken or dodge it.
      breakerFailureThreshold = math.max(1L,
        clusterLong(conf, "breakerFailureThreshold", DEFAULT_BREAKER_FAILURES)).toInt,
      breakerResetMs = math.max(1L, clusterLong(conf, "breakerResetMs", DEFAULT_BREAKER_RESET_MS)),
      credentialEnv = credentialEnv)
  }
}

/**
 * The provider catalog. Spark instantiates it with a no-arg constructor, then calls
 * [[initialize]] with the catalog name and the `spark.sql.catalog.<name>.*` options.
 */
class OpenAIFunctionCatalog extends FunctionCatalog {
  @volatile private var catalogName: String = _
  @volatile private var settings: OpenAISettings = _

  /** Production entry point: resolves against the immutable SparkEnv configuration. */
  override def initialize(name: String, options: CaseInsensitiveStringMap): Unit =
    initialize(name, options, OpenAIConf.sparkEnvConf)

  /** Test entry point with an explicit cluster lookup. */
  def initialize(name: String, options: CaseInsensitiveStringMap,
      conf: OpenAIConf.ClusterConf): Unit = {
    this.catalogName = name
    this.settings = OpenAIConf.resolve(options, conf)
  }

  override def name(): String = catalogName

  override def listFunctions(namespace: Array[String]): Array[Identifier] = {
    if (namespace.nonEmpty) {
      throw new NoSuchNamespaceException(namespace)
    }
    OpenAIFunctions.names.map(Identifier.of(Array.empty[String], _))
  }

  override def loadFunction(ident: Identifier): UnboundFunction = {
    if (ident.namespace().nonEmpty) {
      throw new NoSuchFunctionException(ident)
    }
    ident.name() match {
      case "generate" => new GenerateUnbound(settings, ErrorMode.Fail)
      case "try_generate" => new GenerateUnbound(settings, ErrorMode.Null)
      case "generate_status" => new GenerateUnbound(settings, ErrorMode.Status)
      case _ => throw new NoSuchFunctionException(ident)
    }
  }
}

/** Unbound generate function; validates argument types at bind time. */
class GenerateUnbound(settings: OpenAISettings, mode: ErrorMode) extends UnboundFunction {
  override def name(): String = OpenAIFunctions.formName(mode)
  // Model name only; the endpoint URL is deliberately not exposed through DESCRIBE FUNCTION.
  override def description(): String =
    s"${name()}(prompt STRING) -> STRING, OpenAI-compatible chat model ${settings.chatModel}"

  override def bind(inputType: StructType): BoundFunction = {
    if (inputType.length != 1 || inputType.head.dataType != DataTypes.StringType) {
      throw new UnsupportedOperationException(s"${name()} expects a single STRING argument")
    }
    new GenerateBound(settings, mode)
  }
}

/** Bound generate function implementing the async SPI. */
class GenerateBound(settings: OpenAISettings, mode: ErrorMode)
  extends AsyncScalarFunction[AnyRef] with Serializable {

  override def name(): String = OpenAIFunctions.formName(mode)

  override def inputTypes(): Array[DataType] = Array(DataTypes.StringType)

  override def resultType(): DataType = mode match {
    case ErrorMode.Status => OpenAIFunctions.statusType(DataTypes.StringType)
    case _ => DataTypes.StringType
  }

  override def isResultNullable(): Boolean = true

  // Generative output is not reproducible; see AsyncScalarFunction determinism contract.
  override def isDeterministic(): Boolean = false

  override def maxInFlightPerTask(): Int = settings.maxInFlight

  // Identity for deterministic-call reuse decisions only. It is a hash of the endpoint and model,
  // not the URL itself, so a plan string never carries the endpoint.
  override def canonicalName(): String = {
    val digest = java.security.MessageDigest.getInstance("SHA-256")
      .digest(s"${settings.baseUrl}|${settings.chatModel}".getBytes("UTF-8"))
    val hex = digest.take(8).map(b => f"${b & 0xff}%02x").mkString
    s"spark.ai.openai.${name()}|$hex"
  }

  // Executor-side shared client, keyed by connection identity, never serialized with the function.
  @transient private lazy val client: OpenAIClient = OpenAIClient.forSettings(settings)

  override def produceResultAsync(input: InternalRow): CompletableFuture[AnyRef] = {
    if (input.isNullAt(0)) {
      return CompletableFuture.completedFuture(OpenAIFunctions.nullInput(mode))
    }
    val prompt = input.getUTF8String(0).toString
    val source = client.chatCompletion(prompt, settings)
    val result = source.handle[AnyRef] { (res, err) =>
      if (err == null) OpenAIFunctions.success(mode, res)
      else OpenAIFunctions.failure(mode, err) // Fail: rethrow; Null: null; Status: struct
    }
    // Propagate engine cancellation to the in-flight HTTP call.
    result.whenComplete { (_, _) => if (result.isCancelled) source.cancel(true) }
    result
  }

  override def close(): Unit = { /* client is executor-lifetime; nothing per-task to close */ }
}

object OpenAIFunctions {
  val names: Array[String] = Array("generate", "try_generate", "generate_status")

  def formName(mode: ErrorMode): String = mode match {
    case ErrorMode.Fail => "generate"
    case ErrorMode.Null => "try_generate"
    case ErrorMode.Status => "generate_status"
  }

  def statusType(resultType: DataType): StructType =
    new StructType()
      .add("status", DataTypes.StringType)
      .add("result", resultType)
      .add("error_code", DataTypes.StringType)
      .add("error_message", DataTypes.StringType)
      .add("attempts", DataTypes.IntegerType)
      .add("request_id", DataTypes.StringType)

  private def utf8(s: String): UTF8String = if (s == null) null else UTF8String.fromString(s)

  def nullInput(mode: ErrorMode): AnyRef = mode match {
    case ErrorMode.Status =>
      val r = new GenericInternalRow(6)
      r.update(0, utf8("SKIPPED_NULL_INPUT"))
      r.update(4, Integer.valueOf(0))
      r
    case _ => null
  }

  def success(mode: ErrorMode, res: ChatResult): AnyRef = mode match {
    case ErrorMode.Status =>
      val r = new GenericInternalRow(6)
      r.update(0, utf8("SUCCESS"))
      r.update(1, utf8(res.text))
      r.update(4, Integer.valueOf(res.attempts))
      r.update(5, utf8(res.requestId))
      r
    case _ => utf8(res.text)
  }

  def failure(mode: ErrorMode, err: Throwable): AnyRef = {
    val failure = OpenAIClient.asFailure(err)
    mode match {
      case ErrorMode.Fail =>
        throw failure
      case ErrorMode.Null =>
        if (failure.fatal) throw failure else null
      case ErrorMode.Status =>
        if (failure.fatal) throw failure
        val r = new GenericInternalRow(6)
        r.update(0, utf8("ERROR"))
        r.update(2, utf8(failure.code))
        r.update(3, utf8(OpenAIClient.sanitizedMessage(failure.code)))
        r.update(4, Integer.valueOf(failure.attempts))
        r.update(5, utf8(failure.requestId))
        r
    }
  }
}

/** A successful completion with the metadata the status form reports. */
final case class ChatResult(text: String, attempts: Int, requestId: String)

/**
 * Sanitized exception that never carries a prompt, token, URL, or provider body. `fatal` marks
 * errors that must fail the task even in the tolerant forms: authentication, authorization, and
 * configuration problems are not row-level conditions.
 */
class OpenAIException(
    val code: String,
    val attempts: Int = 0,
    val requestId: String = null)
  extends RuntimeException(s"AI_OPENAI_$code: ${OpenAIClient.sanitizedMessage(code)}") {
  def fatal: Boolean = OpenAIClient.isFatal(code)
}
