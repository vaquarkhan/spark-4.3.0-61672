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
 * SPARK-61672 Amazon Bedrock provider module. This is a separate artifact from Spark core and from
 * the OpenAI module; it brings the AWS SDK v2 Bedrock runtime client with it. Register it with:
 * {{{
 *   spark.sql.catalog.ai = org.apache.spark.sql.ai.bedrock.BedrockFunctionCatalog
 *   spark.sql.catalog.ai.region = us-east-1
 *   spark.sql.catalog.ai.modelId = amazon.nova-micro-v1:0
 *   # Cluster-wide ceilings, set at launch, not from a session:
 *   spark.ai.bedrock.allowedModels = amazon.nova-micro-v1:0,anthropic.claude-sonnet-4-5
 *   spark.ai.bedrock.allowedRegions = us-east-1
 *   spark.sql.asyncFunction.maxInFlightCeiling = 16
 * }}}
 *
 * Credentials are never read on the driver or placed in configuration. The client uses the AWS
 * default credential chain on each executor (EC2 role, container identity, Kubernetes identity, or
 * an approved local profile). Model and region requested through catalog options are rejected
 * unless they are on the immutable cluster allowlist.
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
 * executors with the bound function. No secrets are carried.
 */
final case class BedrockSettings(
    region: String,
    modelId: String,
    maxTokens: Int,
    temperature: Float,
    maxInFlight: Int,
    apiCallTimeoutMs: Long,
    apiCallAttemptTimeoutMs: Long,
    breakerFailureThreshold: Int,
    breakerResetMs: Long)

/**
 * Resolves and validates [[BedrockSettings]] from the catalog options and the immutable cluster
 * configuration.
 *
 * Trust model. Catalog options are session-settable, so they may only TIGHTEN a limit. Every
 * limit has a cluster ceiling under `spark.ai.bedrock.*`; a catalog option above the ceiling is
 * clamped down. The circuit-breaker policy and the allowlists are cluster-only.
 */
object BedrockConf {
  val DEFAULT_MAX_TOKENS = 512
  val DEFAULT_TEMPERATURE = 0.0f
  val DEFAULT_MAX_IN_FLIGHT = 16
  val DEFAULT_API_CALL_TIMEOUT_MS = 25000L
  val DEFAULT_API_CALL_ATTEMPT_TIMEOUT_MS = 20000L
  val DEFAULT_BREAKER_FAILURES = 10
  val DEFAULT_BREAKER_RESET_MS = 30000L
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
        s"spark.ai.bedrock.allowed$kind is empty; the administrator must allowlist at least one " +
          s"$kind before the ai catalog can be used")
    }
    if (!allowlist.contains(value)) {
      // Configuration-time, administrator-facing message; the value is configuration, not data.
      throw new IllegalArgumentException(
        s"$kind '$value' is not on the cluster allowlist spark.ai.bedrock.allowed$kind")
    }
  }

  private def num(source: String, key: String, raw: String): Long =
    try raw.trim.toLong catch {
      case _: NumberFormatException =>
        throw new IllegalArgumentException(s"$source.$key must be an integer")
    }

  private def clusterLong(conf: ClusterConf, key: String, default: Long): Long =
    conf(s"spark.ai.bedrock.$key").map(num("spark.ai.bedrock", key, _)).getOrElse(default)

  private def optLong(options: CaseInsensitiveStringMap, key: String, default: Long): Long =
    if (options.containsKey(key)) num("spark.sql.catalog.<name>", key, options.get(key))
    else default

  /** A limit the session may lower but never raise above the cluster ceiling. */
  private def tighten(options: CaseInsensitiveStringMap, key: String, ceiling: Long): Long =
    math.max(1L, math.min(optLong(options, key, ceiling), ceiling))

  private def optFloat(options: CaseInsensitiveStringMap, key: String, default: Float): Float =
    if (options.containsKey(key)) {
      try options.get(key).trim.toFloat catch {
        case _: NumberFormatException =>
          throw new IllegalArgumentException(s"spark.sql.catalog.<name>.$key must be a number")
      }
    } else default

  private def required(options: CaseInsensitiveStringMap, key: String): String =
    Option(options.get(key)).map(_.trim).filter(_.nonEmpty)
      .getOrElse(throw new IllegalArgumentException(s"spark.sql.catalog.<name>.$key is required"))

  def resolve(options: CaseInsensitiveStringMap): BedrockSettings = resolve(options, sparkEnvConf)

  def resolve(options: CaseInsensitiveStringMap, conf: ClusterConf): BedrockSettings = {
    val region = required(options, "region")
    val modelId = required(options, "modelId")

    requireAllowed("Regions", region, allowed(conf, "spark.ai.bedrock.allowedRegions"))
    requireAllowed("Models", modelId, allowed(conf, "spark.ai.bedrock.allowedModels"))

    val inFlightCeiling = math.max(1L, math.min(HARD_CEILING.toLong,
      conf(CEILING_KEY).map(num("spark.sql.asyncFunction", "maxInFlightCeiling", _))
        .getOrElse(DEFAULT_MAX_IN_FLIGHT.toLong)))

    BedrockSettings(
      region = region,
      modelId = modelId,
      // maxTokens is a direct cost control: cluster ceiling, session may only lower it.
      maxTokens = tighten(options, "maxTokens",
        clusterLong(conf, "maxTokens", DEFAULT_MAX_TOKENS)).toInt,
      temperature = math.min(1.0f, math.max(0.0f,
        optFloat(options, "temperature", DEFAULT_TEMPERATURE))),
      maxInFlight = tighten(options, "maxInFlight", inFlightCeiling).toInt,
      apiCallTimeoutMs = tighten(options, "apiCallTimeoutMs",
        clusterLong(conf, "apiCallTimeoutMs", DEFAULT_API_CALL_TIMEOUT_MS)),
      apiCallAttemptTimeoutMs = tighten(options, "apiCallAttemptTimeoutMs",
        clusterLong(conf, "apiCallAttemptTimeoutMs", DEFAULT_API_CALL_ATTEMPT_TIMEOUT_MS)),
      // Breaker policy is cluster-only.
      breakerFailureThreshold = math.max(1L,
        clusterLong(conf, "breakerFailureThreshold", DEFAULT_BREAKER_FAILURES)).toInt,
      breakerResetMs = math.max(1L, clusterLong(conf, "breakerResetMs", DEFAULT_BREAKER_RESET_MS)))
  }
}

/** The provider catalog. Spark instantiates it with a no-arg constructor, then initializes it. */
class BedrockFunctionCatalog extends FunctionCatalog {
  @volatile private var catalogName: String = _
  @volatile private var settings: BedrockSettings = _

  override def initialize(name: String, options: CaseInsensitiveStringMap): Unit =
    initialize(name, options, BedrockConf.sparkEnvConf)

  /** Test entry point with an explicit cluster lookup. */
  def initialize(name: String, options: CaseInsensitiveStringMap,
      conf: BedrockConf.ClusterConf): Unit = {
    this.catalogName = name
    this.settings = BedrockConf.resolve(options, conf)
  }

  override def name(): String = catalogName

  override def listFunctions(namespace: Array[String]): Array[Identifier] = {
    if (namespace.nonEmpty) {
      throw new NoSuchNamespaceException(namespace)
    }
    BedrockFunctions.names.map(Identifier.of(Array.empty[String], _))
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
class GenerateUnbound(settings: BedrockSettings, mode: ErrorMode) extends UnboundFunction {
  override def name(): String = BedrockFunctions.formName(mode)
  override def description(): String =
    s"${name()}(prompt STRING) via Bedrock ${settings.modelId} in ${settings.region}"

  override def bind(inputType: StructType): BoundFunction = {
    if (inputType.length != 1 || inputType.head.dataType != DataTypes.StringType) {
      throw new UnsupportedOperationException(s"${name()} expects a single STRING argument")
    }
    new GenerateBound(settings, mode)
  }
}

/** Bound generate function implementing the async SPI. */
class GenerateBound(settings: BedrockSettings, mode: ErrorMode)
  extends AsyncScalarFunction[AnyRef] with Serializable {

  override def name(): String = BedrockFunctions.formName(mode)

  override def inputTypes(): Array[DataType] = Array(DataTypes.StringType)

  override def resultType(): DataType = mode match {
    case ErrorMode.Status => BedrockFunctions.statusType(DataTypes.StringType)
    case _ => DataTypes.StringType
  }

  override def isResultNullable(): Boolean = true

  // Generative output is not reproducible; see AsyncScalarFunction determinism contract.
  override def isDeterministic(): Boolean = false

  override def maxInFlightPerTask(): Int = settings.maxInFlight

  // Identity for deterministic-call reuse only; region and model are public identifiers.
  override def canonicalName(): String =
    s"spark.ai.bedrock.${name()}|${settings.region}|${settings.modelId}"

  @transient private lazy val client: BedrockClient = BedrockClient.forSettings(settings)

  override def produceResultAsync(input: InternalRow): CompletableFuture[AnyRef] = {
    if (input.isNullAt(0)) {
      return CompletableFuture.completedFuture(BedrockFunctions.nullInput(mode))
    }
    val prompt = input.getUTF8String(0).toString
    val source = client.converse(prompt, settings)
    val result = source.handle[AnyRef] { (res, err) =>
      if (err == null) BedrockFunctions.success(mode, res)
      else BedrockFunctions.failure(mode, err)
    }
    result.whenComplete { (_, _) => if (result.isCancelled) source.cancel(true) }
    result
  }

  override def close(): Unit = { /* client is executor-lifetime; closed on JVM shutdown */ }
}

object BedrockFunctions {
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

  // `attempts` is NULL for Bedrock: the AWS SDK retries internally and does not expose the count
  // on the response, and a number known to be wrong is worse than no number.
  def success(mode: ErrorMode, res: ConverseResult): AnyRef = mode match {
    case ErrorMode.Status =>
      val r = new GenericInternalRow(6)
      r.update(0, utf8("SUCCESS"))
      r.update(1, utf8(res.text))
      r.update(5, utf8(res.requestId))
      r
    case _ => utf8(res.text)
  }

  def failure(mode: ErrorMode, err: Throwable): AnyRef = {
    val failure = BedrockClient.asFailure(err)
    mode match {
      case ErrorMode.Fail => throw failure
      case ErrorMode.Null => if (failure.fatal) throw failure else null
      case ErrorMode.Status =>
        if (failure.fatal) throw failure
        val r = new GenericInternalRow(6)
        r.update(0, utf8("ERROR"))
        r.update(2, utf8(failure.code))
        r.update(3, utf8(BedrockClient.sanitizedMessage(failure.code)))
        r.update(5, utf8(failure.requestId))
        r
    }
  }
}

/** A successful Converse call with the metadata the status form reports. */
final case class ConverseResult(text: String, requestId: String)

/**
 * Sanitized exception that never carries a prompt or provider body. `fatal` marks errors that must
 * fail the task even in the tolerant forms: authentication, authorization, and configuration
 * problems (including a bad model id) are not row-level conditions.
 */
class BedrockException(val code: String, val requestId: String = null)
  extends RuntimeException(s"AI_BEDROCK_$code: ${BedrockClient.sanitizedMessage(code)}") {
  def fatal: Boolean = BedrockClient.isFatal(code)
}
