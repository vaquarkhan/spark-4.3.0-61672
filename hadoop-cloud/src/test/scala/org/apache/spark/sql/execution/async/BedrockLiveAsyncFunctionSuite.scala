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

import java.time.Duration
import java.util.Collections
import java.util.concurrent.{CompletableFuture, CompletionException}

import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal

import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.retries.DefaultRetryStrategy
import software.amazon.awssdk.services.bedrockruntime.BedrockRuntimeAsyncClient
import software.amazon.awssdk.services.bedrockruntime.model.{ContentBlock, ConversationRole, ConverseRequest, InferenceConfiguration, Message}

import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.DatasourceV2SQLBase
import org.apache.spark.sql.connector.catalog.{Identifier, InMemoryCatalog, SupportsNamespaces}
import org.apache.spark.sql.connector.catalog.functions.{AsyncScalarFunction, BoundFunction, UnboundFunction}
import org.apache.spark.sql.types.{DataType, StringType, StructType}
import org.apache.spark.unsafe.types.UTF8String

/**
 * Opt-in, paid integration test for the native async operator and Amazon Bedrock Converse.
 *
 * The suite is skipped unless SPARK_TEST_LIVE_BEDROCK=1. It uses the executor-visible AWS default
 * credential chain. No access key or session token is read on the driver, serialized in a closure,
 * placed in SparkConf, or logged. The test uses two fixed, non-sensitive prompts, one partition,
 * one in-flight call, one SDK attempt, speculation disabled, and an eight-token response cap.
 */
class BedrockLiveAsyncFunctionSuite extends DatasourceV2SQLBase {

  private val emptyProps: java.util.Map[String, String] = Collections.emptyMap[String, String]

  override protected def sparkConf: SparkConf = super.sparkConf
    .set("spark.speculation", "false")
    .set("spark.task.maxFailures", "1")
    .set("spark.sql.shuffle.partitions", "1")
    .set("spark.sql.asyncFunction.maxInFlightCeiling", "1")
    .set("spark.sql.asyncFunction.reorderWindow", "1")

  private def enabled: Boolean = sys.env.get("SPARK_TEST_LIVE_BEDROCK").contains("1")
  private def region: String = sys.env.getOrElse("SPARK_TEST_BEDROCK_REGION", "us-east-1")
  private def modelId: String = sys.env.getOrElse(
    "SPARK_TEST_BEDROCK_MODEL_ID", "amazon.nova-micro-v1:0")

  test("native async SQL calls live Bedrock Converse") {
    assume(enabled, "set SPARK_TEST_LIVE_BEDROCK=1 to run the paid Bedrock integration test")

    catalog("testcat").asInstanceOf[SupportsNamespaces]
      .createNamespace(Array("bedrock"), emptyProps)
    catalog("testcat").asInstanceOf[InMemoryCatalog].createFunction(
      Identifier.of(Array("bedrock"), "converse"),
      new BedrockConverseUnbound(region, modelId))

    spark.createDataFrame(Seq(
      (0, "Reply with exactly A"),
      (1, "Reply with exactly B"),
      (2, null.asInstanceOf[String])))
      .toDF("ordinal", "prompt")
      .repartition(1)
      .createOrReplaceTempView("bedrock_live_input")

    val df = sql("SELECT ordinal, testcat.bedrock.converse(prompt) AS answer " +
      "FROM bedrock_live_input ORDER BY ordinal")
    val answers = df.collect().map(_.getString(1)).toSeq
    val executed = df.queryExecution.executedPlan
    assert(executed.treeString.contains("AsyncEval"),
      "OPTIMIZED:\n" + df.queryExecution.optimizedPlan.treeString +
        "\nEXECUTED:\n" + executed.treeString)
    assert(answers.length == 3)
    assert(answers(0) != null && answers(0).trim.equalsIgnoreCase("A"))
    assert(answers(1) != null && answers(1).trim.equalsIgnoreCase("B"))
    assert(answers(2) == null)
  }
}

class BedrockConverseUnbound(region: String, modelId: String) extends UnboundFunction {
  override def name(): String = "bedrock_converse"

  override def bind(inputType: StructType): BoundFunction = {
    if (inputType.fields.length != 1 || inputType.fields(0).dataType != StringType) {
      throw new UnsupportedOperationException(
        "bedrock_converse expects exactly one string argument")
    }
    new BedrockConverseFunction(region, modelId)
  }

  override def description(): String = "bedrock_converse(string) -> string"
}

/** Executor-side, cancellation-propagating Bedrock Converse function for the live test. */
class BedrockConverseFunction(region: String, modelId: String)
  extends AsyncScalarFunction[UTF8String] {

  @transient @volatile private var clientRef: BedrockRuntimeAsyncClient = _

  override def name(): String = "bedrock_converse"
  override def inputTypes(): Array[DataType] = Array(StringType)
  override def resultType(): DataType = StringType
  override def isDeterministic(): Boolean = false
  override def maxInFlightPerTask(): Int = 1
  override def canonicalName(): String = s"spark61672.bedrock.converse.$region.$modelId"

  private def client: BedrockRuntimeAsyncClient = {
    var current = clientRef
    if (current == null) synchronized {
      current = clientRef
      if (current == null) {
        val overrideConfig = ClientOverrideConfiguration.builder()
          .apiCallAttemptTimeout(Duration.ofSeconds(20))
          .apiCallTimeout(Duration.ofSeconds(25))
          .retryStrategy(DefaultRetryStrategy.doNotRetry())
          .build()
        current = BedrockRuntimeAsyncClient.builder()
          .region(Region.of(region))
          .credentialsProvider(DefaultCredentialsProvider.create())
          .overrideConfiguration(overrideConfig)
          .build()
        clientRef = current
      }
    }
    current
  }

  override def produceResultAsync(input: InternalRow): CompletableFuture[UTF8String] = {
    if (input.isNullAt(0)) {
      return CompletableFuture.completedFuture(null.asInstanceOf[UTF8String])
    }

    val prompt = input.getUTF8String(0).toString
    val message = Message.builder()
      .role(ConversationRole.USER)
      .content(ContentBlock.builder().text(prompt).build())
      .build()
    val inference = InferenceConfiguration.builder()
      .maxTokens(8)
      .temperature(0.0f)
      .build()
    val request = ConverseRequest.builder()
      .modelId(modelId)
      .messages(message)
      .inferenceConfig(inference)
      .build()

    val sdkFuture = client.converse(request)
    val result = new CompletableFuture[UTF8String]()
    sdkFuture.whenComplete { (response, error) =>
      if (error != null) {
        val safe = error match {
          case c: CompletionException if c.getCause != null => c.getCause.getClass.getSimpleName
          case other => other.getClass.getSimpleName
        }
        result.completeExceptionally(
          new RuntimeException(s"Bedrock Converse failed: $safe"))
      } else {
        try {
          val output = Option(response.output()).flatMap(o => Option(o.message()))
            .toSeq.flatMap(_.content().asScala)
            .flatMap(block => Option(block.text()))
            .mkString
          if (output.trim.isEmpty) {
            result.completeExceptionally(
              new RuntimeException("Bedrock Converse returned no text"))
          } else {
            result.complete(UTF8String.fromString(output))
          }
        } catch {
          case NonFatal(_) => result.completeExceptionally(
            new RuntimeException("Bedrock Converse returned an invalid response"))
        }
      }
    }
    result.whenComplete { (_, _) =>
      if (result.isCancelled) sdkFuture.cancel(true)
    }
    result
  }

  override def close(): Unit = {
    val current = clientRef
    if (current != null) current.close()
  }
}
