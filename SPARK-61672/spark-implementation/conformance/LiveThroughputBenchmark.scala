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

import org.apache.spark.sql.SparkSession

/**
 * SPARK-61672 real throughput benchmark: the same Spark SQL query over the same prompts against
 * REAL Amazon Bedrock, once with the async operator limited to one call in flight (equivalent to a
 * serial UDF) and once with eight in flight. Everything else is identical: same model, same
 * prompts, same partition count, same JVM. This measures what the proposal actually claims, that
 * overlapping remote calls inside a task cuts wall-clock time, on a real service with real latency
 * variance. It is not a mechanism microbenchmark and it does not emulate anything.
 *
 * `spark.sql.asyncFunction.maxInFlightCeiling` is a static configuration, so each setting gets its
 * own SparkContext. One warm-up query per context is excluded from timing.
 *
 * Paid and opt-in: N prompts x 2 runs (+2 warm-ups) short Nova Micro calls. Default N = 24.
 */
object LiveThroughputBenchmark {
  def main(args: Array[String]): Unit = {
    val region = sys.env.getOrElse("SPARK_TEST_BEDROCK_REGION", "us-east-1")
    val model = sys.env.getOrElse("SPARK_TEST_BEDROCK_MODEL_ID", "amazon.nova-micro-v1:0")
    val n = sys.env.get("SPARK_61672_BENCH_ROWS").map(_.toInt).getOrElse(24)

    def run(inFlight: Int): (Long, Int, Long) = {
      val spark = SparkSession.builder()
        .master("local[1]")                       // one task: isolates in-task overlap
        .appName(s"SPARK-61672 live throughput inFlight=$inFlight")
        .config("spark.sql.catalog.bedrock", "org.apache.spark.sql.ai.bedrock.BedrockFunctionCatalog")
        .config("spark.sql.catalog.bedrock.region", region)
        .config("spark.sql.catalog.bedrock.modelId", model)
        .config("spark.sql.catalog.bedrock.maxTokens", "8")
        .config("spark.sql.catalog.bedrock.maxInFlight", inFlight.toString)
        .config("spark.ai.bedrock.allowedModels", model)
        .config("spark.ai.bedrock.allowedRegions", region)
        .config("spark.sql.asyncFunction.maxInFlightCeiling", inFlight.toString)
        .config("spark.sql.asyncFunction.reorderWindow", (inFlight * 4).toString)
        .config("spark.speculation", "false")
        .config("spark.task.maxFailures", "1")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
      spark.sparkContext.setLogLevel("ERROR")
      try {
        spark.range(0, n).selectExpr("id", "CONCAT('Reply with exactly the number ', CAST(id AS STRING)) AS prompt")
          .coalesce(1).createOrReplaceTempView("p")
        // warm-up: SDK client, credential chain, connection; excluded from timing
        spark.sql("SELECT bedrock.generate(prompt) FROM p LIMIT 1").collect()
        val t0 = System.nanoTime()
        val rows = spark.sql("SELECT id, bedrock.generate(prompt) AS a FROM p").collect()
        val ms = (System.nanoTime() - t0) / 1000000
        val correct = rows.count(r => r.getString(1) != null && r.getString(1).trim.contains(r.getLong(0).toString))
        val ordered = rows.map(_.getLong(0)).toSeq == (0L until n).toSeq
        (ms, correct, if (ordered) 1L else 0L)
      } finally spark.stop()
    }

    println(s"SPARK-61672 live throughput benchmark: $n prompts, model $model, region $region")
    println("=" * 78)
    val (serialMs, serialOk, serialOrdered) = run(1)
    println(f"inFlight=1  (serial)   wall=${serialMs}%6d ms   rows/s=${n * 1000.0 / serialMs}%6.2f   correct=$serialOk/$n   ordered=${serialOrdered == 1}")
    val (asyncMs, asyncOk, asyncOrdered) = run(8)
    println(f"inFlight=8  (async)    wall=${asyncMs}%6d ms   rows/s=${n * 1000.0 / asyncMs}%6.2f   correct=$asyncOk/$n   ordered=${asyncOrdered == 1}")
    println("=" * 78)
    val speedup = serialMs.toDouble / asyncMs
    println(f"speedup (serial / async) = ${speedup}%.2fx")
    val ok = serialOk == n && asyncOk == n && serialOrdered == 1 && asyncOrdered == 1 && speedup > 1.0
    println(if (ok) "RESULT: ALL CHECKS PASSED" else "RESULT: CHECK FAILED")
    System.exit(if (ok) 0 else 1)
  }
}
