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

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}

import org.apache.spark.sql.SparkSession

/**
 * SPARK-61672 end-to-end proof in ONE process, for BOTH provider modules: real Spark SQL, a
 * packaged provider jar loaded purely through configuration (no test catalog, no test function),
 * the native async planner path, and a real model service.
 *
 * Catalog `bedrock` is the Bedrock module (AWS SDK v2 Converse, default credential chain).
 * Catalog `oai` is the OpenAI-compatible module pointed at Amazon Bedrock's OpenAI-compatible
 * Chat Completions API with a bearer token, so it too is proven against a real service.
 *
 * This is the same shape as a `spark-sql --jars ... --conf ...` launch. The program prints the
 * plans and rows, including the service-minted request ids, and exits non-zero on any failure.
 *
 * Paid and opt-in. Requires AWS credentials with bedrock:InvokeModel and, for the `oai` catalog,
 * SPARK_TEST_LIVE_OPENAI_BASE_URL plus AI_PROVIDER_TOKEN in the environment.
 */
object LiveSparkSqlProof {
  def main(args: Array[String]): Unit = {
    val region = sys.env.getOrElse("SPARK_TEST_BEDROCK_REGION", "us-east-1")
    val bedrockModel = sys.env.getOrElse("SPARK_TEST_BEDROCK_MODEL_ID", "amazon.nova-micro-v1:0")
    val oaiBase = sys.env.get("SPARK_TEST_LIVE_OPENAI_BASE_URL").map(_.trim).filter(_.nonEmpty)
    val oaiModel = sys.env.getOrElse("SPARK_TEST_LIVE_OPENAI_MODEL", "qwen.qwen3-32b-v1:0")
    val haveOai = oaiBase.isDefined && sys.env.contains("AI_PROVIDER_TOKEN")

    var b = SparkSession.builder()
      .master("local[2]")
      .appName("SPARK-61672 live spark-sql proof")
      // --- Bedrock module, exactly what an administrator would pass to spark-sql --conf -------
      .config("spark.sql.catalog.bedrock", "org.apache.spark.sql.ai.bedrock.BedrockFunctionCatalog")
      .config("spark.sql.catalog.bedrock.region", region)
      .config("spark.sql.catalog.bedrock.modelId", bedrockModel)
      .config("spark.sql.catalog.bedrock.maxTokens", "8")
      .config("spark.ai.bedrock.allowedModels", bedrockModel)
      .config("spark.ai.bedrock.allowedRegions", region)
      .config("spark.ai.bedrock.maxTokens", "64")
      .config("spark.sql.asyncFunction.maxInFlightCeiling", "2")
      .config("spark.speculation", "false")
      .config("spark.task.maxFailures", "1")
      .config("spark.sql.shuffle.partitions", "1")
      .config("spark.ui.enabled", "false")
    if (haveOai) {
      // --- OpenAI-compatible module against a real endpoint -----------------------------------
      b = b.config("spark.sql.catalog.oai", "org.apache.spark.sql.ai.openai.OpenAIFunctionCatalog")
        .config("spark.sql.catalog.oai.baseUrl", oaiBase.get)
        .config("spark.sql.catalog.oai.chatModel", oaiModel)
        .config("spark.sql.catalog.oai.requestTimeoutMs", "30000")
        .config("spark.ai.openai.allowedBaseUrls", oaiBase.get)
        .config("spark.ai.openai.allowedModels", oaiModel)
        .config("spark.ai.openai.credentialEnv", "AI_PROVIDER_TOKEN")
    }
    val spark = b.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    var failures = 0
    def check(cond: Boolean, msg: String): Unit = {
      println((if (cond) "PASS  " else "FAIL  ") + msg)
      if (!cond) failures += 1
    }

    try {
      spark.sql(
        """CREATE OR REPLACE TEMP VIEW prompts AS
          |SELECT * FROM VALUES
          |  (0, 'Reply with exactly A'),
          |  (1, 'Reply with exactly B'),
          |  (2, CAST(NULL AS STRING))
          |AS prompts(ordinal, prompt)""".stripMargin)

      def proveCatalog(cat: String, label: String): Unit = {
        println(s"\n##### $label (catalog `$cat`) #####")
        val df = spark.sql(
          s"SELECT ordinal, $cat.generate(prompt) AS answer FROM prompts ORDER BY ordinal")
        val optimized = df.queryExecution.optimizedPlan.treeString
        val executed = df.queryExecution.executedPlan.treeString
        println("=== optimized plan ===\n" + optimized)
        println("=== executed plan ===\n" + executed)
        check(optimized.contains("EvalAsyncFunctions"), s"$cat: optimized plan contains EvalAsyncFunctions")
        check(executed.contains("AsyncEval"), s"$cat: executed plan contains AsyncEval")

        val rows = df.collect()
        println("=== rows ===")
        rows.foreach(r => println(s"  ordinal=${r.getInt(0)}  answer=${r.getString(1)}"))
        check(rows.length == 3, s"$cat: three rows returned")
        check(rows(0).getString(1) != null && rows(0).getString(1).trim.toUpperCase.contains("A"),
          s"$cat: row 0 answer contains A from the model")
        check(rows(1).getString(1) != null && rows(1).getString(1).trim.toUpperCase.contains("B"),
          s"$cat: row 1 answer contains B from the model")
        check(rows(2).isNullAt(1), s"$cat: NULL prompt stays NULL (no call)")

        val st = spark.sql(
          s"SELECT $cat.generate_status(prompt) AS s FROM prompts WHERE ordinal = 0").collect()
        val s = st(0).getStruct(0)
        val status = s.getString(0)
        val requestId = if (s.isNullAt(5)) null else s.getString(5)
        println(s"=== generate_status ===  status=$status  result=${s.getString(1)}  request_id=$requestId")
        check(status == "SUCCESS", s"$cat: generate_status reports SUCCESS")
        check(requestId != null && requestId.nonEmpty, s"$cat: service request id present")

        val limited = spark.sql(
          s"SELECT $cat.generate(prompt) AS a FROM prompts WHERE ordinal = 1 LIMIT 1")
        val lplan = limited.queryExecution.optimizedPlan.treeString
        println("=== pushdown plan ===\n" + lplan)
        val filterBelow = lplan.indexOf("EvalAsyncFunctions") < lplan.lastIndexOf("LocalRelation") &&
          !lplan.substring(0, lplan.indexOf("EvalAsyncFunctions")).contains("Filter")
        check(filterBelow, s"$cat: filter and LIMIT are placed below the async node")
        val lrows = limited.collect()
        check(lrows.length == 1 && lrows(0).getString(0).trim.toUpperCase.contains("B"),
          s"$cat: filtered + limited query returns only B")
      }

      proveCatalog("bedrock", "Bedrock module via AWS SDK v2 Converse")
      if (haveOai) proveCatalog("oai", "OpenAI-compatible module via Bedrock's OpenAI-compatible API")
      else println("\n(OpenAI-compatible catalog skipped: set SPARK_TEST_LIVE_OPENAI_BASE_URL and AI_PROVIDER_TOKEN)")

      // --- Real example over the dummy reviews data: real summaries from a real model -------
      val dataPath = sys.env.getOrElse("SPARK_61672_REVIEWS_CSV", "")
      if (dataPath.nonEmpty && Files.exists(Paths.get(dataPath))) {
        println("\n##### Real summaries over examples/data/reviews.csv (Bedrock) #####")
        spark.read.option("header", "true").option("inferSchema", "true").csv(dataPath)
          .createOrReplaceTempView("reviews")
        val summarized = spark.sql(
          """SELECT id, stars, review,
            |       bedrock.try_generate(CONCAT('Summarize in at most six words: ', review)) AS summary
            |FROM reviews
            |WHERE stars >= 3
            |ORDER BY id""".stripMargin)
        val out = summarized.collect()
        val sb = new StringBuilder
        sb.append("id,stars,review,summary\n")
        out.foreach { r =>
          val id = r.getInt(0); val stars = r.getInt(1)
          val review = if (r.isNullAt(2)) "" else r.getString(2)
          val summary = if (r.isNullAt(3)) "NULL" else r.getString(3).replace("\n", " ").trim
          println(f"  $id%-4d $stars%d  ${summary}%-45s | $review")
          sb.append(s"$id,$stars,${q(review)},${q(summary)}\n")
        }
        val nonNull = out.count(r => !r.isNullAt(3))
        val nullReviews = out.count(r => r.isNullAt(2))
        check(out.nonEmpty, "reviews query returned rows")
        check(out.filter(r => r.isNullAt(2)).forall(r => r.isNullAt(3)), "NULL reviews produced NULL summaries without a call")
        check(nonNull == out.length - nullReviews, "every non-NULL review received a real model summary")
        val reportPath = sys.env.getOrElse("SPARK_61672_REVIEWS_OUT", "")
        if (reportPath.nonEmpty) {
          Files.write(Paths.get(reportPath), sb.toString.getBytes(StandardCharsets.UTF_8))
          println(s"wrote $reportPath")
        }
      }
    } finally {
      spark.stop()
    }

    println(if (failures == 0) "RESULT: ALL CHECKS PASSED" else s"RESULT: $failures FAILURE(S)")
    System.exit(if (failures == 0) 0 else 1)
  }

  private def q(s: String): String = "\"" + s.replace("\"", "\"\"") + "\""
}
