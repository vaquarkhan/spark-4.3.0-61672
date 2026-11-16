-- SPARK-61672 example: OpenAI-compatible provider
--
-- Launch with the ai-openai provider jar and this configuration:
--
--   export AI_PROVIDER_TOKEN=...   # on each executor, not in SQL or config
--   spark-sql \
--     --jars spark-ai-openai_2.13-4.3.0.jar \
--     --conf spark.sql.catalog.ai=org.apache.spark.sql.ai.openai.OpenAIFunctionCatalog \
--     --conf spark.sql.catalog.ai.baseUrl=https://approved.example.com/v1 \
--     --conf spark.sql.catalog.ai.chatModel=approved-model \
--     --conf spark.ai.openai.allowedBaseUrls=https://approved.example.com/v1 \
--     --conf spark.ai.openai.allowedModels=approved-model \
--     --conf spark.ai.openai.credentialEnv=AI_PROVIDER_TOKEN \
--     --conf spark.ai.openai.maxResponseBytes=16777216 \
--     --conf spark.ai.openai.ratePermitsPerSec=50 \
--     --conf spark.sql.asyncFunction.maxInFlightCeiling=16 \
--     -f 02-openai.sql
--
-- The endpoint and model are rejected unless they are on the cluster allowlist. The token is read
-- from the executor environment variable named by the CLUSTER setting spark.ai.openai.credentialEnv;
-- a catalog option named credentialEnv is rejected so a session cannot choose which secret is sent.
-- Every spark.ai.openai.* numeric value is a ceiling a session may lower but not raise.

CREATE OR REPLACE TEMP VIEW reviews AS
SELECT * FROM VALUES
  (101, 'Battery lasted all day and setup was simple.', 5),
  (102, 'Package arrived late and the box was dented.', 2),
  (103, 'Clear instructions and fast performance.', 5),
  (104, CAST(NULL AS STRING), 5)
AS reviews(id, review, stars);

-- The SQL is identical to the Bedrock example. Only the launch configuration changed.
SELECT id, ai.generate(review) AS summary
FROM reviews
WHERE stars = 5
LIMIT 3;

SELECT id, ai.try_generate(review) AS summary
FROM reviews;
