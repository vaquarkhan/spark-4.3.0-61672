-- SPARK-61672 example: Amazon Bedrock provider
--
-- Launch spark-sql (or spark-submit) with the ai-bedrock provider jar and this configuration.
-- The administrator sets everything below at launch; a query author cannot widen these.
--
--   spark-sql \
--     --jars spark-ai-bedrock_2.13-4.3.0.jar \
--     --conf spark.sql.catalog.ai=org.apache.spark.sql.ai.bedrock.BedrockFunctionCatalog \
--     --conf spark.sql.catalog.ai.region=us-east-1 \
--     --conf spark.sql.catalog.ai.modelId=amazon.nova-micro-v1:0 \
--     --conf spark.ai.bedrock.allowedModels=amazon.nova-micro-v1:0 \
--     --conf spark.ai.bedrock.allowedRegions=us-east-1 \
--     --conf spark.sql.asyncFunction.maxInFlightCeiling=16 \
--     -f 01-bedrock.sql
--
-- Credentials come from the AWS default chain on each executor (EC2 role, container identity,
-- Kubernetes identity, or an approved local profile). No access key is placed in SQL or config.

CREATE OR REPLACE TEMP VIEW reviews AS
SELECT * FROM VALUES
  (101, 'Battery lasted all day and setup was simple.', 5),
  (102, 'Package arrived late and the box was dented.', 2),
  (103, 'Clear instructions and fast performance.', 5),
  (104, CAST(NULL AS STRING), 5),
  (105, 'The screen is bright but the speaker is weak.', 3)
AS reviews(id, review, stars);

-- Strict form: a terminal provider error fails the query.
SELECT id, ai.generate(review) AS summary
FROM reviews
WHERE stars = 5
LIMIT 3;

-- Tolerant form: a row-level provider error returns NULL instead of failing the query.
SELECT id, ai.try_generate(review) AS summary
FROM reviews
WHERE stars >= 3;

-- Observable form: returns a struct with status, result, error code, and request id.
SELECT id, ai.generate_status(review) AS outcome
FROM reviews
WHERE stars = 5;
