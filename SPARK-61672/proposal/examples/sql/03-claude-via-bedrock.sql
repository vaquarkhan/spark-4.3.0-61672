-- SPARK-61672 example: Anthropic Claude through Amazon Bedrock
--
-- This reuses the same ai-bedrock provider jar. Only the administrator-selected model changes.
-- The business SQL does not change at all.
--
--   spark-sql \
--     --jars spark-ai-bedrock_2.13-4.3.0.jar \
--     --conf spark.sql.catalog.ai=org.apache.spark.sql.ai.bedrock.BedrockFunctionCatalog \
--     --conf spark.sql.catalog.ai.region=us-east-1 \
--     --conf spark.sql.catalog.ai.modelId=anthropic.claude-sonnet-4-5 \
--     --conf spark.ai.bedrock.allowedModels=anthropic.claude-sonnet-4-5,amazon.nova-micro-v1:0 \
--     --conf spark.ai.bedrock.allowedRegions=us-east-1 \
--     --conf spark.sql.asyncFunction.maxInFlightCeiling=16 \
--     -f 03-claude-via-bedrock.sql

CREATE OR REPLACE TEMP VIEW reviews AS
SELECT * FROM VALUES
  (101, 'Battery lasted all day and setup was simple.', 5),
  (103, 'Clear instructions and fast performance.', 5),
  (106, 'Exceeded my expectations for the price.', 5)
AS reviews(id, review, stars);

-- Same function, same SQL. The provider module translates it to Claude's Converse request.
SELECT id, ai.generate(review) AS summary
FROM reviews
WHERE stars = 5;
