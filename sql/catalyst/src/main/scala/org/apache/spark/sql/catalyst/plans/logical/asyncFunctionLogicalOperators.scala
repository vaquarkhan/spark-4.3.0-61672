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

package org.apache.spark.sql.catalyst.plans.logical

import org.apache.spark.sql.catalyst.expressions.{ApplyAsyncFunctionExpression, Attribute, AttributeSet}
import org.apache.spark.sql.catalyst.trees.TreePattern.{EVAL_ASYNC_FUNCTIONS, TreePattern}

/**
 * SPARK-61672: a logical node that evaluates one or more [[ApplyAsyncFunctionExpression]] calls
 * over its child and appends the results as `resultAttrs`.
 *
 * It lives in catalyst (not sql/core) so the catalyst optimizer rules `LimitPushDown` and
 * `PushPredicateThroughNonJoin.canPushThrough` can match it, the same way they match
 * `BatchEvalPython` and `ArrowEvalPython`. The node is strictly one output row per input row, in
 * input order, so a `LocalLimit` may be pushed below it and a deterministic predicate on child
 * columns may move below it.
 */
case class EvalAsyncFunctions(
    functions: Seq[ApplyAsyncFunctionExpression],
    resultAttrs: Seq[Attribute],
    child: LogicalPlan)
  extends UnaryNode {

  override def output: Seq[Attribute] = child.output ++ resultAttrs
  override def producedAttributes: AttributeSet = AttributeSet(resultAttrs)
  override def maxRows: Option[Long] = child.maxRows
  override def maxRowsPerPartition: Option[Long] = child.maxRowsPerPartition

  final override val nodePatterns: Seq[TreePattern] = Seq(EVAL_ASYNC_FUNCTIONS)

  override protected def withNewChildInternal(newChild: LogicalPlan): EvalAsyncFunctions =
    copy(child = newChild)
}
