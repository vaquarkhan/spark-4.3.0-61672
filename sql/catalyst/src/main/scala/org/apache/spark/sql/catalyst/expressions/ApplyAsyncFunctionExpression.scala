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

package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.sql.connector.catalog.functions.AsyncScalarFunction
import org.apache.spark.sql.catalyst.trees.TreePattern.{ASYNC_FUNCTION, TreePattern}
import org.apache.spark.sql.types.{AbstractDataType, DataType}
import org.apache.spark.util.ArrayImplicits._

/**
 * SPARK-61672: an unevaluable expression that wraps a bound [[AsyncScalarFunction]] call.
 *
 * It is modelled on [[PythonUDF]]: it is [[Unevaluable]] so neither interpreted eval nor codegen
 * can issue the remote call, and it must be extracted by `ExtractAsyncFunctions` into an
 * `EvalAsyncFunctions` logical node before physical planning.
 *
 * Determinism follows the bound function's semantic contract. A function that may return
 * different values for the same input must report `isDeterministic = false`; such calls receive a
 * stable per-expression identity and are never collapsed with another call. Deterministic calls
 * use the provider's canonical name and canonical children for common-subexpression reuse.
 * Because the expression is [[Unevaluable]], `foldable` is always false, so constant folding can
 * never trigger a remote call.
 */
case class ApplyAsyncFunctionExpression(
    function: AsyncScalarFunction[_],
    children: Seq[Expression],
    callId: ExprId = NamedExpression.newExprId)
  extends Expression
  with UserDefinedExpression
  with Unevaluable
  with ImplicitCastInputTypes {

  override def name: String = function.name()
  override def dataType: DataType = function.resultType()
  override def nullable: Boolean = function.isResultNullable
  override def inputTypes: Seq[AbstractDataType] =
    function.inputTypes().toImmutableArraySeq

  override lazy val deterministic: Boolean =
    function.isDeterministic && children.forall(_.deterministic)

  final override val nodePatterns: Seq[TreePattern] = Seq(ASYNC_FUNCTION)

  // Deterministic calls share a provider-defined canonical identity. Nondeterministic calls carry
  // a stable per-expression identity so optimizer copies of one call still refer to that call,
  // while two syntactically identical calls remain distinct.
  override lazy val canonicalized: Expression = {
    val identity = if (deterministic) {
      function.canonicalName()
    } else {
      s"nondeterministic:${callId.id}:${callId.jvmId}"
    }
    AsyncFunctionIdentity(identity, dataType, children.map(_.canonicalized))
  }

  override def toString: String = s"$name(${children.mkString(", ")})"
  override def sql: String = s"$name(${children.map(_.sql).mkString(", ")})"

  override protected def withNewChildrenInternal(
      newChildren: IndexedSeq[Expression]): ApplyAsyncFunctionExpression =
    copy(children = newChildren)
}

/**
 * Canonical placeholder for an [[ApplyAsyncFunctionExpression]] whose equality does not depend on
 * the bound function's object identity. It is never evaluated or planned.
 */
case class AsyncFunctionIdentity(
    funcCanonicalName: String,
    dataType: DataType,
    children: Seq[Expression])
  extends Expression with Unevaluable {
  override def nullable: Boolean = true
  override protected def withNewChildrenInternal(
      newChildren: IndexedSeq[Expression]): AsyncFunctionIdentity =
    copy(children = newChildren)
}
