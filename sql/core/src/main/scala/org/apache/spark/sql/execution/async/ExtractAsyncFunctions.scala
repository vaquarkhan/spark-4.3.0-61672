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

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.SparkException
import org.apache.spark.sql.catalyst.expressions.{Alias, ApplyAsyncFunctionExpression, AttributeReference, Expression, NamedExpression}
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, EvalAsyncFunctions, LogicalPlan, Project, Subquery}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.ASYNC_FUNCTION

/**
 * Moves async functions that consume aggregate results into a Project above the Aggregate. This
 * mirrors ExtractPythonUDFFromAggregate and prevents an AggregateExpression from being projected
 * over raw child rows inside EvalAsyncFunctions.
 */
object ExtractAsyncFunctionsFromAggregate extends Rule[LogicalPlan] {

  private def belongsToAggregate(expression: Expression, aggregate: Aggregate): Boolean = {
    expression.isInstanceOf[AggregateExpression] ||
      aggregate.groupingExpressions.exists(_.semanticEquals(expression))
  }

  private def hasAsyncOverAggregate(
      expression: Expression,
      aggregate: Aggregate): Boolean = expression.exists {
    case async: ApplyAsyncFunctionExpression =>
      async.references.isEmpty || async.exists(belongsToAggregate(_, aggregate))
    case _ => false
  }

  private def extract(aggregate: Aggregate): LogicalPlan = {
    val projectList = new ArrayBuffer[NamedExpression]()
    val aggregateExpressions = new ArrayBuffer[NamedExpression]()
    aggregate.aggregateExpressions.foreach { expression =>
      if (hasAsyncOverAggregate(expression, aggregate)) {
        val rewritten = expression.transformDown {
          case e: Expression if belongsToAggregate(e, aggregate) =>
            val alias = e match {
              case named: NamedExpression => named
              case other => Alias(other, "aggregate")()
            }
            aggregateExpressions += alias
            alias.toAttribute
        }
        projectList += rewritten.asInstanceOf[NamedExpression]
      } else {
        aggregateExpressions += expression
        projectList += expression.toAttribute
      }
    }
    Project(projectList.toSeq,
      aggregate.copy(aggregateExpressions = aggregateExpressions.toSeq))
  }

  override def apply(plan: LogicalPlan): LogicalPlan = plan.transformUpWithPruning(
    _.containsPattern(ASYNC_FUNCTION)) {
    case aggregate: Aggregate
        if aggregate.aggregateExpressions.exists(hasAsyncOverAggregate(_, aggregate)) =>
      extract(aggregate)
  }
}

/**
 * SPARK-61672: lifts [[ApplyAsyncFunctionExpression]] calls out of the operator that contains them
 * into a dedicated [[EvalAsyncFunctions]] node evaluated before that operator, mirroring
 * `ExtractPythonUDFs`.
 *
 * The call expression is `Unevaluable`, so this extraction is mandatory for correctness: a call
 * left inside a Project or Filter would throw at execution time because neither interpreted eval
 * nor codegen can issue the remote call. The rule runs in the logical optimizer (see
 * `SparkOptimizer`), is non-excludable, and never matches an already-extracted
 * [[EvalAsyncFunctions]] node.
 *
 * Calls that are equal under `canonicalized` (the identity keyed on the provider's canonical name)
 * are evaluated once and share a single result attribute.
 */
object ExtractAsyncFunctions extends Rule[LogicalPlan] {

  /**
   * Returns the innermost async calls that can be evaluated in this pass. An outer async call is
   * deferred while any child contains another async call; recursive extraction then builds a
   * second EvalAsyncFunctions node after the inner result has become an attribute.
   */
  private def collectAsyncFunctions(
      expr: Expression): Seq[ApplyAsyncFunctionExpression] = expr match {
    case af: ApplyAsyncFunctionExpression =>
      val nested = af.children.flatMap(collectAsyncFunctions)
      if (nested.nonEmpty) nested else Seq(af)
    case e => e.children.flatMap(collectAsyncFunctions)
  }

  def apply(plan: LogicalPlan): LogicalPlan = plan match {
    // A correlated subquery is rewritten into a join later and will pass through this rule again;
    // extract async functions only once, as the Python UDF rule does.
    case s: Subquery if s.correlated => plan

    case _ => plan.transformUpWithPruning(_.containsPattern(ASYNC_FUNCTION)) {
      // Safe guard: the rule runs once, but if we meet an already-built node, leave it alone.
      case e: EvalAsyncFunctions => e
      case p: LogicalPlan => extract(p)
    }
  }

  private def extract(plan: LogicalPlan): LogicalPlan = {
    // Deduplicate by canonical form, preserving first-seen order.
    val distinct = mutable.LinkedHashMap[Expression, ApplyAsyncFunctionExpression]()
    plan.expressions.flatMap(collectAsyncFunctions).foreach { f =>
      distinct.getOrElseUpdate(f.canonicalized, f)
    }
    val funcs = distinct.values.filter(_.references.subsetOf(plan.inputSet)).toSeq
    if (funcs.isEmpty) {
      plan
    } else {
      // Key the rewrite map on canonical form so identical calls collapse to one attribute.
      val attributeMap = mutable.HashMap[Expression, Expression]()
      val newChildren = plan.children.map { child =>
        val validFuncs = funcs.filter(_.references.subsetOf(child.outputSet))
        if (validFuncs.nonEmpty) {
          val resultAttrs = validFuncs.zipWithIndex.map { case (f, i) =>
            AttributeReference(s"asyncFunc$i", f.dataType, f.nullable)()
          }
          attributeMap ++= validFuncs.map(_.canonicalized).zip(resultAttrs)
          EvalAsyncFunctions(validFuncs, resultAttrs, child)
        } else {
          child
        }
      }
      // A call that references more than one child cannot be evaluated here.
      funcs.filterNot(f => attributeMap.contains(f.canonicalized)).foreach { f =>
        throw SparkException.internalError(
          s"Invalid async function $f, requires attributes from more than one child.")
      }
      val rewritten = plan.withNewChildren(newChildren).transformExpressions {
        case af: ApplyAsyncFunctionExpression if attributeMap.contains(af.canonicalized) =>
          attributeMap(af.canonicalized)
      }
      // Extract any remaining async calls that were nested above the ones just handled.
      val newPlan = extract(rewritten)
      if (newPlan.output != plan.output) {
        // Trim the appended result columns if the parent did not keep them.
        Project(plan.output, newPlan)
      } else {
        newPlan
      }
    }
  }
}
