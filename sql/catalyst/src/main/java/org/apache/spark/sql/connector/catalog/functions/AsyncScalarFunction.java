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

package org.apache.spark.sql.connector.catalog.functions;

import java.io.Serializable;
import java.util.concurrent.CompletableFuture;

import org.apache.spark.annotation.Evolving;
import org.apache.spark.sql.catalyst.InternalRow;

/**
 * SPARK-61672: a {@link BoundFunction} whose result for each input row is produced asynchronously,
 * typically by a remote call such as a model inference endpoint.
 * <p>
 * Unlike {@link ScalarFunction}, which produces a result synchronously on the task thread, an
 * async scalar function returns a {@link CompletableFuture}. The engine keeps a bounded number of
 * such futures in flight per task and emits output rows in input order. This lets a single Spark
 * task overlap many high-latency calls instead of processing rows one at a time, while leaving
 * Spark's stage, retry and speculation model unchanged.
 * <p>
 * <b>Execution contract.</b>
 * <ul>
 *   <li>The engine calls {@link #produceResultAsync(InternalRow)} on the task thread with a copy of
 *       the projected argument row, then moves on to the next input without blocking.</li>
 *   <li>At most {@link #maxInFlightPerTask()} futures are outstanding at once (further clamped by a
 *       cluster-wide engine ceiling).</li>
 *   <li>Output rows are emitted strictly in input order regardless of completion order. One output
 *       row is produced per input row; cardinality and partitioning are preserved.</li>
 *   <li>A future that completes exceptionally fails the task, which is the strict error mode. A
 *       provider that wants record-level tolerance should complete the future normally with a
 *       sentinel (for example NULL or a status struct) rather than failing it.</li>
 * </ul>
 * <p>
 * <b>At-least-once.</b> A Spark task retry, speculative attempt, stage re-execution or executor
 * loss re-runs the whole partition, which re-issues calls for that partition. This interface does
 * not and cannot provide exactly-once external invocation.
 * <p>
 * <b>Determinism.</b> Implementations must override {@link #isDeterministic()} and return
 * {@code false} whenever the remote service may return different values for the same input. Spark
 * may evaluate equivalent deterministic calls once, so reporting {@code true} is a semantic
 * promise, not merely a performance hint. Generative model functions should normally return
 * {@code false}.
 * <p>
 * <b>Serialization.</b> A bound function is shipped to executor tasks. Implementations must keep
 * clients, credential providers, threads, and other non-serializable state transient and create
 * them lazily on the executor.
 *
 * @param <R> the JVM type of result values, consistent with {@link #resultType()} under the same
 *            mapping documented on {@link ScalarFunction}.
 *
 * @since 4.4.0
 */
@Evolving
public interface AsyncScalarFunction<R> extends BoundFunction, Serializable {

  /**
   * Issues the call for a single input row and returns a future for its result.
   * <p>
   * This method is invoked on the task thread and must not block; all waiting is done by the
   * engine on the returned future. The {@code input} row is owned by the engine and is safe to
   * read synchronously inside this method, but must not be retained by any helper thread after the
   * method returns.
   *
   * @param input the projected, engine-owned argument row for one output row
   * @return a future that completes with the result value or completes exceptionally with the
   *         terminal error
   */
  CompletableFuture<R> produceResultAsync(InternalRow input);

  /**
   * The maximum number of futures the engine keeps outstanding per task for this function. The
   * engine clamps this to {@code spark.sql.asyncFunction.maxInFlightCeiling}. Defaults to 16.
   */
  default int maxInFlightPerTask() {
    return 16;
  }

  /**
   * Returns whether repeated calls with the same inputs are guaranteed to produce the same result.
   * Implementations must make this choice explicitly because Spark may reuse deterministic calls.
   * Generative model functions should normally return {@code false}.
   */
  @Override
  boolean isDeterministic();

  /**
   * A stable identity used to decide whether two deterministic calls in one operator are the same
   * and may be evaluated once. Implementations <b>must</b> override this; the
   * {@link BoundFunction} default is a random UUID, which would make every call distinct.
   */
  @Override
  String canonicalName();

  /**
   * Called at most once per distinct bound-function instance when the task finishes. The engine
   * first asks outstanding source futures to cancel and waits for a short bounded interval.
   * Cancellation is best effort, so an implementation must make this method idempotent and safe
   * when its client still has an operation in progress.
   */
  default void close() {}
}
