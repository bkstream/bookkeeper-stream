/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.bookkeeper.stream.common;

import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.FutureFallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListenableScheduledFuture;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.SettableFuture;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.bookkeeper.util.MathUtils;
import org.apache.commons.lang.StringUtils;

import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Scheduler to execute stream operations
 */
public class Scheduler {

    public static class OrderingListenableFuture<V> implements ListenableFuture<V> {

        private final Object orderingKey;
        private final ListenableFuture<V> future;
        private final Scheduler scheduler;

        private OrderingListenableFuture(Object orderingKey,
                                         ListenableFuture<V> future,
                                         Scheduler scheduler) {
            Preconditions.checkNotNull(orderingKey);
            Preconditions.checkNotNull(future);
            Preconditions.checkNotNull(scheduler);
            this.orderingKey    = orderingKey;
            this.future         = future;
            this.scheduler      = scheduler;
        }

        @Override
        public void addListener(Runnable listener, Executor executor) {
            this.future.addListener(listener, executor);
        }

        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            return this.future.cancel(mayInterruptIfRunning);
        }

        @Override
        public boolean isCancelled() {
            return this.future.isCancelled();
        }

        @Override
        public boolean isDone() {
            return this.future.isDone();
        }

        @Override
        public V get() throws InterruptedException, ExecutionException {
            return this.future.get();
        }

        @Override
        public V get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
            return this.future.get(timeout, unit);
        }

        /**
         * Registers separate success and failure callbacks to be run when the Future's
         * computation is complete or, if the computation is already complete, immediately.
         *
         * The callback will be executed in same executor as the future computation run.
         *
         * @param callback
         * @return
         */
        public OrderingListenableFuture<V> addCallback(FutureCallback<V> callback) {
            Futures.addCallback(future, callback, scheduler.getExecutor(orderingKey));
            return this;
        }

        /**
         * Returns a new <i>ListenableFuture</i> whose result is asynchronously derived from the result
         * of the given <i>Future</i>. More precisely, the returned <i>Future</i> takes its result from
         * a <i>Future</i> produced by applying the given <i>AsyncFunction</i> to the result of original
         * <i>Future</i>.
         *
         * The AsyncFunction will be executed in same executor as the original future computation run.
         *
         * @param function A function to transform the result of the input future to the result of the
         *                 output future.
         * @return A future that holds result of the function (if input succeed), or the original input's
         * failure (if not)
         */
        public <O> OrderingListenableFuture<O> transform(AsyncFunction<? super V, ? extends O> function) {
            ListenableFuture<O> transformedFuture =
                    Futures.transform(future, function, scheduler.getExecutor(orderingKey));
            return new OrderingListenableFuture<>(orderingKey, transformedFuture, scheduler);
        }

        /**
         * Returns a new <i>ListenableFuture</i> whose result is the product of applying the given
         * <i>Function</i> to the result of the given <i>Future</i>.
         *
         * The Function will be executed in same executor as the original future computation run.
         *
         * @param function A function to transform the results of the provided future to the results of
         *                 the returned future.
         * @return A future that holds result of the function (if input succeed), or the original input's
         * failure (if not)
         */
        public <O> OrderingListenableFuture<O> tranform(Function<? super V, ? extends O> function) {
            ListenableFuture<O> transformedFuture =
                    Futures.transform(future, function, scheduler.getExecutor(orderingKey));
            return new OrderingListenableFuture<>(orderingKey, transformedFuture, scheduler);
        }

        /**
         * Returns a <i>Future</i> whose result is taken current future or, if current future fails, from
         * the Future provided by the <i>fallback</i>.
         * {@link com.google.common.util.concurrent.FutureFallback#create(Throwable)} is not invoked until
         * current future has failed, so if current future succeeds, it is never invoked. If during the
         * invocation of fallback, an exception is thrown, the exception is used as the result of returned
         * <i>Future</i>.
         *
         * @param fallback future fallback to be called if current future fails.
         * @return A future representing current future with fallback.
         */
        public OrderingListenableFuture<V> fallback(FutureFallback<V> fallback) {
            ListenableFuture<V> fallbackFuture =
                    Futures.withFallback(future, fallback, scheduler.getExecutor(orderingKey));
            return new OrderingListenableFuture<>(orderingKey, fallbackFuture, scheduler);
        }
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static class Builder {

        private String _name;
        private int _numExecutors;

        private Builder() {}

        public Builder name(String name) {
            this._name = name;
            return this;
        }

        public Builder numExecutors(int numExecutors) {
            this._numExecutors = numExecutors;
            return this;
        }

        public Scheduler build() {
            return new Scheduler(this._name, this._numExecutors);
        }

    }

    protected final String name;
    protected final ListeningScheduledExecutorService[] executors;
    protected final Random random;

    private Scheduler(String name,
                      int numExecutors) {
        Preconditions.checkArgument(StringUtils.isNotBlank(name), "Require a not-blank scheduler name");
        Preconditions.checkArgument(numExecutors > 0, "Require positive value for num executors");

        this.name = name;
        this.executors = new ListeningScheduledExecutorService[numExecutors];
        for (int i = 0; i < numExecutors; i++) {
            StringBuilder sb = new StringBuilder(name);
            sb.append("-executor-").append(i).append("-%d");
            ThreadFactoryBuilder tfb = new ThreadFactoryBuilder().setNameFormat(sb.toString());
            executors[i] = MoreExecutors.listeningDecorator(Executors.newSingleThreadScheduledExecutor(tfb.build()));
        }
        this.random = new Random(System.currentTimeMillis());
    }

    /**
     * Get the executor service to run tasks ordering by <i>key</i>.
     *
     * @param key ordering key to choose executor to run tasks.
     * @return executor service to run tasks ordering by <i>key</i>.
     */
    private ListeningScheduledExecutorService getExecutor(Object key) {
        if (executors.length == 1) {
            return executors[0];
        }
        return executors[MathUtils.signSafeMod(Objects.hashCode(key), executors.length)];
    }

    /**
     * Get an executor service to run tasks.
     *
     * @return executor service to run tasks.
     */
    private ListeningScheduledExecutorService getExecutor() {
        if (executors.length == 1) {
            return executors[0];
        }
        return executors[random.nextInt(executors.length)];
    }

    /**
     * Submit a value-returning <i>task</i> for execution and returns
     * a Future representing the pending result for the task. All
     * tasks submitted using same <i>key</i> would be executed in same
     * thread.
     *
     * @param key
     *          submit key
     * @param task
     *          task to execute
     */
    public <V> OrderingListenableFuture<V> submit(Object key, Callable<V> task) {
        return new OrderingListenableFuture<>(key, getExecutor(key).submit(task), this);
    }

    /**
     * Submit a <i>task</i> for execution and returns a Future representing the result for
     * the task. All tasks submitted using same <i>key</i> would be executed in same thread.
     *
     * @param key submit key
     * @param runnable task to execute
     * @return future representing the pending result for the task.
     */
    public OrderingListenableFuture<?> submit(Object key, Runnable runnable) {
        return new OrderingListenableFuture<>(key, getExecutor(key).submit(runnable), this);
    }

    /**
     * Schedule task to run in given <i>delay</i>
     *
     * @param key submit key
     * @param runnable task to schedule
     * @param delay delay
     * @param unit time unit of delay period
     * @return future representing the schedule result for the task.
     */
    public <V> OrderingListenableFuture<?> schedule(Object key, Runnable runnable, long delay, TimeUnit unit) {
        ListenableScheduledFuture<?> future = getExecutor(key).schedule(runnable, delay, unit);
        return new OrderingListenableFuture<>(key, future, this);
    }

    /**
     * Create an ordering future using ordering <i>key</i>.
     *
     * @param key ordering key
     * @param future future
     * @return ordering future.
     */
    public <V> OrderingListenableFuture<V> createOrderingFuture(Object key, SettableFuture<V> future) {
        return new OrderingListenableFuture<>(key, future, this);
    }

    /**
     * Submit a <i>task</i> for execution and returns a Future representing the result for
     * the task.
     *
     * @param runnable task to execute
     * @return future representing the pending result for the task.
     */
    public ListenableFuture<?> submit(Runnable runnable) {
        return getExecutor().submit(runnable);
    }

    /**
     * Schedule task to run in given <i>delay</i>
     *
     * @param runnable task to schedule
     * @param delay delay
     * @param unit time unit of delay period
     * @return future representing the schedule result for the task.
     */
    public ListenableFuture<?> schedule(Runnable runnable, long delay, TimeUnit unit) {
        return getExecutor().schedule(runnable, delay, unit);
    }

    /**
     * Shutdown the scheduler
     */
    public void shutdown() {
        for (ListeningScheduledExecutorService executor : executors) {
            executor.shutdown();
        }
    }
}
