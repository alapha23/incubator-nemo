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
package org.apache.nemo.runtime.executor.common.datatransfer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import java.util.ArrayDeque;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A blocking queue implementation which is capable of closing.
 *
 * @param <T> the type of elements
 */
@ThreadSafe
public final class ClosableBlockingQueue<T> implements AutoCloseable {
  private static final Logger LOG = LoggerFactory.getLogger(ClosableBlockingQueue.class.getName());
  private final ArrayBlockingQueue<T> queue;
  private volatile boolean closed = false;
  private volatile Throwable throwable = null;

  /**
   * Creates a closable blocking queue.
   */
  public ClosableBlockingQueue() {
    queue = new ArrayBlockingQueue<>(100);
  }

  public boolean isEmpty() {
    return queue.isEmpty();
  }

  /**
   * Creates a closable blocking queue.
   *
   * @param numElements the lower bound on initial capacity of the queue
   */
  public ClosableBlockingQueue(final int numElements) {
    queue = new ArrayBlockingQueue<>(numElements);
  }

  /**
   * Adds an element.
   *
   * @param element the element to add
   * @throws IllegalStateException if the input end of this queue has been closed
   * @throws NullPointerException if {@code element} is {@code null}
   */
  public void put(final T element) {
    if (element == null) {
      throw new NullPointerException();
    }
    if (closed) {
      throw new IllegalStateException("This queue has been closed");
    }

    //LOG.info("BlockingQueue add");

    queue.add(element);
    //notifyAll();
  }

  /**
   * Mark the input end of this queue as closed.
   */
  @Override
  public synchronized void close() {
    closed = true;
    //notifyAll();
  }

  /**
   * Mark the input end of this queue as closed.
   * @param throwableToSet a throwable to set as the cause
   */
  public synchronized void closeExceptionally(final Throwable throwableToSet) {
    this.throwable = throwableToSet;
    close();
  }

  /**
   * Retrieves and removes the head of this queue, waiting if necessary.
   *
   * @return the head of this queue, or {@code null} if no elements are there and this queue has been closed
   * @throws InterruptedException when interrupted while waiting
   */
  @Nullable
  public T take() throws InterruptedException {
    if (queue.isEmpty()) {
      throw new RuntimeException("The queue should not be empty");
    }

    return queue.poll();

    /*
    while (queue.isEmpty() && !closed) {
      wait();
    }

    // This should come after wait(), to be always checked on close
    if (throwable != null) {
      throw new RuntimeException(throwable);
    }

    //LOG.info("Remaining byteBuf: {}", queue.size());

    // retrieves and removes the head of the underlying collection, or return null if the queue is empty
    count.decrementAndGet();
    return queue.poll();
    */
  }

  /**
   * Retrieves, but does not removes, the head of this queue, waiting if necessary.
   *
   * @return the head of this queue, or {@code null} if no elements are there and this queue has been closed
   * @throws InterruptedException when interrupted while waiting
   */
  @Nullable
  public T peek() throws InterruptedException {
    return queue.peek();

    /*
    while (queue.isEmpty() && !closed) {
      wait();
    }

    // This should come after wait(), to be always checked on close
    if (throwable != null) {
      throw new RuntimeException(throwable);
    }

    // retrieves the head of the underlying collection, or return null if the queue is empty
    return queue.peek();
    */
  }
}
