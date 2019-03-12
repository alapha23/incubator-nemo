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
package org.apache.nemo.client.beam;

import org.apache.nemo.client.JobLauncher;
import org.apache.nemo.driver.NemoDriver;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;
import org.apache.nemo.client.ClientEndpoint;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.metrics.MetricResults;
import org.joda.time.Duration;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * Beam result.
 */
public final class NemoPipelineResult extends ClientEndpoint implements PipelineResult {

  private static final Logger LOG = LoggerFactory.getLogger(NemoPipelineResult.class.getName());
  /**
   * Default constructor.
   */
  public NemoPipelineResult() {
    super(new BeamStateTranslator());
  }

  @Override
  public State getState() {
    return (State) super.getPlanState();
  }

  @Override
  public State cancel() throws IOException {
    throw new UnsupportedOperationException("cancel() in frontend.beam.NemoPipelineResult");
  }

  @Override
  public State waitUntilFinish(final Duration duration) {
    // Insteading of hanging the job, we make this function non-blocking
    try {
      LOG.info("Duration {}", duration.getStandardSeconds());

      Thread.sleep(TimeUnit.SECONDS.toMillis(duration.getStandardSeconds()));
      LOG.info("After sleeping {}", duration.getStandardSeconds());
      JobLauncher.shutdown();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
    return State.DONE;
  }

  @Override
  public State waitUntilFinish() {
    return waitUntilFinish(Duration.ZERO);
  }

  @Override
  public MetricResults metrics() {
    throw new UnsupportedOperationException("metrics() in frontend.beam.NemoPipelineResult");
  }
}
