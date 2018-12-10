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
package org.apache.nemo.compiler.frontend.beam.transform;

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.nemo.common.ir.OutputCollector;
import org.apache.nemo.common.punctuation.Watermark;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * DoFn transform implementation when there is no side input.
 *
 * @param <InputT> input type.
 * @param <OutputT> output type.
 */
public final class DoFnTransform<InputT, OutputT> extends AbstractDoFnTransform<InputT, InputT, OutputT> {
  private static final Logger LOG = LoggerFactory.getLogger(DoFnTransform.class.getName());

  private long inputWatermark = Long.MIN_VALUE;

  /**
   * DoFnTransform Constructor.
   */
  public DoFnTransform(final DoFn<InputT, OutputT> doFn,
                       final Coder<InputT> inputCoder,
                       final Map<TupleTag<?>, Coder<?>> outputCoders,
                       final TupleTag<OutputT> mainOutputTag,
                       final List<TupleTag<?>> additionalOutputTags,
                       final WindowingStrategy<?, ?> windowingStrategy,
                       final PipelineOptions options,
                       final DisplayData displayData) {
    super(doFn, inputCoder, outputCoders, mainOutputTag,
      additionalOutputTags, windowingStrategy, Collections.emptyMap(), options, displayData);
  }

  @Override
  protected DoFn wrapDoFn(final DoFn initDoFn) {
    return initDoFn;
  }

  @Override
  public void onData(final WindowedValue<InputT> data) {
    //LOG.info("{} OnData: {}", getContext().getIRVertex().getId(), data);
    // Do not need any push-back logic.
    checkAndInvokeBundle();
    getDoFnRunner().processElement(data);

    checkAndFinishBundle();
  }

  @Override
  public void onWatermark(final Watermark watermark) {
    checkAndInvokeBundle();
    inputWatermark = watermark.getTimestamp();
    getOutputCollector().emitWatermark(watermark);
    checkAndFinishBundle();
  }

  @Override
  protected void beforeClose() {
  }

  @Override
  OutputCollector wrapOutputCollector(final OutputCollector oc) {
    return new WrapOC(oc);
  }

  final class WrapOC implements OutputCollector<OutputT> {

    private final OutputCollector<OutputT> oc;

    WrapOC(final OutputCollector<OutputT> oc) {
      this.oc = oc;
    }

    @Override
    public void emit(OutputT output) {
      /*
      if (getContext().getIRVertex().getId().equals("vertex12") ||
        getContext().getIRVertex().getId().equals("vertex17")) {
        LOG.info("{} {} Output: {}", System.currentTimeMillis(), getContext().getIRVertex().getId(),
          output);
      }
      */
      oc.emit(output);
    }

    @Override
    public void emitWatermark(Watermark watermark) {

      oc.emitWatermark(watermark);
    }

    @Override
    public <T> void emit(String dstVertexId, T output) {
      oc.emit(dstVertexId, output);
    }
  }
}
