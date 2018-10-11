/*
 * Copyright (C) 2018 Seoul National University
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nemo.compiler.frontend.beam.transform;

import org.apache.beam.runners.core.*;
import org.apache.beam.runners.core.construction.SerializablePipelineOptions;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.reflect.DoFnInvoker;
import org.apache.beam.sdk.transforms.reflect.DoFnInvokers;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.nemo.common.ir.OutputCollector;
import org.apache.nemo.common.ir.vertex.transform.Transform;
import org.apache.nemo.compiler.frontend.beam.NemoPipelineOptions;

import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * DoFn transform implementation.
 *
 * @param <InputT> input type.
 * @param <InterT> intermediate type.
 * @param <OutputT> output type.
 */
public abstract class AbstractTransform<InputT, InterT, OutputT> implements
  Transform<WindowedValue<InputT>, WindowedValue<OutputT>> {

  private OutputCollector<WindowedValue<OutputT>> outputCollector;
  private final TupleTag<OutputT> mainOutputTag;
  private final List<TupleTag<?>> additionalOutputTags;
  private final Collection<PCollectionView<?>> sideInputs;
  private final WindowingStrategy<?, ?> windowingStrategy;
  private final DoFn<InterT, OutputT> doFn;
  private final SerializablePipelineOptions serializedOptions;
  private transient DoFnRunner<InterT, OutputT> doFnRunner;
  private transient SideInputReader sideInputReader;
  private transient DoFnInvoker<InterT, OutputT> doFnInvoker;
  private final Coder<InputT> inputCoder;
  private final Map<TupleTag<?>, Coder<?>> outputCoders;
  private transient DoFnRunners.OutputManager outputManager;

  /**
   * DoFnTransform Constructor.
   *
   * @param doFn    doFn.
   * @param options Pipeline options.
   */
  public AbstractTransform(final DoFn<InterT, OutputT> doFn,
                           final Coder<InputT> inputCoder,
                           final Map<TupleTag<?>, Coder<?>> outputCoders,
                           final TupleTag<OutputT> mainOutputTag,
                           final List<TupleTag<?>> additionalOutputTags,
                           final WindowingStrategy<?, ?> windowingStrategy,
                           final Collection<PCollectionView<?>> sideInputs,
                           final PipelineOptions options) {
    this.doFn = doFn;
    this.inputCoder = inputCoder;
    this.outputCoders = outputCoders;
    this.mainOutputTag = mainOutputTag;
    this.additionalOutputTags = additionalOutputTags;
    this.sideInputs = sideInputs;
    this.serializedOptions = new SerializablePipelineOptions(options);
    this.windowingStrategy = windowingStrategy;
  }

  final DoFnRunners.OutputManager getOutputManager() {
    return outputManager;
  }

  final WindowingStrategy getWindowingStrategy() {
    return windowingStrategy;
  }

  final SideInputReader getSideInputReader() {
    return sideInputReader;
  }

  final TupleTag<OutputT> getMainOutputTag() {
    return mainOutputTag;
  }

  final DoFnRunner<InterT, OutputT> getDoFnRunner() {
    return doFnRunner;
  }

  @Override
  public final void prepare(final Context context, final OutputCollector<WindowedValue<OutputT>> oc) {
    // deserialize pipeline option
    final NemoPipelineOptions options = serializedOptions.get().as(NemoPipelineOptions.class);
    this.outputCollector = oc;

    // create output manager
    outputManager = new DefaultOutputManager<>(
      outputCollector, context, mainOutputTag);

    // create side input reader
    if (!sideInputs.isEmpty()) {
      sideInputReader = new BroadcastVariableSideInputReader(context, sideInputs);
    } else {
      sideInputReader = NullSideInputReader.of(sideInputs);
    }

    // create step context
    // this transform does not support state and timer.
    final StepContext stepContext = new StepContext() {
      @Override
      public StateInternals stateInternals() {
        throw new UnsupportedOperationException("Not support stateInternals in DoFnTransform");
      }

      @Override
      public TimerInternals timerInternals() {
        throw new UnsupportedOperationException("Not support timerInternals in DoFnTransform");
      }
    };

    final DoFn wrappedDoFn = wrapDoFn(doFn);

    // invoker
    doFnInvoker = DoFnInvokers.invokerFor(wrappedDoFn);
    doFnInvoker.invokeSetup();

    // DoFnRunners.simpleRunner takes care of all the hard stuff of running the DoFn
    // and that this approach is the standard used by most of the Beam runners
    doFnRunner = DoFnRunners.simpleRunner(
      options,
      wrappedDoFn,
      sideInputReader,
      outputManager,
      mainOutputTag,
      additionalOutputTags,
      stepContext,
      inputCoder,
      outputCoders,
      windowingStrategy);

    doFnRunner.startBundle();
  }

  abstract DoFn wrapDoFn(final DoFn initDoFn);

  @Override
  public abstract void onData(final WindowedValue<InputT> data);

  public final DoFn getDoFn() {
    return doFn;
  }

  abstract void beforeClose();

  @Override
  public final void close() {
    beforeClose();

    doFnRunner.finishBundle();
    doFnInvoker.invokeTeardown();
  }

  @Override
  public abstract String toString();
}
