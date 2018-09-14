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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.nemo.common.ir.OutputCollector;
import org.apache.nemo.common.ir.vertex.transform.Transform;
import org.apache.nemo.compiler.frontend.beam.NemoPipelineOptions;
import org.apache.reef.io.Tuple;
import org.junit.Before;
import org.junit.Test;

import java.util.*;

import static java.util.Collections.emptyList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public final class DoFnTransformTest {

  // views and windows for testing side inputs
  private PCollectionView<Iterable<String>> view1;
  private PCollectionView<Iterable<String>> view2;

  @Before
  public void setUp() {
    Pipeline.create().apply(Create.of("1"));
    view1 = Pipeline.create().apply(Create.of("1")).apply(View.asIterable());
    view2 = Pipeline.create().apply(Create.of("2")).apply(View.asIterable());
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testSingleOutput() {

    final TupleTag<String> outputTag = new TupleTag<>("main-output");

    final SimpleDoFnTransform<String, String> simpleDoFnTransform =
      new SimpleDoFnTransform<>(
        new IdentityDoFn<>(),
        null,
        null,
        outputTag,
        Collections.emptyList(),
        WindowingStrategy.globalDefault(),
        emptyList(), /* side inputs */
        PipelineOptionsFactory.as(NemoPipelineOptions.class));

    final Transform.Context context = mock(Transform.Context.class);
    final OutputCollector<WindowedValue<String>> oc = new TestOutputCollector<>();
    simpleDoFnTransform.prepare(context, oc);

    simpleDoFnTransform.onData(WindowedValue.valueInGlobalWindow("Hello"));

    assertEquals(((TestOutputCollector<String>) oc).outputs.get(0), WindowedValue.valueInGlobalWindow("Hello"));

    simpleDoFnTransform.close();
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testMultiOutputOutput() {

    TupleTag<String> mainOutput = new TupleTag<>("main-output");
    TupleTag<String> additionalOutput1 = new TupleTag<>("output-1");
    TupleTag<String> additionalOutput2 = new TupleTag<>("output-2");

    ImmutableList<TupleTag<?>> tags = ImmutableList.of(additionalOutput1, additionalOutput2);

    ImmutableMap<String, String> tagsMap =
      ImmutableMap.<String, String>builder()
        .put(additionalOutput1.getId(), additionalOutput1.getId())
        .put(additionalOutput2.getId(), additionalOutput2.getId())
        .build();

    final SimpleDoFnTransform<String, String> simpleDoFnTransform =
      new SimpleDoFnTransform<>(
        new MultiOutputDoFn(additionalOutput1, additionalOutput2),
        null,
        null,
        mainOutput,
        tags,
        WindowingStrategy.globalDefault(),
        emptyList(), /* side inputs */
        PipelineOptionsFactory.as(NemoPipelineOptions.class));

    // mock context
    final Transform.Context context = mock(Transform.Context.class);
    when(context.getTagToAdditionalChildren()).thenReturn(tagsMap);

    final OutputCollector<WindowedValue<String>> oc = new TestOutputCollector<>();
    simpleDoFnTransform.prepare(context, oc);

    simpleDoFnTransform.onData(WindowedValue.valueInGlobalWindow("one"));
    simpleDoFnTransform.onData(WindowedValue.valueInGlobalWindow("two"));
    simpleDoFnTransform.onData(WindowedValue.valueInGlobalWindow("hello"));

    // main output
    assertEquals(WindowedValue.valueInGlobalWindow("got: hello"),
      ((TestOutputCollector<String>) oc).outputs.get(0));

    // additional output 1
    assertTrue(((TestOutputCollector<String>) oc).getTaggedOutputs().contains(
      new Tuple<>(additionalOutput1.getId(), WindowedValue.valueInGlobalWindow("extra: one"))
    ));
    assertTrue(((TestOutputCollector<String>) oc).getTaggedOutputs().contains(
      new Tuple<>(additionalOutput1.getId(), WindowedValue.valueInGlobalWindow("got: hello"))
    ));

    // additional output 2
    assertTrue(((TestOutputCollector<String>) oc).getTaggedOutputs().contains(
      new Tuple<>(additionalOutput2.getId(), WindowedValue.valueInGlobalWindow("extra: two"))
    ));
    assertTrue(((TestOutputCollector<String>) oc).getTaggedOutputs().contains(
      new Tuple<>(additionalOutput2.getId(), WindowedValue.valueInGlobalWindow("got: hello"))
    ));

    simpleDoFnTransform.close();
  }


  // TODO #216: implement side input and windowing
  @Test
  public void testSideInputs() {
    // mock context
    final Transform.Context context = mock(Transform.Context.class);
    when(context.getBroadcastVariable(view1)).thenReturn(
      WindowedValue.valueInGlobalWindow(ImmutableList.of("1")));
    when(context.getBroadcastVariable(view2)).thenReturn(
      WindowedValue.valueInGlobalWindow(ImmutableList.of("2")));

    TupleTag<Tuple<String, Iterable<String>>> outputTag = new TupleTag<>("main-output");

    WindowedValue<String> first = WindowedValue.valueInGlobalWindow("first");
    WindowedValue<String> second = WindowedValue.valueInGlobalWindow("second");

    final Map<String, PCollectionView<Iterable<String>>> eventAndViewMap =
      ImmutableMap.of(first.getValue(), view1, second.getValue(), view2);

    final SimpleDoFnTransform<String, Tuple<String, Iterable<String>>> simpleDoFnTransform =
      new SimpleDoFnTransform<>(
        new SimpleSideInputDoFn<>(eventAndViewMap),
        null,
        null,
        outputTag,
        Collections.emptyList(),
        WindowingStrategy.globalDefault(),
        ImmutableList.of(view1, view2), /* side inputs */
        PipelineOptionsFactory.as(NemoPipelineOptions.class));

    final OutputCollector<WindowedValue<Tuple<String, Iterable<String>>>> oc = new TestOutputCollector<>();
    simpleDoFnTransform.prepare(context, oc);

    simpleDoFnTransform.onData(first);
    simpleDoFnTransform.onData(second);

    assertEquals(WindowedValue.valueInGlobalWindow(new Tuple<>("first", ImmutableList.of("1"))),
      ((TestOutputCollector<Tuple<String,Iterable<String>>>) oc).getOutput().get(0));

    assertEquals(WindowedValue.valueInGlobalWindow(new Tuple<>("second", ImmutableList.of("2"))),
      ((TestOutputCollector<Tuple<String,Iterable<String>>>) oc).getOutput().get(1));

    simpleDoFnTransform.close();
  }

  @Test
  public void testStatefulFn() {

    final DoFn<KV<String, Long>, KV<String, Long>> filterElementsEqualToCountFn =
      new DoFn<KV<String, Long>, KV<String, Long>>() {

        @StateId("counter")
        private final StateSpec<ValueState<Long>> counterSpec =
          StateSpecs.value(VarLongCoder.of());

        @ProcessElement
        public void processElement(
          ProcessContext context, @StateId("counter") ValueState<Long> count) {
          long currentCount = Optional.ofNullable(count.read()).orElse(0L);
          currentCount = currentCount + 1;
          count.write(currentCount);

          KV<String, Long> currentElement = context.element();
          if (currentCount == currentElement.getValue()) {
            context.output(currentElement);
          }
        }
      };

    final TupleTag<KV<String, Long>> outputTag = new TupleTag<>("main-output");

    final StatefulDoFnTransform<String, Long, KV<String, Long>> statefulDoFnTransform =
      new StatefulDoFnTransform<>(
        filterElementsEqualToCountFn,
        null,
        null,
        outputTag,
        Collections.emptyList(),
        WindowingStrategy.globalDefault(),
        emptyList(), /* side inputs */
        PipelineOptionsFactory.as(NemoPipelineOptions.class));


    final Transform.Context context = mock(Transform.Context.class);
    final OutputCollector<WindowedValue<KV<String, Long>>> oc = new TestOutputCollector<>();
    statefulDoFnTransform.prepare(context, oc);

    // count: 3 for key "a"
    statefulDoFnTransform.onData(WindowedValue.valueInGlobalWindow(KV.of("a", 100L)));
    statefulDoFnTransform.onData(WindowedValue.valueInGlobalWindow(KV.of("a", 100L)));
    statefulDoFnTransform.onData(WindowedValue.valueInGlobalWindow(KV.of("a", 3L)));

    // count: 4 for key "b"
    statefulDoFnTransform.onData(WindowedValue.valueInGlobalWindow(KV.of("b", 100L)));
    statefulDoFnTransform.onData(WindowedValue.valueInGlobalWindow(KV.of("b", 100L)));
    statefulDoFnTransform.onData(WindowedValue.valueInGlobalWindow(KV.of("b", 100L)));
    statefulDoFnTransform.onData(WindowedValue.valueInGlobalWindow(KV.of("b", 4L)));

    assertEquals(ImmutableList.of(
      WindowedValue.valueInGlobalWindow(KV.of("a", 3L)),
      WindowedValue.valueInGlobalWindow(KV.of("b", 4L))),
      ((TestOutputCollector<KV<String,Long>>) oc).getOutput());

    statefulDoFnTransform.close();
  }

  private static final class TestOutputCollector<T> implements OutputCollector<WindowedValue<T>> {
    private final List<WindowedValue<T>> outputs;
    private final List<Tuple<String, WindowedValue<T>>> taggedOutputs;

    TestOutputCollector() {
      this.outputs = new LinkedList<>();
      this.taggedOutputs = new LinkedList<>();
    }

    @Override
    public void emit(WindowedValue<T> output) {
      outputs.add(output);
    }

    @Override
    public <O> void emit(String dstVertexId, O output) {
      final WindowedValue<T> val = (WindowedValue<T>) output;
      final Tuple<String, WindowedValue<T>> tuple = new Tuple<>(dstVertexId, val);
      taggedOutputs.add(tuple);
    }

    public List<WindowedValue<T>> getOutput() {
      return outputs;
    }

    public List<Tuple<String, WindowedValue<T>>> getTaggedOutputs() {
      return taggedOutputs;
    }
  }

  /**
   * Identitiy do fn.
   * @param <T> type
   */
  private static class IdentityDoFn<T> extends DoFn<T, T> {
    @ProcessElement
    public void processElement(final ProcessContext c) throws Exception {
      c.output(c.element());
    }
  }

  /**
   * Side input do fn.
   * @param <T> type
   */
  private static class SimpleSideInputDoFn<T, V> extends DoFn<T, Tuple<T, V>> {
    private final Map<T, PCollectionView<V>> idAndViewMap;

    public SimpleSideInputDoFn(final Map<T, PCollectionView<V>> idAndViewMap) {
      this.idAndViewMap = idAndViewMap;
    }

    @ProcessElement
    public void processElement(final ProcessContext c) throws Exception {
      final PCollectionView<V> view = idAndViewMap.get(c.element());
      final V sideInput = c.sideInput(view);
      c.output(new Tuple<>(c.element(), sideInput));
    }
  }


  /**
   * Multi output do fn.
   */
  private static class MultiOutputDoFn extends DoFn<String, String> {
    private TupleTag<String> additionalOutput1;
    private TupleTag<String> additionalOutput2;

    public MultiOutputDoFn(TupleTag<String> additionalOutput1, TupleTag<String> additionalOutput2) {
      this.additionalOutput1 = additionalOutput1;
      this.additionalOutput2 = additionalOutput2;
    }

    @ProcessElement
    public void processElement(ProcessContext c) throws Exception {
      if ("one".equals(c.element())) {
        c.output(additionalOutput1, "extra: one");
      } else if ("two".equals(c.element())) {
        c.output(additionalOutput2, "extra: two");
      } else {
        c.output("got: " + c.element());
        c.output(additionalOutput1, "got: " + c.element());
        c.output(additionalOutput2, "got: " + c.element());
      }
    }
  }
}
