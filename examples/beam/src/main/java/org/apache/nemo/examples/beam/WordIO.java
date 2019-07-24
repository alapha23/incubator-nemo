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
package org.apache.nemo.examples.beam;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;

import java.util.Arrays;
import java.util.List;

/**
 * WordIO application.
 */
public final class WordIO {
  /**
   * Private Constructor.
   */
  private WordIO() {
  }

  /**
   * Main function for the MR BEAM program.
   *
   * @param args arguments.
   */

  public static void main(final String[] args) {
    final List<String> LINES = Arrays.asList(
      "To be, or not to be: that is the question: ",
      "Whether 'tis nobler in the mind to suffer ",
      "The slings and arrows of outrageous fortune, ",
      "Or to take arms against a sea of troubles, ");

    final PipelineOptions options = NemoPipelineOptionsFactory.create();
    options.setJobName("WordCount");

    final Pipeline p = Pipeline.create(options);
    final PCollection<String> result = p.apply(Create.of(LINES)).setCoder(StringUtf8Coder.of());
    //final PCollection<String> result = p.apply("readFile", TextIO.read().from(inputFilePath));
    PCollection<Integer> wordLengths = result.apply(
      MapElements.into(TypeDescriptors.integers())
        .via((String word) -> word.length()));

    p.run();
  }
}
