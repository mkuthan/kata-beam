/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.beam.learning.katas.coretransforms.branching;

import org.apache.beam.learning.katas.util.Log;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

public class Task {

  public static void main(String[] args) {
    PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();
    Pipeline pipeline = Pipeline.create(options);

    PCollection<Integer> numbers =
        pipeline.apply(Create.of(1, 2, 3, 4, 5));

    PCollection<Integer> mult5Results = applyMultiply5Transform(numbers);
    PCollection<Integer> mult10Results = applyMultiply10Transform(numbers);

    mult5Results.apply("Log multiply 5", Log.ofElements("Multiplied by 5: "));
    mult10Results.apply("Log multiply 10", Log.ofElements("Multiplied by 10: "));

    pipeline.run();
  }

  static PCollection<Integer> applyMultiply5Transform(PCollection<Integer> input) {
    return input.apply("multiplyBy5", ParDo.of(new MultiplyByFn(5)));
  }

  static PCollection<Integer> applyMultiply10Transform(PCollection<Integer> input) {
    return input.apply("multiplyBy10", ParDo.of(new MultiplyByFn(10)));
  }

  static class MultiplyByFn extends DoFn<Integer, Integer> {

    private final int by;

    public MultiplyByFn(int by) {
      this.by = by;
    }

    @ProcessElement
    public void processElement(@Element Integer in, OutputReceiver<Integer> out) {
      out.output(in * by);
    }

  }

}