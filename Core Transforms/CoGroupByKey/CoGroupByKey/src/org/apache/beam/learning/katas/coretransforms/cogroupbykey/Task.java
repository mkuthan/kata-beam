/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.beam.learning.katas.coretransforms.cogroupbykey;

import org.apache.beam.learning.katas.util.Log;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;

import static org.apache.beam.sdk.values.TypeDescriptors.kvs;
import static org.apache.beam.sdk.values.TypeDescriptors.strings;

public class Task {

  public static void main(String[] args) {
    PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();
    Pipeline pipeline = Pipeline.create(options);

    PCollection<String> fruits =
        pipeline.apply("Fruits",
            Create.of("apple", "banana", "cherry")
        );

    PCollection<String> countries =
        pipeline.apply("Countries",
            Create.of("australia", "brazil", "canada")
        );

    PCollection<String> output = applyTransform(fruits, countries);

    output.apply(Log.ofElements());

    pipeline.run();
  }

  static PCollection<String> applyTransform(
      PCollection<String> fruits, PCollection<String> countries) {

    MapElements<String, KV<String, String>> byFirstLetter = MapElements
        .into(kvs(strings(), strings()))
        .via(word -> KV.of(word.substring(0, 1), word));

    TupleTag<String> fruitsTag = new TupleTag<>();
    TupleTag<String> countriesTag = new TupleTag<>();

    PCollection<KV<String, String>> fruitsByKey = fruits.apply("fruitsByKey", byFirstLetter);

    PCollection<KV<String, String>> countriesByKey = countries.apply("countriesByKey", byFirstLetter);

    PCollection<KV<String, CoGbkResult>> fruitsAndCountries = KeyedPCollectionTuple
        .of(fruitsTag, fruitsByKey)
        .and(countriesTag, countriesByKey)
        .apply(CoGroupByKey.create());

    return fruitsAndCountries.apply(ParDo.of(new FormatResultFn(fruitsTag, countriesTag)));
  }

  static class FormatResultFn extends DoFn<KV<String, CoGbkResult>, String> {

    private final TupleTag<String> fruitsTag;
    private final TupleTag<String> countriesTag;

    public FormatResultFn(TupleTag<String> fruitsTag, TupleTag<String> countriesTag) {
      this.fruitsTag = fruitsTag;
      this.countriesTag = countriesTag;
    }

    @ProcessElement
    public void processElement(@Element KV<String, CoGbkResult> fruitAndCountry, OutputReceiver<String> out) {
      String alphabet = fruitAndCountry.getKey();
      CoGbkResult coGbkResult = fruitAndCountry.getValue();

      String fruit = coGbkResult.getOnly(fruitsTag);
      String country = coGbkResult.getOnly(countriesTag);

      out.output(new WordsAlphabet(alphabet, fruit, country).toString());
    }
  }
}