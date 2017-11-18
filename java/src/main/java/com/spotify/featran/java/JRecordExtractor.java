/*
 * Copyright 2017 Spotify AB.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.spotify.featran.java;

import com.spotify.featran.RecordExtractor;

import java.util.List;

/**
 * Java wrapper for {@link RecordExtractor}.
 */
public class JRecordExtractor<T, F> {

  private final RecordExtractor<T, F> self;

  JRecordExtractor(RecordExtractor<T, F> self) {
    this.self = self;
  }

  /**
   * Java wrapper for {@link RecordExtractor#featureNames()}.
   */
  public List<String> featureNames() {
    return JavaOps.featureNames(self);
  }

  /**
   * Java wrapper for {@link RecordExtractor#featureValue(Object)}.
   */
  public F featureValue(T record) {
    return self.featureValue(record);
  }

}
