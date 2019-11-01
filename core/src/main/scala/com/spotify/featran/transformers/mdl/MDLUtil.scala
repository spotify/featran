/*
 * Copyright 2018 Spotify AB.
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

package com.spotify.featran.transformers.mdl

private object MDLUtil {
  def plusI(x: Array[Long], y: Array[Long]): Unit = {
    var i = 0
    while (i < x.length) {
      x(i) += y(i)
      i += 1
    }
  }

  def plus(x: Array[Long], y: Array[Long]): Array[Long] = {
    val r = Array.fill(x.length)(0L)
    var i = 0
    while (i < x.length) {
      r(i) = x(i) + y(i)
      i += 1
    }
    r
  }

  def minus(x: Array[Long], y: Array[Long]): Array[Long] = {
    val r = Array.fill(x.length)(0L)
    var i = 0
    while (i < x.length) {
      r(i) = x(i) - y(i)
      i += 1
    }
    r
  }
}
