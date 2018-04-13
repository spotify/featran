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

package com.spotify.featran.numpy

import java.io.{ByteArrayOutputStream, OutputStream}

import org.scalatest._

class NumPyTest extends FlatSpec with Matchers {

  private def test(f: OutputStream => Unit)(expectedFile: String): Unit = {
    val actual = {
      val baos = new ByteArrayOutputStream()
      f(baos)
      baos.toByteArray
    }

    val expected = {
      val in = this.getClass.getResourceAsStream(expectedFile)
      val out = new ByteArrayOutputStream(math.max(32, in.available()))
      val buf = new Array[Byte](8192)
      var r = in.read(buf)
      while (r != -1) {
        out.write(buf, 0, r)
        r = in.read(buf)
      }
      out.toByteArray
    }

    actual shouldBe expected
  }

  "NumPy" should "work with 1-dimensional arrays" in {
    val a1d = (0 until 10).toArray
    test(NumPy.write(_, a1d))("/a1d-int.npy")
    test(NumPy.write(_, a1d.map(_.toLong)))("/a1d-long.npy")
    test(NumPy.write(_, a1d.map(_.toFloat)))("/a1d-float.npy")
    test(NumPy.write(_, a1d.map(_.toDouble)))("/a1d-double.npy")

    // scalastyle:off no.whitespace.before.left.bracket
    the[IllegalArgumentException] thrownBy {
      test(NumPy.write(_, a1d, Seq(20)))("/a1d-int.npy")
    } should have message "requirement failed: Invalid shape, 20 != 10"
    // scalastyle:on no.whitespace.before.left.bracket
  }

  it should "work with 2-dimensional arrays" in {
    val a2d = (for {
      i <- 0 until 10
      j <- 0 until 5
    } yield i * 10 + j).toArray
    test(NumPy.write(_, a2d, Seq(10, 5)))("/a2d-int.npy")
    test(NumPy.write(_, a2d.map(_.toLong), Seq(10, 5)))("/a2d-long.npy")
    test(NumPy.write(_, a2d.map(_.toFloat), Seq(10, 5)))("/a2d-float.npy")
    test(NumPy.write(_, a2d.map(_.toDouble), Seq(10, 5)))("/a2d-double.npy")

    // scalastyle:off no.whitespace.before.left.bracket
    the[IllegalArgumentException] thrownBy {
      test(NumPy.write(_, a2d, Seq(20, 5)))("/a1d-int.npy")
    } should have message "requirement failed: Invalid shape, 20 * 5 != 50"
    // scalastyle:on no.whitespace.before.left.bracket
  }

  it should "work with iterators" in {
    val a2d = (0 until 10).map(i => (0 until 5).map(j => i * 10 + j).toArray)
    test(NumPy.write(_, a2d.iterator, 10, 5))("/a2d-int.npy")
    test(NumPy.write(_, a2d.iterator.map(_.map(_.toLong)), 10, 5))("/a2d-long.npy")
    test(NumPy.write(_, a2d.iterator.map(_.map(_.toFloat)), 10, 5))("/a2d-float.npy")
    test(NumPy.write(_, a2d.iterator.map(_.map(_.toDouble)), 10, 5))("/a2d-double.npy")

    // scalastyle:off no.whitespace.before.left.bracket
    the[IllegalArgumentException] thrownBy {
      test(NumPy.write(_, a2d.iterator, 10, 10))("/a2d-int.npy")
    } should have message "requirement failed: Invalid row size, expected: 10, actual: 5"

    the[IllegalArgumentException] thrownBy {
      test(NumPy.write(_, a2d.iterator, 20, 5))("/a2d-int.npy")
    } should have message "requirement failed: Invalid number of rows, expected: 20, actual: 10"

    // hit the header.length % 16 == 0 condition
    the[IllegalArgumentException] thrownBy {
      test(NumPy.write(_, a2d.iterator, 1000000000, 50))("/a2d-int.npy")
    } should have message "requirement failed: Invalid row size, expected: 50, actual: 5"
    // scalastyle:on no.whitespace.before.left.bracket
  }

}
