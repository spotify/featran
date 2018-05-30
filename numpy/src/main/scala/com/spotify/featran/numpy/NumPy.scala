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

import java.io.OutputStream

import simulacrum.typeclass

/**
 * Type class for NumPy numeric types.
 */
@typeclass trait NumPyType[@specialized(Int, Long, Float, Double) T] {
  def descr: String

  def sizeOf: Int

  def write(out: OutputStream, value: T): Unit
}

object NumPyType {

  // from Guava LittleEndianDataOutputStream
  implicit class LittleEndianOutputStream(val out: OutputStream) extends AnyVal {
    def writeInt(v: Int): Unit = {
      out.write(0xFF & v)
      out.write(0xFF & (v >> 8))
      out.write(0xFF & (v >> 16))
      out.write(0xFF & (v >> 24))
    }

    def writeLong(v: Long): Unit = {
      var value = java.lang.Long.reverseBytes(v)
      val result = new Array[Byte](8)
      var i = 7
      while (i >= 0) {
        result(i) = (value & 0xFFL).toByte
        value >>= 8
        i -= 1
      }
      out.write(result)
    }

    def writeFloat(v: Float): Unit = writeInt(java.lang.Float.floatToIntBits(v))
    def writeDouble(v: Double): Unit =
      writeLong(java.lang.Double.doubleToLongBits(v))
  }

  implicit val intNumPyType = new NumPyType[Int] {
    override val descr: String = "<i4"
    override val sizeOf: Int = 4
    override def write(out: OutputStream, value: Int): Unit =
      out.writeInt(value)
  }

  implicit val longNumPyType = new NumPyType[Long] {
    override val descr: String = "<i8"
    override val sizeOf: Int = 8
    override def write(out: OutputStream, value: Long): Unit =
      out.writeLong(value)
  }

  implicit val floatNumPyType = new NumPyType[Float] {
    override val descr: String = "<f4"
    override val sizeOf: Int = 4
    override def write(out: OutputStream, value: Float): Unit =
      out.writeFloat(value)
  }

  implicit val doubleNumPyType = new NumPyType[Double] {
    override val descr: String = "<f8"
    override val sizeOf: Int = 8
    override def write(out: OutputStream, value: Double): Unit =
      out.writeDouble(value)
  }

}

/**
 * Utilities for writing data as NumPy `.npy` files.
 */
object NumPy {

  private def header[T: NumPyType](dimensions: Seq[Int]): String = {
    // https://docs.scipy.org/doc/numpy/neps/npy-format.html
    val dims = dimensions.mkString(", ")
    val shape = if (dimensions.length > 1) s"($dims)" else s"($dims,)"
    val h =
      s"{'descr': '${NumPyType[T].descr}', 'fortran_order': False, 'shape': $shape, }"
    // 11 bytes: magic "0x93NUMPY", major version, minor version, (short) HEADER_LEN, '\n'
    val l = h.length + 11
    // pad magic string + 4 + HEADER_LEN to be evenly divisible by 16
    val n = if (l % 16 == 0) 0 else (l / 16 + 1) * 16 - l
    h + " " * n + "\n"
  }

  private def writeHeader[T: NumPyType](out: OutputStream, dimensions: Seq[Int]): Unit = {
    // magic
    out.write(0x93)
    out.write("NUMPY".getBytes)

    // major, minor
    out.write(1)
    out.write(0)

    // header
    val headerString = header(dimensions)
    // from Guava LittleEndianDataOutputStream#writeShort
    val l = headerString.length
    out.write(0xFF & l)
    out.write(0xFF & (l >> 8))
    out.write(headerString.getBytes)
  }

  private def writeData[T: NumPyType](out: OutputStream, data: Array[T]): Unit = {
    var i = 0
    while (i < data.length) {
      NumPyType[T].write(out, data(i))
      i += 1
    }
  }

  /**
   * Write an array as a NumPy `.npy` file to an output stream. Default shape is `(data.length)`.
   */
  def write[@specialized(Int, Long, Float, Double) T: NumPyType](
    out: OutputStream,
    data: Array[T],
    shape: Seq[Int] = Seq.empty): Unit = {

    val dims = if (shape.isEmpty) {
      Seq(data.length)
    } else {
      require(data.length == shape.product,
              s"Invalid shape, ${shape.mkString(" * ")} != ${data.length}")
      shape
    }
    writeHeader(out, dims)
    writeData(out, data)
    out.flush()
  }

  /**
   * Write an iterator of arrays as a 2-dimensional NumPy `.npy` file to an output stream. Each
   * array should have length `numCols` and the iterator should have `numRows` elements.
   */
  def write[@specialized(Int, Long, Float, Double) T: NumPyType](out: OutputStream,
                                                                 data: Iterator[Array[T]],
                                                                 numRows: Int,
                                                                 numCols: Int): Unit = {

    val dims = Seq(numRows, numCols)
    writeHeader[T](out, dims)
    var n = 0
    while (data.hasNext) {
      val row = data.next()
      require(row.length == numCols, s"Invalid row size, expected: $numCols, actual: ${row.length}")
      writeData(out, row)
      n += 1
    }
    require(n == numRows, s"Invalid number of rows, expected: $numRows, actual: $n")
    out.flush()
  }

}
