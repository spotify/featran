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

package com.spotify.featran.scio

import com.esotericsoftware.kryo.{Kryo, Serializer}
import com.esotericsoftware.kryo.io.{Input, Output}
import com.spotify.scio.coders.KryoRegistrar
import com.twitter.algebird.QTree
import com.twitter.chill._

@KryoRegistrar
class FeatranKryoRegistrar extends IKryoRegistrar {
  override def apply(k: Kryo): Unit =
    k.forClass(new QTreeSerializer)
}

private class QTreeSerializer extends Serializer[QTree[Any]] {
  setImmutable(true)

  override def read(kryo: Kryo, input: Input, cls: Class[QTree[Any]]): QTree[Any] = {
    val (v1, v2, v3) = (input.readLong(), input.readInt(), input.readLong())
    val v4 = kryo.readClassAndObject(input)
    val v5 = kryo.readClassAndObject(input).asInstanceOf[Option[QTree[Any]]]
    val v6 = kryo.readClassAndObject(input).asInstanceOf[Option[QTree[Any]]]
    QTree(v1, v2, v3, v4, v5, v6)
  }

  override def write(kryo: Kryo, output: Output, obj: QTree[Any]): Unit = {
    output.writeLong(obj._1)
    output.writeInt(obj._2)
    output.writeLong(obj._3)
    kryo.writeClassAndObject(output, obj._4)
    kryo.writeClassAndObject(output, obj._5)
    kryo.writeClassAndObject(output, obj._6)
  }
}
