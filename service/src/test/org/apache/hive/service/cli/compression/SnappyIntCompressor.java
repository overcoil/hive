/**
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

package org.apache.hive.service.cli.compression;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.DoubleBuffer;
import java.nio.IntBuffer;
import java.util.Arrays;

import org.apache.hive.service.cli.Column;
import org.iq80.snappy.Snappy;

/**
 * A ColumnCompressor implementation that uses snappy for compression.
 *
 */
public class SnappyIntCompressor implements ColumnCompressor {

  /**
   * Before calling the compress() method below, should check if the column is compressible by any
   * given plugin.
   *
   * @Param col Column to be checked
   * @return boolean which says yes/no
   */
  @Override
  public boolean isCompressible(Column col) {
    switch (col.getType()) {
      case INT_TYPE:
        return true;
      default:
        return false;
    }
  }

  /**
   * Compresses a given column.
   * 
   * @Param col A column to be compressed
   * @return byte array with compressed data
   */
  @Override
  public byte[] compress(Column col) {
    switch (col.getType()) {
      case INT_TYPE:
        return compressIntArray(col.getInts());
      default:
        return new byte[0];
    }
  }

  private byte[] compressIntArray(int[] input) {
    byte[] compressed = null;
    ByteBuffer byteBuffer = ByteBuffer.allocate(input.length * 4);
    IntBuffer intBuffer = byteBuffer.asIntBuffer();
    intBuffer.put(input);
    return snappyCompress(byteBuffer.array());
  }

  private byte[] snappyCompress(byte[] input) {
    int maxLength = Snappy.maxCompressedLength(input.length);
    byte[] compressed = new byte[maxLength];
    int compressedLength = Snappy.compress(input, 0, input.length, compressed, 0);
    return Arrays.copyOfRange(compressed, 0, compressedLength);
  }

  @Override
  public String getCompressorSet() {
    return "snappy";
  }

  @Override
  public String getVendor() {
    return "snappy";
  }

  /**
   * Method provided for testing purposes. Decompresses a compressed byte array. should be
   * compressed using the compress() function below which uses the snappy JAR files. If Snappy JAR
   * file isn't used, this method will return the wrong result.
   */
  public static int[] decompress(byte[] input) {
    byte[] uncompressed = Snappy.uncompress(input, 0, input.length);
    IntBuffer intBuffer = ByteBuffer.wrap(uncompressed).asIntBuffer();
    int[] array = new int[intBuffer.remaining()];
    intBuffer.get(array);
    return array;
  }
}
