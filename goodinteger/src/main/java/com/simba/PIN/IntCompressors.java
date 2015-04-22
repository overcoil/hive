package com.simba.PIN;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.hive.service.cli.thrift.TTypeId;

public class IntCompressors extends SimbaCompressorCommon {

  private int bitoffset;
  private int bitsPerLen;
  private int index;

  public int getBitOffset() {
    return this.bitoffset;
  }

  private IntCompressors() {
    this.maxLen = 0;
    this.minLen = 32;
  }

  public static IntCompressors getIntCompressor() {
    return new IntCompressors();
  }

  private void computeIntegersMinMax(int[] vars, int size) {
    for (int i = 0; i < size; i++) {
      int num = vars[i];
      num = (-num >> 31) ^ (-num << 1);
      int wid = 32 - Integer.numberOfLeadingZeros(num);
      lenBuf[i] = wid;
      bitoffset += wid;
      if (this.minLen > wid) {
        this.minLen = wid;
      }
      if (this.maxLen < wid) {
        this.maxLen = wid;
      }
    }
  }
  /**
   * Main function that takes an array of integers and the size you want to compress and 
   * returns a byte array.
   * @Param vars array of integers
   * @Param size the size of the array you want to compress
   * @return byte array of compressed integers
   */
  public byte[] compressIntegers(int[] vars, int size) {
   
    lenBuf = new int[size];
    computeIntegersMinMax(vars, size);
    long acc = 0;
    int accbits = 0;
    int numbits = 0;

    int intNum = 0;
    long num = 0;

    bitsPerLen = 0;
    for (int i = maxLen - minLen; i != 0; i = i >> 1) {
      bitsPerLen++;
    }

    int encodedSize = this.compressedData.mySize(size, bitsPerLen, 4);

    this.compressedData.setVersion((byte) 0);

    this.compressedData.setBitsPerLen(bitsPerLen);
    this.compressedData.setMinLen(minLen);

    byte[] packedLens = this.cUtils.packLengths(lenBuf, bitsPerLen, minLen);

    this.compressedData.setPackedLens(packedLens);

    byte[] packedInts = this.compressedData.writeDataToBuffer(encodedSize);
    int index = this.compressedData.getIndex();

    for (int i = 0; i < size; i++) {
      numbits = lenBuf[i] - 1;

      if (numbits > 0) {
        intNum = vars[i];
        intNum = (-intNum >> 31) ^ (-intNum << 1);
        num = intNum & 0xFFFFFFFFL;
        num &= ~((long) 1 << numbits);
        acc |= num << accbits;
        accbits += numbits;
      } else {
        continue;
      }

      if (accbits >= 64) {
        packedInts = this.cUtils.emptyAccumulator(acc, index, packedInts);
        index += 8;
        accbits -= 64;
        if (numbits - accbits == 0) {
          acc = 0;
        } else {
          acc = num >>> (numbits - accbits);
        }
      }
    }

    int expr = (accbits + 7) / 8;
    switch (expr) {
      case 8:
        packedInts[index++] = (byte) acc;
        acc >>>= 8;

      case 7:
        packedInts[index++] = (byte) acc;
        acc >>>= 8;
      case 6:
        packedInts[index++] = (byte) acc;
        acc >>>= 8;

      case 5:
        packedInts[index++] = (byte) acc;
        acc >>>= 8;
      case 4:
        packedInts[index++] = (byte) acc;
        acc >>>= 8;

      case 3:
        packedInts[index++] = (byte) acc;
        acc >>>= 8;

      case 2:
        packedInts[index++] = (byte) acc;
        acc >>>= 8;

      case 1:
        packedInts[index++] = (byte) acc;
        
	  default:
		break;
    }

    return ArrayUtils.subarray(packedInts, 0, index);
  }
}
