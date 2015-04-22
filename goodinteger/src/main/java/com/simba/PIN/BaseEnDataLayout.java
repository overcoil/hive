package com.simba.PIN;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import org.apache.commons.lang3.ArrayUtils;

/*
 * This class represents the attributes of a standard object that
 * has all the members a compressor would need to send to the consumer.
 * For e.g for an int compressor, it would need
 * [version][numValues][bitsPerLen][minLen][packedLens][encodedValues]
 * while a string compressor would need
 * [version][type][numValues][bitsPerLen][minLen][prefixLen][prefix][packedLens][encodedValues]
 * Instead of manually writing each of these members into a bytebuffer, this class would encapsulate
 * those details and also provide methods to write these into a bytebuffer
 */



public class BaseEnDataLayout {

  protected byte version;
  protected int bitsPerLen;
  protected int minLen;
  protected byte[] packedLens;
  protected int index;

  public int getIndex() {
    return index;
  }

  public void setIndex(int index) {
    this.index = index;
  }
  /**
   * Given the bitsPerLen and the number of elements, my duty is to tell you how big can I get. As
   * of now, I assume that the layout looks as shown above. If the layout changes, then, the size
   * I return *should* also change. tl;dr change me if EnDataLayout undergoes member changes
   * @Param size size of array
   * @Param bitsPerLen number of bits required for each length 
   * @param bytesize size of the data type (e.g int is 4 bytes)
   * @return total capacity needed for the bytebuffer
   */
  public int mySize(int size, int bitsPerLen, int bytesize) {
    
    return 9 + (size * bitsPerLen + 7) / 8 + (bytesize * size);
  }
  /**
   * Method to write data into bytebuffer. 
   * @Param capacity amount of capacity needed for the bytebuffer
   * @return byte array of the byte buffer
   */
  public byte[] writeDataToBuffer(int capacity) {
    
    ByteBuffer enData = ByteBuffer.allocate(capacity);
    enData.order(ByteOrder.LITTLE_ENDIAN);
    byte[] auxData = new byte[3];
    auxData[0] = (byte)this.version;
    auxData[1] = (byte)this.bitsPerLen;
    auxData[2] = (byte)this.minLen;
    byte[] allData = ArrayUtils.addAll(auxData, this.packedLens);
    enData.put(allData);
    index = index + 3 + this.packedLens.length;
    return enData.array();
  }

  public byte getVersion() {
    return version;
  }

  public void setVersion(byte version) {
    this.version = version;
  }

  public int getBitsPerLen() {
    return bitsPerLen;
  }

  public void setBitsPerLen(int bitsPerLen) {
    this.bitsPerLen = bitsPerLen;
  }

  public int getMinLen() {
    return minLen;
  }

  public void setMinLen(int minLen) {
    this.minLen = minLen;
  }

  public byte[] getPackedLens() {
    return packedLens;
  }

  public void setPackedLens(byte[] packedLens) {
    this.packedLens = packedLens;
  }
}
