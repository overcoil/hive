package com.simba.PIN;

public class SimbaCompressorCommon {
  /*
   * Main class, that should be extended by all the other classes generally. This can have the main
   * redundant members, min_len, max_len, bitsPerLen, size, and so forth. All the getters can go
   * here, and then the rest of the compressors can only care about the logic.
   */
  protected int minLen;
  protected int maxLen;
  protected int[] lenBuf;
  protected int bitsPerLen;
  protected CompressionUtils cUtils;
  protected BaseEnDataLayout compressedData;

  public BaseEnDataLayout getCompressedData() {
    return this.compressedData;
  }

  public int getMinLen() {
    return this.minLen;
  }

  public int getMaxLen() {
    return this.maxLen;
  }

  public int[] getLengthBuffer() {
    return this.lenBuf;
  }

  public SimbaCompressorCommon() {
    this.maxLen = 0;
    this.minLen = 8;
    this.compressedData = new BaseEnDataLayout();
    this.cUtils = new CompressionUtils();
  }

}
