package com.simba.PIN;

/*
 * This will have methods that can be utilized across several compression methods
 * E.g packLengths()
 */
public class CompressionUtils {

  public byte[] packLengths(int[] lenBuf, int bitsPerLen, int minLen) {
    return packLengths(lenBuf, lenBuf.length, bitsPerLen, minLen);
  }

  /**
   * This method takes in a length array (where length = #bits), bitsPerLen and the minimum length
   * And packs the lengths together. So, if you send in the lengths as {1, 2, 3}, which means the
   * first element has 1 bit, second has 2 bits and third has 3 bits. Then, this method would send
   * back {0110} (the min_len is subtracted)
   * @Param lenBuf array of lengths
   * @Param lenNum number of lengths being sent
   * @Param bitsPerLen number of bits required for each length
   * @param minLen self explanatory 
   * @return a byte array of packed lengths
   */
  public byte[] packLengths(int[] lenBuf, int lenNum, int bitsPerLen, int minLen) {
    

    int size = lenNum;
    int bufsize = (size * bitsPerLen + 7) / 8;
    byte[] packedLens = new byte[bufsize];
    int acc = 0;
    int bits = 0;
    int bufIdx = 0;

    for (int i = 0; i < size; i++) {
      for (; bits > 7; bits -= 8) {
        packedLens[bufIdx++] = (byte) acc;
        acc >>>= 8;
      }
      acc |= (lenBuf[i] - minLen) << bits;
      bits += bitsPerLen;
    }

    for (; bits > 0; bits -= 8) {
      packedLens[bufIdx++] = (byte) acc;
      acc >>>= 8;
    }
    return packedLens;
  }
  /**
   * Given an input array which already may have some bytes or not, empty the accumulator, byte at
   * a time and put it into the inp_array. return that array The reason for this here is that we
   * have an accumulator, which basically does what the name suggests. While it has less than 8
   * bytes, we keep adding more bits to it (which is compressed data) and then once that threshold
   * is reached, we empty it by writing the individual bytes into a packed array
   * @Param acc long accumulator
   * @Param index the start index from where the packedInts array should be updated 
   * @param packedInts an array of packedInts
   */
  public byte[] emptyAccumulator(long acc, int index, byte[] packedInts) {
    
    packedInts[index++] = (byte) acc;
    acc >>= 8;
    packedInts[index++] = (byte) acc;
    acc >>= 8;
    packedInts[index++] = (byte) acc;
    acc >>= 8;
    packedInts[index++] = (byte) acc;
    acc >>= 8;
    packedInts[index++] = (byte) acc;
    acc >>= 8;
    packedInts[index++] = (byte) acc;
    acc >>= 8;
    packedInts[index++] = (byte) acc;
    acc >>= 8;
    packedInts[index++] = (byte) acc;

    return packedInts;
  }
}
