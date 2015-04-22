package com.simba.PIN;

import java.util.List;
import org.apache.hive.service.cli.*;

public class ColumnCompressorImpl implements ColumnCompressor {
  
  /**
   * Before calling the compress() method below, should check if the column is compressable 
   * by any given plugin.
   * @Param col Column to be checked
   * @return boolean which says yes/no
   */
  public boolean isCompressable(Column col) {
    switch (col.getType()) {
      case TINYINT_TYPE:
      case SMALLINT_TYPE:
      case BIGINT_TYPE:
      case FLOAT_TYPE:
      case DOUBLE_TYPE:
      case STRING_TYPE:
      case BINARY_TYPE:
      case BOOLEAN_TYPE:
        return false;
      case INT_TYPE:
        return true;
      default:
        break;
    }
    return false;
  }

  /**
   * Compresses a given column. 
   * @Param col A column to be compressed
   * @return byte array with compressed data
   */
  public byte[] compress(Column col) {
    switch (col.getType()) {
      case INT_TYPE:
        return IntCompressors.getIntCompressor().compressIntegers(col.getInts(), col.size());
      default:
        break;
    }
    return new byte[0];
  }
}
