package org.apache.hive.service.cli;
import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hive.service.cli.thrift.TEnColumn;
import org.apache.hive.service.cli.thrift.TRowSet;
import org.apache.hive.service.cli.thrift.TRow;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.BitSet;
import java.nio.ByteBuffer;

public class EncodedColumnBasedSet extends ColumnBasedSet {
  /**
   * Class that can handle resultSets which are a mix of compressed and uncompressed columns
   * To compress columns, it would need a JSON string a client would send(ThriftCLIService.java)
   * The JSON string would have vendor, compressorSet and the entryClass (for a plugin) 
   * Using them, server would know which entry class to call. A
   * All of these entry classes would have to implement the interface
   * (ColumnCompressor.java)
   * The compressorInfo is the JSON string in the form
   * {"INT_TYPE": {"vendor": (name), "compressorSet":(name), "entryClass": (name)}, ...} 
   * If the client mentioned the wrong vendor or classname or compressorSet, or 
   * it does so correctly but the server does not have it, the column would not be compressed
   * The client can find out which columns are/aren't compressed  by looking at the compressorBitmap
   * The column number that's compressed would have it's  respective bit set and vice-versa
   */

  private String compressorInfo;
  private static ColumnCompressor columnCompressorImpl = null;
  /*
   * Compressors that shouldn't be used specified as csv under "hive.resultSet.compressor.disable". 
   * For each column below, the compressor specified by the client is checked for in the list. 
   * If present in the compressorList, that compressor is NOT used
   * and the column is sent as-is
   */
  private HiveConf hiveConf = new HiveConf();
  private String[] listCompressor = this.hiveConf.getVar(
      ConfVars.HIVE_SERVER2_RESULTSET_COMPRESSOR_DISABLE).split(",");
  private List<String> compressorList = Arrays.asList(listCompressor);


  public EncodedColumnBasedSet(TableSchema schema) {
    super(schema);
  }

  public EncodedColumnBasedSet(TRowSet tRowSet) {
    super(tRowSet);
  }

  public EncodedColumnBasedSet(Type[] types, List<Column> subset,
                               long startOffset) {
    super(types, subset, startOffset);
  }

  private static final byte[] MASKS = new byte[] {
    0x01, 0x02, 0x04, 0x08, 0x10, 0x20, 0x40, (byte)0x80
  };

  public static byte[] toBinary(BitSet bitset) {
    byte[] nulls = new byte[1 + (bitset.length() / 8)];
    for (int i = 0; i < bitset.length(); i++) {
      nulls[i / 8] |= bitset.get(i) ? MASKS[i % 8] : 0;
    }
    return nulls;
  }

  /**
   * Given an index, update the TRowSet with the column (uncompressed) 
   * and update the bitmap with that index set to false.
   * @param tRowSet a given TRowSet
   * @param i index for the column which needs to be inserted into TColumns
   * @param bitmap bitmap that needs to be updated with info that column "i" is not compressed
   * 
   */
  
  private void addToUnCompressedData(TRowSet tRowSet, int i, BitSet bitmap) {
    tRowSet.addToColumns(columns.get(i).toTColumn());
    bitmap.set(i, false);
  }
  
  /**
   * Main function that converts the columns in the RowSet if the compressorInfo points to a valid  
   * class and the compressor is not part of the disable compressorList (referred to above)
   *
   */
  @Override
  public TRowSet toTRowSet() {
   

    TRowSet tRowSet = new TRowSet(getStartOffset(), new ArrayList<TRow>());
    ColumnCompressor columnCompressorImpl = null;
    JSONObject jsonContainer = null;
    BitSet compressorBitmap = new BitSet();
    String compressorInfo = hiveConf.get("CompressorInfo");
    if (!hiveConf.getBoolVar(ConfVars.HIVE_SERVER2_RESULTSET_COMPRESSOR_ENABLED) || compressorInfo == null) {
      for (int i = 0; i < columns.size(); i++) {
        addToUnCompressedData(tRowSet, i, compressorBitmap);
      }
    }
    else {
      try {
        jsonContainer = new JSONObject(compressorInfo);
      }
      catch (JSONException e) {
      }

      for (int i = 0; i < columns.size(); i++) {

        String vendor = "";
        String compressorSet = "";
        String entryClass = "";

        try {
          JSONObject jsonObject = jsonContainer.getJSONObject(columns.get(i).getType().toString());
          vendor = jsonObject.getString("vendor");
          compressorSet = jsonObject.getString("compressorSet");
          entryClass = jsonObject.getString("entryClass");
        }  catch (JSONException e) {
          
        }
        String compressorName = vendor + "." + compressorSet + "." + entryClass;
        if (compressorList.contains(compressorName) == true) {
          addToUnCompressedData(tRowSet, i, compressorBitmap);
          continue;
        }

        try {
          columnCompressorImpl = (ColumnCompressor) Class.forName(compressorName).newInstance();
        } catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
          addToUnCompressedData(tRowSet, i, compressorBitmap);
          continue;
        }
        if (columnCompressorImpl.isCompressible(columns.get(i)) == false) {
          addToUnCompressedData(tRowSet, i, compressorBitmap);
          continue;
        }
        TEnColumn tEnColumn = new TEnColumn();
        int size = columns.get(i).size();
        tEnColumn.setSize(size);
        tEnColumn.setType(columns.get(i).getType().toTType());
        tEnColumn.setNulls(columns.get(i).getNulls());
        tEnColumn.setCompressorName(compressorSet);
        compressorBitmap.set(i, true);
        if (size == 0) {
          tEnColumn.setEnData(new byte[0]);
        } else {
          tEnColumn.setEnData(columnCompressorImpl.compress(columns.get(i)));
        }
        tRowSet.addToEnColumns(tEnColumn);
      }
    }
    ByteBuffer bitmap = ByteBuffer.wrap(toBinary(compressorBitmap));
    tRowSet.setCompressorBitmap(bitmap);
    return tRowSet;
  }

  @Override
  public EncodedColumnBasedSet extractSubset(int maxRows) {
    int numRows = Math.min(numRows(), maxRows);

    List<Column> subset = new ArrayList<Column>();
    for (int i = 0; i < columns.size(); i++) {
      subset.add(columns.get(i).extractSubset(0, numRows));
    }
    EncodedColumnBasedSet result = new EncodedColumnBasedSet(types, subset, startOffset);
    startOffset += numRows;
    return result;
  }
  
  public void SetConf(HiveConf conf) {
    this.hiveConf = conf;
  }
}
