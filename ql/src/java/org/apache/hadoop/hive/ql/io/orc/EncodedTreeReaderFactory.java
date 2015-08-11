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
package org.apache.hadoop.hive.ql.io.orc;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hive.common.io.encoded.EncodedColumnBatch;
import org.apache.hadoop.hive.common.io.encoded.EncodedColumnBatch.ColumnStreamData;
import org.apache.hadoop.hive.ql.io.orc.encoded.OrcBatchKey;

/**
 *
 */
public class EncodedTreeReaderFactory extends TreeReaderFactory {

  protected static class TimestampStreamReader extends TimestampTreeReader {
    private boolean isFileCompressed;
    private SettableUncompressedStream _presentStream;
    private SettableUncompressedStream _secondsStream;
    private SettableUncompressedStream _nanosStream;

    private TimestampStreamReader(int columnId, SettableUncompressedStream present,
        SettableUncompressedStream data, SettableUncompressedStream nanos, boolean isFileCompressed,
        OrcProto.ColumnEncoding encoding, boolean skipCorrupt) throws IOException {
      super(columnId, present, data, nanos, encoding, skipCorrupt);
      this.isFileCompressed = isFileCompressed;
      this._presentStream = present;
      this._secondsStream = data;
      this._nanosStream = nanos;
    }

    @Override
    public void seek(PositionProvider index) throws IOException {
      if (present != null) {
        if (isFileCompressed) {
          index.getNext();
        }
        present.seek(index);
      }

      // data stream could be empty stream or already reached end of stream before present stream.
      // This can happen if all values in stream are nulls or last row group values are all null.
      if (_secondsStream.available() > 0) {
        if (isFileCompressed) {
          index.getNext();
        }
        data.seek(index);
      }

      if (_nanosStream.available() > 0) {
        if (isFileCompressed) {
          index.getNext();
        }
        nanos.seek(index);
      }
    }

    @Override
    public void setBuffers(ColumnStreamData[] buffers, boolean sameStripe)
        throws IOException {
      super.setBuffers(buffers, sameStripe);
      if (_presentStream != null) {
        _presentStream.setBuffers(StreamUtils.createDiskRangeInfo(presentStreamBuffer));
      }
      if (_secondsStream != null) {
        _secondsStream.setBuffers(StreamUtils.createDiskRangeInfo(dataStreamBuffer));
      }
      if (_nanosStream != null) {
        _nanosStream.setBuffers(StreamUtils.createDiskRangeInfo(secondaryStreamBuffer));
      }
    }

    public static class StreamReaderBuilder {
      private Long fileId;
      private int columnIndex;
      private ColumnStreamData presentStream;
      private ColumnStreamData dataStream;
      private ColumnStreamData nanosStream;
      private CompressionCodec compressionCodec;
      private OrcProto.ColumnEncoding columnEncoding;
      private boolean skipCorrupt;

      public StreamReaderBuilder setFileId(Long fileId) {
        this.fileId = fileId;
        return this;
      }

      public StreamReaderBuilder setColumnIndex(int columnIndex) {
        this.columnIndex = columnIndex;
        return this;
      }

      public StreamReaderBuilder setPresentStream(ColumnStreamData presentStream) {
        this.presentStream = presentStream;
        return this;
      }

      public StreamReaderBuilder setSecondsStream(ColumnStreamData dataStream) {
        this.dataStream = dataStream;
        return this;
      }

      public StreamReaderBuilder setNanosStream(ColumnStreamData secondaryStream) {
        this.nanosStream = secondaryStream;
        return this;
      }

      public StreamReaderBuilder setCompressionCodec(CompressionCodec compressionCodec) {
        this.compressionCodec = compressionCodec;
        return this;
      }

      public StreamReaderBuilder setColumnEncoding(OrcProto.ColumnEncoding encoding) {
        this.columnEncoding = encoding;
        return this;
      }

      public StreamReaderBuilder skipCorrupt(boolean skipCorrupt) {
        this.skipCorrupt = skipCorrupt;
        return this;
      }

      public TimestampStreamReader build() throws IOException {
        SettableUncompressedStream present = StreamUtils
            .createSettableUncompressedStream(OrcProto.Stream.Kind.PRESENT.name(),
                fileId, presentStream);

        SettableUncompressedStream data = StreamUtils
            .createSettableUncompressedStream(OrcProto.Stream.Kind.DATA.name(), fileId,
                dataStream);

        SettableUncompressedStream nanos = StreamUtils
            .createSettableUncompressedStream(OrcProto.Stream.Kind.SECONDARY.name(),
                fileId, nanosStream);

        boolean isFileCompressed = compressionCodec != null;
        return new TimestampStreamReader(columnIndex, present, data, nanos,
            isFileCompressed, columnEncoding, skipCorrupt);
      }
    }

    public static StreamReaderBuilder builder() {
      return new StreamReaderBuilder();
    }
  }

  protected static class StringStreamReader extends StringTreeReader {
    private boolean _isFileCompressed;
    private boolean _isDictionaryEncoding;
    private SettableUncompressedStream _presentStream;
    private SettableUncompressedStream _dataStream;
    private SettableUncompressedStream _lengthStream;
    private SettableUncompressedStream _dictionaryStream;

    private StringStreamReader(int columnId, SettableUncompressedStream present,
        SettableUncompressedStream data, SettableUncompressedStream length,
        SettableUncompressedStream dictionary,
        boolean isFileCompressed, OrcProto.ColumnEncoding encoding) throws IOException {
      super(columnId, present, data, length, dictionary, encoding);
      this._isDictionaryEncoding = dictionary != null;
      this._isFileCompressed = isFileCompressed;
      this._presentStream = present;
      this._dataStream = data;
      this._lengthStream = length;
      this._dictionaryStream = dictionary;
    }

    @Override
    public void seek(PositionProvider index) throws IOException {
      if (present != null) {
        if (_isFileCompressed) {
          index.getNext();
        }
        reader.present.seek(index);
      }

      if (_isDictionaryEncoding) {
        // DICTIONARY encoding

        // data stream could be empty stream or already reached end of stream before present stream.
        // This can happen if all values in stream are nulls or last row group values are all null.
        if (_dataStream.available() > 0) {
          if (_isFileCompressed) {
            index.getNext();
          }
          ((StringDictionaryTreeReader) reader).reader.seek(index);
        }
      } else {
        // DIRECT encoding

        // data stream could be empty stream or already reached end of stream before present stream.
        // This can happen if all values in stream are nulls or last row group values are all null.
        if (_dataStream.available() > 0) {
          if (_isFileCompressed) {
            index.getNext();
          }
          ((StringDirectTreeReader) reader).stream.seek(index);
        }

        if (_lengthStream.available() > 0) {
          if (_isFileCompressed) {
            index.getNext();
          }
          ((StringDirectTreeReader) reader).lengths.seek(index);
        }
      }
    }

    @Override
    public void setBuffers(ColumnStreamData[] buffers, boolean sameStripe)
        throws IOException {
      super.setBuffers(buffers, sameStripe);
      if (_presentStream != null) {
        _presentStream.setBuffers(StreamUtils.createDiskRangeInfo(presentStreamBuffer));
      }
      if (_dataStream != null) {
        _dataStream.setBuffers(StreamUtils.createDiskRangeInfo(dataStreamBuffer));
      }
      if (!_isDictionaryEncoding) {
        if (_lengthStream != null) {
          _lengthStream.setBuffers(StreamUtils.createDiskRangeInfo(lengthsStreamBuffer));
        }
      }

      // set these streams only if the stripe is different
      if (!sameStripe && _isDictionaryEncoding) {
        if (_lengthStream != null) {
          _lengthStream.setBuffers(StreamUtils.createDiskRangeInfo(lengthsStreamBuffer));
        }
        if (_dictionaryStream != null) {
          _dictionaryStream.setBuffers(StreamUtils.createDiskRangeInfo(dictionaryStreamBuffer));
        }
      }
    }

    public static class StreamReaderBuilder {
      private Long fileId;
      private int columnIndex;
      private ColumnStreamData presentStream;
      private ColumnStreamData dataStream;
      private ColumnStreamData dictionaryStream;
      private ColumnStreamData lengthStream;
      private CompressionCodec compressionCodec;
      private OrcProto.ColumnEncoding columnEncoding;

      public StreamReaderBuilder setFileId(Long fileId) {
        this.fileId = fileId;
        return this;
      }

      public StreamReaderBuilder setColumnIndex(int columnIndex) {
        this.columnIndex = columnIndex;
        return this;
      }

      public StreamReaderBuilder setPresentStream(ColumnStreamData presentStream) {
        this.presentStream = presentStream;
        return this;
      }

      public StreamReaderBuilder setDataStream(ColumnStreamData dataStream) {
        this.dataStream = dataStream;
        return this;
      }

      public StreamReaderBuilder setLengthStream(ColumnStreamData lengthStream) {
        this.lengthStream = lengthStream;
        return this;
      }

      public StreamReaderBuilder setDictionaryStream(ColumnStreamData dictStream) {
        this.dictionaryStream = dictStream;
        return this;
      }

      public StreamReaderBuilder setCompressionCodec(CompressionCodec compressionCodec) {
        this.compressionCodec = compressionCodec;
        return this;
      }

      public StreamReaderBuilder setColumnEncoding(OrcProto.ColumnEncoding encoding) {
        this.columnEncoding = encoding;
        return this;
      }

      public StringStreamReader build() throws IOException {
        SettableUncompressedStream present = StreamUtils
            .createSettableUncompressedStream(OrcProto.Stream.Kind.PRESENT.name(),
                fileId, presentStream);

        SettableUncompressedStream data = StreamUtils
            .createSettableUncompressedStream(OrcProto.Stream.Kind.DATA.name(), fileId,
                dataStream);

        SettableUncompressedStream length = StreamUtils
            .createSettableUncompressedStream(OrcProto.Stream.Kind.LENGTH.name(), fileId,
                lengthStream);

        SettableUncompressedStream dictionary = StreamUtils.createSettableUncompressedStream(
            OrcProto.Stream.Kind.DICTIONARY_DATA.name(), fileId, dictionaryStream);

        boolean isFileCompressed = compressionCodec != null;
        return new StringStreamReader(columnIndex, present, data, length, dictionary,
            isFileCompressed, columnEncoding);
      }
    }

    public static StreamReaderBuilder builder() {
      return new StreamReaderBuilder();
    }

  }

  protected static class ShortStreamReader extends ShortTreeReader {
    private boolean isFileCompressed;
    private SettableUncompressedStream _presentStream;
    private SettableUncompressedStream _dataStream;

    private ShortStreamReader(int columnId, SettableUncompressedStream present,
        SettableUncompressedStream data, boolean isFileCompressed,
        OrcProto.ColumnEncoding encoding) throws IOException {
      super(columnId, present, data, encoding);
      this.isFileCompressed = isFileCompressed;
      this._presentStream = present;
      this._dataStream = data;
    }

    @Override
    public void seek(PositionProvider index) throws IOException {
      if (present != null) {
        if (isFileCompressed) {
          index.getNext();
        }
        present.seek(index);
      }

      // data stream could be empty stream or already reached end of stream before present stream.
      // This can happen if all values in stream are nulls or last row group values are all null.
      if (_dataStream.available() > 0) {
        if (isFileCompressed) {
          index.getNext();
        }
        reader.seek(index);
      }
    }

    @Override
    public void setBuffers(ColumnStreamData[] buffers, boolean sameStripe)
        throws IOException {
      super.setBuffers(buffers, sameStripe);
      if (_presentStream != null) {
        _presentStream.setBuffers(StreamUtils.createDiskRangeInfo(presentStreamBuffer));
      }
      if (_dataStream != null) {
        _dataStream.setBuffers(StreamUtils.createDiskRangeInfo(dataStreamBuffer));
      }
    }

    public static class StreamReaderBuilder {
      private Long fileId;
      private int columnIndex;
      private ColumnStreamData presentStream;
      private ColumnStreamData dataStream;
      private CompressionCodec compressionCodec;
      private OrcProto.ColumnEncoding columnEncoding;

      public StreamReaderBuilder setFileId(Long fileId) {
        this.fileId = fileId;
        return this;
      }

      public StreamReaderBuilder setColumnIndex(int columnIndex) {
        this.columnIndex = columnIndex;
        return this;
      }

      public StreamReaderBuilder setPresentStream(ColumnStreamData presentStream) {
        this.presentStream = presentStream;
        return this;
      }

      public StreamReaderBuilder setDataStream(ColumnStreamData dataStream) {
        this.dataStream = dataStream;
        return this;
      }

      public StreamReaderBuilder setCompressionCodec(CompressionCodec compressionCodec) {
        this.compressionCodec = compressionCodec;
        return this;
      }

      public StreamReaderBuilder setColumnEncoding(OrcProto.ColumnEncoding encoding) {
        this.columnEncoding = encoding;
        return this;
      }

      public ShortStreamReader build() throws IOException {
        SettableUncompressedStream present = StreamUtils
            .createSettableUncompressedStream(OrcProto.Stream.Kind.PRESENT.name(),
                fileId, presentStream);

        SettableUncompressedStream data = StreamUtils
            .createSettableUncompressedStream(OrcProto.Stream.Kind.DATA.name(), fileId,
                dataStream);

        boolean isFileCompressed = compressionCodec != null;
        return new ShortStreamReader(columnIndex, present, data, isFileCompressed,
            columnEncoding);
      }
    }

    public static StreamReaderBuilder builder() {
      return new StreamReaderBuilder();
    }
  }

  protected static class LongStreamReader extends LongTreeReader {
    private boolean _isFileCompressed;
    private SettableUncompressedStream _presentStream;
    private SettableUncompressedStream _dataStream;

    private LongStreamReader(int columnId, SettableUncompressedStream present,
        SettableUncompressedStream data, boolean isFileCompressed,
        OrcProto.ColumnEncoding encoding, boolean skipCorrupt) throws IOException {
      super(columnId, present, data, encoding, skipCorrupt);
      this._isFileCompressed = isFileCompressed;
      this._presentStream = present;
      this._dataStream = data;
    }

    @Override
    public void seek(PositionProvider index) throws IOException {
      if (present != null) {
        if (_isFileCompressed) {
          index.getNext();
        }
        present.seek(index);
      }

      // data stream could be empty stream or already reached end of stream before present stream.
      // This can happen if all values in stream are nulls or last row group values are all null.
      if (_dataStream.available() > 0) {
        if (_isFileCompressed) {
          index.getNext();
        }
        reader.seek(index);
      }
    }

    @Override
    public void setBuffers(ColumnStreamData[] buffers, boolean sameStripe)
        throws IOException {
      super.setBuffers(buffers, sameStripe);
      if (_presentStream != null) {
        _presentStream.setBuffers(StreamUtils.createDiskRangeInfo(presentStreamBuffer));
      }
      if (_dataStream != null) {
        _dataStream.setBuffers(StreamUtils.createDiskRangeInfo(dataStreamBuffer));
      }
    }

    public static class StreamReaderBuilder {
      private Long fileId;
      private int columnIndex;
      private ColumnStreamData presentStream;
      private ColumnStreamData dataStream;
      private CompressionCodec compressionCodec;
      private OrcProto.ColumnEncoding columnEncoding;
      private boolean skipCorrupt;

      public StreamReaderBuilder setFileId(Long fileId) {
        this.fileId = fileId;
        return this;
      }

      public StreamReaderBuilder setColumnIndex(int columnIndex) {
        this.columnIndex = columnIndex;
        return this;
      }

      public StreamReaderBuilder setPresentStream(ColumnStreamData presentStream) {
        this.presentStream = presentStream;
        return this;
      }

      public StreamReaderBuilder setDataStream(ColumnStreamData dataStream) {
        this.dataStream = dataStream;
        return this;
      }

      public StreamReaderBuilder setCompressionCodec(CompressionCodec compressionCodec) {
        this.compressionCodec = compressionCodec;
        return this;
      }

      public StreamReaderBuilder setColumnEncoding(OrcProto.ColumnEncoding encoding) {
        this.columnEncoding = encoding;
        return this;
      }

      public StreamReaderBuilder skipCorrupt(boolean skipCorrupt) {
        this.skipCorrupt = skipCorrupt;
        return this;
      }

      public LongStreamReader build() throws IOException {
        SettableUncompressedStream present = StreamUtils
            .createSettableUncompressedStream(OrcProto.Stream.Kind.PRESENT.name(),
                fileId, presentStream);

        SettableUncompressedStream data = StreamUtils
            .createSettableUncompressedStream(OrcProto.Stream.Kind.DATA.name(), fileId,
                dataStream);

        boolean isFileCompressed = compressionCodec != null;
        return new LongStreamReader(columnIndex, present, data, isFileCompressed,
            columnEncoding, skipCorrupt);
      }
    }

    public static StreamReaderBuilder builder() {
      return new StreamReaderBuilder();
    }
  }

  protected static class IntStreamReader extends IntTreeReader {
    private boolean _isFileCompressed;
    private SettableUncompressedStream _presentStream;
    private SettableUncompressedStream _dataStream;

    private IntStreamReader(int columnId, SettableUncompressedStream present,
        SettableUncompressedStream data, boolean isFileCompressed,
        OrcProto.ColumnEncoding encoding) throws IOException {
      super(columnId, present, data, encoding);
      this._isFileCompressed = isFileCompressed;
      this._dataStream = data;
      this._presentStream = present;
    }

    @Override
    public void seek(PositionProvider index) throws IOException {
      if (present != null) {
        if (_isFileCompressed) {
          index.getNext();
        }
        present.seek(index);
      }

      // data stream could be empty stream or already reached end of stream before present stream.
      // This can happen if all values in stream are nulls or last row group values are all null.
      if (_dataStream.available() > 0) {
        if (_isFileCompressed) {
          index.getNext();
        }
        reader.seek(index);
      }
    }

    @Override
    public void setBuffers(ColumnStreamData[] buffers, boolean sameStripe)
        throws IOException {
      super.setBuffers(buffers, sameStripe);
      if (_presentStream != null) {
        _presentStream.setBuffers(StreamUtils.createDiskRangeInfo(presentStreamBuffer));
      }
      if (_dataStream != null) {
        _dataStream.setBuffers(StreamUtils.createDiskRangeInfo(dataStreamBuffer));
      }
    }

    public static class StreamReaderBuilder {
      private Long fileId;
      private int columnIndex;
      private ColumnStreamData presentStream;
      private ColumnStreamData dataStream;
      private CompressionCodec compressionCodec;
      private OrcProto.ColumnEncoding columnEncoding;

      public StreamReaderBuilder setFileId(Long fileId) {
        this.fileId = fileId;
        return this;
      }

      public StreamReaderBuilder setColumnIndex(int columnIndex) {
        this.columnIndex = columnIndex;
        return this;
      }

      public StreamReaderBuilder setPresentStream(ColumnStreamData presentStream) {
        this.presentStream = presentStream;
        return this;
      }

      public StreamReaderBuilder setDataStream(ColumnStreamData dataStream) {
        this.dataStream = dataStream;
        return this;
      }

      public StreamReaderBuilder setCompressionCodec(CompressionCodec compressionCodec) {
        this.compressionCodec = compressionCodec;
        return this;
      }

      public StreamReaderBuilder setColumnEncoding(OrcProto.ColumnEncoding encoding) {
        this.columnEncoding = encoding;
        return this;
      }

      public IntStreamReader build() throws IOException {
        SettableUncompressedStream present = StreamUtils
            .createSettableUncompressedStream(OrcProto.Stream.Kind.PRESENT.name(),
                fileId, presentStream);

        SettableUncompressedStream data = StreamUtils
            .createSettableUncompressedStream(OrcProto.Stream.Kind.DATA.name(), fileId,
                dataStream);

        boolean isFileCompressed = compressionCodec != null;
        return new IntStreamReader(columnIndex, present, data, isFileCompressed,
            columnEncoding);
      }
    }

    public static StreamReaderBuilder builder() {
      return new StreamReaderBuilder();
    }

  }

  protected static class FloatStreamReader extends FloatTreeReader {
    private boolean _isFileCompressed;
    private SettableUncompressedStream _presentStream;
    private SettableUncompressedStream _dataStream;

    private FloatStreamReader(int columnId, SettableUncompressedStream present,
        SettableUncompressedStream data, boolean isFileCompressed) throws IOException {
      super(columnId, present, data);
      this._isFileCompressed = isFileCompressed;
      this._presentStream = present;
      this._dataStream = data;
    }

    @Override
    public void seek(PositionProvider index) throws IOException {
      if (present != null) {
        if (_isFileCompressed) {
          index.getNext();
        }
        present.seek(index);
      }

      // data stream could be empty stream or already reached end of stream before present stream.
      // This can happen if all values in stream are nulls or last row group values are all null.
      if (_dataStream.available() > 0) {
        if (_isFileCompressed) {
          index.getNext();
        }
        stream.seek(index);
      }
    }

    @Override
    public void setBuffers(ColumnStreamData[] buffers, boolean sameStripe)
        throws IOException {
      super.setBuffers(buffers, sameStripe);
      if (_presentStream != null) {
        _presentStream.setBuffers(StreamUtils.createDiskRangeInfo(presentStreamBuffer));
      }
      if (_dataStream != null) {
        _dataStream.setBuffers(StreamUtils.createDiskRangeInfo(dataStreamBuffer));
      }
    }

    public static class StreamReaderBuilder {
      private Long fileId;
      private int columnIndex;
      private ColumnStreamData presentStream;
      private ColumnStreamData dataStream;
      private CompressionCodec compressionCodec;

      public StreamReaderBuilder setFileId(Long fileId) {
        this.fileId = fileId;
        return this;
      }

      public StreamReaderBuilder setColumnIndex(int columnIndex) {
        this.columnIndex = columnIndex;
        return this;
      }

      public StreamReaderBuilder setPresentStream(ColumnStreamData presentStream) {
        this.presentStream = presentStream;
        return this;
      }

      public StreamReaderBuilder setDataStream(ColumnStreamData dataStream) {
        this.dataStream = dataStream;
        return this;
      }

      public StreamReaderBuilder setCompressionCodec(CompressionCodec compressionCodec) {
        this.compressionCodec = compressionCodec;
        return this;
      }

      public FloatStreamReader build() throws IOException {
        SettableUncompressedStream present = StreamUtils
            .createSettableUncompressedStream(OrcProto.Stream.Kind.PRESENT.name(),
                fileId, presentStream);

        SettableUncompressedStream data = StreamUtils
            .createSettableUncompressedStream(OrcProto.Stream.Kind.DATA.name(), fileId,
                dataStream);

        boolean isFileCompressed = compressionCodec != null;
        return new FloatStreamReader(columnIndex, present, data, isFileCompressed);
      }
    }

    public static StreamReaderBuilder builder() {
      return new StreamReaderBuilder();
    }

  }

  protected static class DoubleStreamReader extends DoubleTreeReader {
    private boolean _isFileCompressed;
    private SettableUncompressedStream _presentStream;
    private SettableUncompressedStream _dataStream;

    private DoubleStreamReader(int columnId, SettableUncompressedStream present,
        SettableUncompressedStream data, boolean isFileCompressed) throws IOException {
      super(columnId, present, data);
      this._isFileCompressed = isFileCompressed;
      this._presentStream = present;
      this._dataStream = data;
    }

    @Override
    public void seek(PositionProvider index) throws IOException {
      if (present != null) {
        if (_isFileCompressed) {
          index.getNext();
        }
        present.seek(index);
      }

      // data stream could be empty stream or already reached end of stream before present stream.
      // This can happen if all values in stream are nulls or last row group values are all null.
      if (_dataStream.available() > 0) {
        if (_isFileCompressed) {
          index.getNext();
        }
        stream.seek(index);
      }
    }

    @Override
    public void setBuffers(ColumnStreamData[] buffers, boolean sameStripe)
        throws IOException {
      super.setBuffers(buffers, sameStripe);
      if (_presentStream != null) {
        _presentStream.setBuffers(StreamUtils.createDiskRangeInfo(presentStreamBuffer));
      }
      if (_dataStream != null) {
        _dataStream.setBuffers(StreamUtils.createDiskRangeInfo(dataStreamBuffer));
      }
    }

    public static class StreamReaderBuilder {
      private Long fileId;
      private int columnIndex;
      private ColumnStreamData presentStream;
      private ColumnStreamData dataStream;
      private CompressionCodec compressionCodec;

      public StreamReaderBuilder setFileId(Long fileId) {
        this.fileId = fileId;
        return this;
      }

      public StreamReaderBuilder setColumnIndex(int columnIndex) {
        this.columnIndex = columnIndex;
        return this;
      }

      public StreamReaderBuilder setPresentStream(ColumnStreamData presentStream) {
        this.presentStream = presentStream;
        return this;
      }

      public StreamReaderBuilder setDataStream(ColumnStreamData dataStream) {
        this.dataStream = dataStream;
        return this;
      }

      public StreamReaderBuilder setCompressionCodec(CompressionCodec compressionCodec) {
        this.compressionCodec = compressionCodec;
        return this;
      }

      public DoubleStreamReader build() throws IOException {
        SettableUncompressedStream present = StreamUtils
            .createSettableUncompressedStream(OrcProto.Stream.Kind.PRESENT.name(),
                fileId, presentStream);

        SettableUncompressedStream data = StreamUtils
            .createSettableUncompressedStream(OrcProto.Stream.Kind.DATA.name(), fileId,
                dataStream);

        boolean isFileCompressed = compressionCodec != null;
        return new DoubleStreamReader(columnIndex, present, data, isFileCompressed);
      }
    }

    public static StreamReaderBuilder builder() {
      return new StreamReaderBuilder();
    }
  }

  protected static class DecimalStreamReader extends DecimalTreeReader {
    private boolean _isFileCompressed;
    private SettableUncompressedStream _presentStream;
    private SettableUncompressedStream _valueStream;
    private SettableUncompressedStream _scaleStream;

    private DecimalStreamReader(int columnId, int precision, int scale,
        SettableUncompressedStream presentStream,
        SettableUncompressedStream valueStream, SettableUncompressedStream scaleStream,
        boolean isFileCompressed,
        OrcProto.ColumnEncoding encoding) throws IOException {
      super(columnId, precision, scale, presentStream, valueStream, scaleStream, encoding);
      this._isFileCompressed = isFileCompressed;
      this._presentStream = presentStream;
      this._valueStream = valueStream;
      this._scaleStream = scaleStream;
    }

    @Override
    public void seek(PositionProvider index) throws IOException {
      if (present != null) {
        if (_isFileCompressed) {
          index.getNext();
        }
        present.seek(index);
      }

      // data stream could be empty stream or already reached end of stream before present stream.
      // This can happen if all values in stream are nulls or last row group values are all null.
      if (_valueStream.available() > 0) {
        if (_isFileCompressed) {
          index.getNext();
        }
        valueStream.seek(index);
      }

      if (_scaleStream.available() > 0) {
        if (_isFileCompressed) {
          index.getNext();
        }
        scaleReader.seek(index);
      }
    }

    @Override
    public void setBuffers(ColumnStreamData[] buffers, boolean sameStripe)
        throws IOException {
      super.setBuffers(buffers, sameStripe);
      if (_presentStream != null) {
        _presentStream.setBuffers(StreamUtils.createDiskRangeInfo(presentStreamBuffer));
      }
      if (_valueStream != null) {
        _valueStream.setBuffers(StreamUtils.createDiskRangeInfo(dataStreamBuffer));
      }
      if (_scaleStream != null) {
        _scaleStream.setBuffers(StreamUtils.createDiskRangeInfo(secondaryStreamBuffer));
      }
    }

    public static class StreamReaderBuilder {
      private Long fileId;
      private int columnIndex;
      private ColumnStreamData presentStream;
      private ColumnStreamData valueStream;
      private ColumnStreamData scaleStream;
      private int scale;
      private int precision;
      private CompressionCodec compressionCodec;
      private OrcProto.ColumnEncoding columnEncoding;

      public StreamReaderBuilder setFileId(Long fileId) {
        this.fileId = fileId;
        return this;
      }

      public StreamReaderBuilder setColumnIndex(int columnIndex) {
        this.columnIndex = columnIndex;
        return this;
      }

      public StreamReaderBuilder setPrecision(int precision) {
        this.precision = precision;
        return this;
      }

      public StreamReaderBuilder setScale(int scale) {
        this.scale = scale;
        return this;
      }

      public StreamReaderBuilder setPresentStream(ColumnStreamData presentStream) {
        this.presentStream = presentStream;
        return this;
      }

      public StreamReaderBuilder setValueStream(ColumnStreamData valueStream) {
        this.valueStream = valueStream;
        return this;
      }

      public StreamReaderBuilder setScaleStream(ColumnStreamData scaleStream) {
        this.scaleStream = scaleStream;
        return this;
      }

      public StreamReaderBuilder setCompressionCodec(CompressionCodec compressionCodec) {
        this.compressionCodec = compressionCodec;
        return this;
      }

      public StreamReaderBuilder setColumnEncoding(OrcProto.ColumnEncoding encoding) {
        this.columnEncoding = encoding;
        return this;
      }

      public DecimalStreamReader build() throws IOException {
        SettableUncompressedStream presentInStream = StreamUtils.createSettableUncompressedStream(
            OrcProto.Stream.Kind.PRESENT.name(), fileId, presentStream);

        SettableUncompressedStream valueInStream = StreamUtils.createSettableUncompressedStream(
            OrcProto.Stream.Kind.DATA.name(), fileId, valueStream);

        SettableUncompressedStream scaleInStream = StreamUtils.createSettableUncompressedStream(
            OrcProto.Stream.Kind.SECONDARY.name(), fileId, scaleStream);

        boolean isFileCompressed = compressionCodec != null;
        return new DecimalStreamReader(columnIndex, precision, scale, presentInStream,
            valueInStream,
            scaleInStream, isFileCompressed, columnEncoding);
      }
    }

    public static StreamReaderBuilder builder() {
      return new StreamReaderBuilder();
    }
  }

  protected static class DateStreamReader extends DateTreeReader {
    private boolean isFileCompressed;
    private SettableUncompressedStream _presentStream;
    private SettableUncompressedStream _dataStream;

    private DateStreamReader(int columnId, SettableUncompressedStream present,
        SettableUncompressedStream data, boolean isFileCompressed,
        OrcProto.ColumnEncoding encoding) throws IOException {
      super(columnId, present, data, encoding);
      this.isFileCompressed = isFileCompressed;
      this._presentStream = present;
      this._dataStream = data;
    }

    @Override
    public void seek(PositionProvider index) throws IOException {
      if (present != null) {
        if (isFileCompressed) {
          index.getNext();
        }
        present.seek(index);
      }

      // data stream could be empty stream or already reached end of stream before present stream.
      // This can happen if all values in stream are nulls or last row group values are all null.
      if (_dataStream.available() > 0) {
        if (isFileCompressed) {
          index.getNext();
        }
        reader.seek(index);
      }
    }

    @Override
    public void setBuffers(ColumnStreamData[] buffers, boolean sameStripe)
        throws IOException {
      super.setBuffers(buffers, sameStripe);
      if (_presentStream != null) {
        _presentStream.setBuffers(StreamUtils.createDiskRangeInfo(presentStreamBuffer));
      }
      if (_dataStream != null) {
        _dataStream.setBuffers(StreamUtils.createDiskRangeInfo(dataStreamBuffer));
      }
    }

    public static class StreamReaderBuilder {
      private Long fileId;
      private int columnIndex;
      private ColumnStreamData presentStream;
      private ColumnStreamData dataStream;
      private CompressionCodec compressionCodec;
      private OrcProto.ColumnEncoding columnEncoding;

      public StreamReaderBuilder setFileId(Long fileId) {
        this.fileId = fileId;
        return this;
      }

      public StreamReaderBuilder setColumnIndex(int columnIndex) {
        this.columnIndex = columnIndex;
        return this;
      }

      public StreamReaderBuilder setPresentStream(ColumnStreamData presentStream) {
        this.presentStream = presentStream;
        return this;
      }

      public StreamReaderBuilder setDataStream(ColumnStreamData dataStream) {
        this.dataStream = dataStream;
        return this;
      }

      public StreamReaderBuilder setCompressionCodec(CompressionCodec compressionCodec) {
        this.compressionCodec = compressionCodec;
        return this;
      }

      public StreamReaderBuilder setColumnEncoding(OrcProto.ColumnEncoding encoding) {
        this.columnEncoding = encoding;
        return this;
      }

      public DateStreamReader build() throws IOException {
        SettableUncompressedStream present = StreamUtils
            .createSettableUncompressedStream(OrcProto.Stream.Kind.PRESENT.name(),
                fileId, presentStream);


        SettableUncompressedStream data = StreamUtils
            .createSettableUncompressedStream(OrcProto.Stream.Kind.DATA.name(), fileId,
                dataStream);

        boolean isFileCompressed = compressionCodec != null;
        return new DateStreamReader(columnIndex, present, data, isFileCompressed,
            columnEncoding);
      }
    }

    public static StreamReaderBuilder builder() {
      return new StreamReaderBuilder();
    }
  }

  protected static class CharStreamReader extends CharTreeReader {
    private boolean _isFileCompressed;
    private boolean _isDictionaryEncoding;
    private SettableUncompressedStream _presentStream;
    private SettableUncompressedStream _dataStream;
    private SettableUncompressedStream _lengthStream;
    private SettableUncompressedStream _dictionaryStream;

    private CharStreamReader(int columnId, int maxLength,
        SettableUncompressedStream present, SettableUncompressedStream data,
        SettableUncompressedStream length, SettableUncompressedStream dictionary,
        boolean isFileCompressed, OrcProto.ColumnEncoding encoding) throws IOException {
      super(columnId, maxLength, present, data, length,
          dictionary, encoding);
      this._isDictionaryEncoding = dictionary != null;
      this._isFileCompressed = isFileCompressed;
      this._presentStream = present;
      this._dataStream = data;
      this._lengthStream = length;
      this._dictionaryStream = dictionary;
    }

    @Override
    public void seek(PositionProvider index) throws IOException {
      if (present != null) {
        if (_isFileCompressed) {
          index.getNext();
        }
        reader.present.seek(index);
      }

      if (_isDictionaryEncoding) {
        // DICTIONARY encoding

        // data stream could be empty stream or already reached end of stream before present stream.
        // This can happen if all values in stream are nulls or last row group values are all null.
        if (_dataStream.available() > 0) {
          if (_isFileCompressed) {
            index.getNext();
          }
          ((StringDictionaryTreeReader) reader).reader.seek(index);
        }
      } else {
        // DIRECT encoding

        // data stream could be empty stream or already reached end of stream before present stream.
        // This can happen if all values in stream are nulls or last row group values are all null.
        if (_dataStream.available() > 0) {
          if (_isFileCompressed) {
            index.getNext();
          }
          ((StringDirectTreeReader) reader).stream.seek(index);
        }

        if (_lengthStream.available() > 0) {
          if (_isFileCompressed) {
            index.getNext();
          }
          ((StringDirectTreeReader) reader).lengths.seek(index);
        }
      }
    }

    @Override
    public void setBuffers(ColumnStreamData[] buffers, boolean sameStripe)
        throws IOException {
      super.setBuffers(buffers, sameStripe);
      if (_presentStream != null) {
        _presentStream.setBuffers(StreamUtils.createDiskRangeInfo(presentStreamBuffer));
      }
      if (_dataStream != null) {
        _dataStream.setBuffers(StreamUtils.createDiskRangeInfo(dataStreamBuffer));
      }
      if (!_isDictionaryEncoding) {
        if (_lengthStream != null) {
          _lengthStream.setBuffers(StreamUtils.createDiskRangeInfo(lengthsStreamBuffer));
        }
      }

      // set these streams only if the stripe is different
      if (!sameStripe && _isDictionaryEncoding) {
        if (_lengthStream != null) {
          _lengthStream.setBuffers(StreamUtils.createDiskRangeInfo(lengthsStreamBuffer));
        }
        if (_dictionaryStream != null) {
          _dictionaryStream.setBuffers(StreamUtils.createDiskRangeInfo(dictionaryStreamBuffer));
        }
      }
    }

    public static class StreamReaderBuilder {
      private Long fileId;
      private int columnIndex;
      private int maxLength;
      private ColumnStreamData presentStream;
      private ColumnStreamData dataStream;
      private ColumnStreamData dictionaryStream;
      private ColumnStreamData lengthStream;
      private CompressionCodec compressionCodec;
      private OrcProto.ColumnEncoding columnEncoding;

      public StreamReaderBuilder setFileId(Long fileId) {
        this.fileId = fileId;
        return this;
      }

      public StreamReaderBuilder setColumnIndex(int columnIndex) {
        this.columnIndex = columnIndex;
        return this;
      }

      public StreamReaderBuilder setMaxLength(int maxLength) {
        this.maxLength = maxLength;
        return this;
      }

      public StreamReaderBuilder setPresentStream(ColumnStreamData presentStream) {
        this.presentStream = presentStream;
        return this;
      }

      public StreamReaderBuilder setDataStream(ColumnStreamData dataStream) {
        this.dataStream = dataStream;
        return this;
      }

      public StreamReaderBuilder setLengthStream(ColumnStreamData lengthStream) {
        this.lengthStream = lengthStream;
        return this;
      }

      public StreamReaderBuilder setDictionaryStream(ColumnStreamData dictStream) {
        this.dictionaryStream = dictStream;
        return this;
      }

      public StreamReaderBuilder setCompressionCodec(CompressionCodec compressionCodec) {
        this.compressionCodec = compressionCodec;
        return this;
      }

      public StreamReaderBuilder setColumnEncoding(OrcProto.ColumnEncoding encoding) {
        this.columnEncoding = encoding;
        return this;
      }

      public CharStreamReader build() throws IOException {
        SettableUncompressedStream present = StreamUtils
            .createSettableUncompressedStream(OrcProto.Stream.Kind.PRESENT.name(),
                fileId, presentStream);

        SettableUncompressedStream data = StreamUtils
            .createSettableUncompressedStream(OrcProto.Stream.Kind.DATA.name(), fileId,
                dataStream);

        SettableUncompressedStream length = StreamUtils
            .createSettableUncompressedStream(OrcProto.Stream.Kind.LENGTH.name(), fileId,
                lengthStream);

        SettableUncompressedStream dictionary = StreamUtils.createSettableUncompressedStream(
            OrcProto.Stream.Kind.DICTIONARY_DATA.name(), fileId, dictionaryStream);

        boolean isFileCompressed = compressionCodec != null;
        return new CharStreamReader(columnIndex, maxLength, present, data, length,
            dictionary, isFileCompressed, columnEncoding);
      }
    }

    public static StreamReaderBuilder builder() {
      return new StreamReaderBuilder();
    }

  }

  protected static class VarcharStreamReader extends VarcharTreeReader {
    private boolean _isFileCompressed;
    private boolean _isDictionaryEncoding;
    private SettableUncompressedStream _presentStream;
    private SettableUncompressedStream _dataStream;
    private SettableUncompressedStream _lengthStream;
    private SettableUncompressedStream _dictionaryStream;

    private VarcharStreamReader(int columnId, int maxLength,
        SettableUncompressedStream present, SettableUncompressedStream data,
        SettableUncompressedStream length, SettableUncompressedStream dictionary,
        boolean isFileCompressed, OrcProto.ColumnEncoding encoding) throws IOException {
      super(columnId, maxLength, present, data, length,
          dictionary, encoding);
      this._isDictionaryEncoding = dictionary != null;
      this._isFileCompressed = isFileCompressed;
      this._presentStream = present;
      this._dataStream = data;
      this._lengthStream = length;
      this._dictionaryStream = dictionary;
    }

    @Override
    public void seek(PositionProvider index) throws IOException {
      if (present != null) {
        if (_isFileCompressed) {
          index.getNext();
        }
        reader.present.seek(index);
      }

      if (_isDictionaryEncoding) {
        // DICTIONARY encoding

        // data stream could be empty stream or already reached end of stream before present stream.
        // This can happen if all values in stream are nulls or last row group values are all null.
        if (_dataStream.available() > 0) {
          if (_isFileCompressed) {
            index.getNext();
          }
          ((StringDictionaryTreeReader) reader).reader.seek(index);
        }
      } else {
        // DIRECT encoding

        // data stream could be empty stream or already reached end of stream before present stream.
        // This can happen if all values in stream are nulls or last row group values are all null.
        if (_dataStream.available() > 0) {
          if (_isFileCompressed) {
            index.getNext();
          }
          ((StringDirectTreeReader) reader).stream.seek(index);
        }

        if (_lengthStream.available() > 0) {
          if (_isFileCompressed) {
            index.getNext();
          }
          ((StringDirectTreeReader) reader).lengths.seek(index);
        }
      }
    }

    @Override
    public void setBuffers(ColumnStreamData[] buffers, boolean sameStripe)
        throws IOException {
      super.setBuffers(buffers, sameStripe);
      if (_presentStream != null) {
        _presentStream.setBuffers(StreamUtils.createDiskRangeInfo(presentStreamBuffer));
      }
      if (_dataStream != null) {
        _dataStream.setBuffers(StreamUtils.createDiskRangeInfo(dataStreamBuffer));
      }
      if (!_isDictionaryEncoding) {
        if (_lengthStream != null) {
          _lengthStream.setBuffers(StreamUtils.createDiskRangeInfo(lengthsStreamBuffer));
        }
      }

      // set these streams only if the stripe is different
      if (!sameStripe && _isDictionaryEncoding) {
        if (_lengthStream != null) {
          _lengthStream.setBuffers(StreamUtils.createDiskRangeInfo(lengthsStreamBuffer));
        }
        if (_dictionaryStream != null) {
          _dictionaryStream.setBuffers(StreamUtils.createDiskRangeInfo(dictionaryStreamBuffer));
        }
      }
    }

    public static class StreamReaderBuilder {
      private Long fileId;
      private int columnIndex;
      private int maxLength;
      private ColumnStreamData presentStream;
      private ColumnStreamData dataStream;
      private ColumnStreamData dictionaryStream;
      private ColumnStreamData lengthStream;
      private CompressionCodec compressionCodec;
      private OrcProto.ColumnEncoding columnEncoding;

      public StreamReaderBuilder setFileId(Long fileId) {
        this.fileId = fileId;
        return this;
      }

      public StreamReaderBuilder setColumnIndex(int columnIndex) {
        this.columnIndex = columnIndex;
        return this;
      }

      public StreamReaderBuilder setMaxLength(int maxLength) {
        this.maxLength = maxLength;
        return this;
      }

      public StreamReaderBuilder setPresentStream(ColumnStreamData presentStream) {
        this.presentStream = presentStream;
        return this;
      }

      public StreamReaderBuilder setDataStream(ColumnStreamData dataStream) {
        this.dataStream = dataStream;
        return this;
      }

      public StreamReaderBuilder setLengthStream(ColumnStreamData lengthStream) {
        this.lengthStream = lengthStream;
        return this;
      }

      public StreamReaderBuilder setDictionaryStream(ColumnStreamData dictStream) {
        this.dictionaryStream = dictStream;
        return this;
      }

      public StreamReaderBuilder setCompressionCodec(CompressionCodec compressionCodec) {
        this.compressionCodec = compressionCodec;
        return this;
      }

      public StreamReaderBuilder setColumnEncoding(OrcProto.ColumnEncoding encoding) {
        this.columnEncoding = encoding;
        return this;
      }

      public VarcharStreamReader build() throws IOException {
        SettableUncompressedStream present = StreamUtils
            .createSettableUncompressedStream(OrcProto.Stream.Kind.PRESENT.name(),
                fileId, presentStream);

        SettableUncompressedStream data = StreamUtils
            .createSettableUncompressedStream(OrcProto.Stream.Kind.DATA.name(), fileId,
                dataStream);

        SettableUncompressedStream length = StreamUtils
            .createSettableUncompressedStream(OrcProto.Stream.Kind.LENGTH.name(), fileId,
                lengthStream);

        SettableUncompressedStream dictionary = StreamUtils.createSettableUncompressedStream(
            OrcProto.Stream.Kind.DICTIONARY_DATA.name(), fileId, dictionaryStream);

        boolean isFileCompressed = compressionCodec != null;
        return new VarcharStreamReader(columnIndex, maxLength, present, data, length,
            dictionary, isFileCompressed, columnEncoding);
      }
    }

    public static StreamReaderBuilder builder() {
      return new StreamReaderBuilder();
    }

  }

  protected static class ByteStreamReader extends ByteTreeReader {
    private boolean _isFileCompressed;
    private SettableUncompressedStream _presentStream;
    private SettableUncompressedStream _dataStream;

    private ByteStreamReader(int columnId, SettableUncompressedStream present,
        SettableUncompressedStream data, boolean isFileCompressed) throws IOException {
      super(columnId, present, data);
      this._isFileCompressed = isFileCompressed;
      this._presentStream = present;
      this._dataStream = data;
    }

    @Override
    public void seek(PositionProvider index) throws IOException {
      if (present != null) {
        if (_isFileCompressed) {
          index.getNext();
        }
        present.seek(index);
      }

      // data stream could be empty stream or already reached end of stream before present stream.
      // This can happen if all values in stream are nulls or last row group values are all null.
      if (_dataStream.available() > 0) {
        if (_isFileCompressed) {
          index.getNext();
        }
        reader.seek(index);
      }
    }

    @Override
    public void setBuffers(ColumnStreamData[] buffers, boolean sameStripe)
        throws IOException {
      super.setBuffers(buffers, sameStripe);
      if (_presentStream != null) {
        _presentStream.setBuffers(StreamUtils.createDiskRangeInfo(presentStreamBuffer));
      }
      if (_dataStream != null) {
        _dataStream.setBuffers(StreamUtils.createDiskRangeInfo(dataStreamBuffer));
      }
    }

    public static class StreamReaderBuilder {
      private Long fileId;
      private int columnIndex;
      private ColumnStreamData presentStream;
      private ColumnStreamData dataStream;
      private CompressionCodec compressionCodec;

      public StreamReaderBuilder setFileId(Long fileId) {
        this.fileId = fileId;
        return this;
      }

      public StreamReaderBuilder setColumnIndex(int columnIndex) {
        this.columnIndex = columnIndex;
        return this;
      }

      public StreamReaderBuilder setPresentStream(ColumnStreamData presentStream) {
        this.presentStream = presentStream;
        return this;
      }

      public StreamReaderBuilder setDataStream(ColumnStreamData dataStream) {
        this.dataStream = dataStream;
        return this;
      }

      public StreamReaderBuilder setCompressionCodec(CompressionCodec compressionCodec) {
        this.compressionCodec = compressionCodec;
        return this;
      }

      public ByteStreamReader build() throws IOException {
        SettableUncompressedStream present = StreamUtils
            .createSettableUncompressedStream(OrcProto.Stream.Kind.PRESENT.name(),
                fileId, presentStream);

        SettableUncompressedStream data = StreamUtils
            .createSettableUncompressedStream(OrcProto.Stream.Kind.DATA.name(), fileId,
                dataStream);

        boolean isFileCompressed = compressionCodec != null;
        return new ByteStreamReader(columnIndex, present, data, isFileCompressed);
      }
    }

    public static StreamReaderBuilder builder() {
      return new StreamReaderBuilder();
    }
  }

  protected static class BinaryStreamReader extends BinaryTreeReader {
    private boolean _isFileCompressed;
    private SettableUncompressedStream _presentStream;
    private SettableUncompressedStream _dataStream;
    private SettableUncompressedStream _lengthsStream;

    private BinaryStreamReader(int columnId, SettableUncompressedStream present,
        SettableUncompressedStream data, SettableUncompressedStream length,
        boolean isFileCompressed,
        OrcProto.ColumnEncoding encoding) throws IOException {
      super(columnId, present, data, length, encoding);
      this._isFileCompressed = isFileCompressed;
      this._presentStream = present;
      this._dataStream = data;
      this._lengthsStream = length;
    }

    @Override
    public void seek(PositionProvider index) throws IOException {
      if (present != null) {
        if (_isFileCompressed) {
          index.getNext();
        }
        present.seek(index);
      }

      // data stream could be empty stream or already reached end of stream before present stream.
      // This can happen if all values in stream are nulls or last row group values are all null.
      if (_dataStream.available() > 0) {
        if (_isFileCompressed) {
          index.getNext();
        }
        stream.seek(index);
      }

      if (lengths != null && _lengthsStream.available() > 0) {
        if (_isFileCompressed) {
          index.getNext();
        }
        lengths.seek(index);
      }
    }

    @Override
    public void setBuffers(ColumnStreamData[] buffers, boolean sameStripe)
        throws IOException {
      super.setBuffers(buffers, sameStripe);
      if (_presentStream != null) {
        _presentStream.setBuffers(StreamUtils.createDiskRangeInfo(presentStreamBuffer));
      }
      if (_dataStream != null) {
        _dataStream.setBuffers(StreamUtils.createDiskRangeInfo(dataStreamBuffer));
      }
      if (_lengthsStream != null) {
        _lengthsStream.setBuffers(StreamUtils.createDiskRangeInfo(lengthsStreamBuffer));
      }
    }

    public static class StreamReaderBuilder {
      private Long fileId;
      private int columnIndex;
      private ColumnStreamData presentStream;
      private ColumnStreamData dataStream;
      private ColumnStreamData lengthStream;
      private CompressionCodec compressionCodec;
      private OrcProto.ColumnEncoding columnEncoding;

      public StreamReaderBuilder setFileId(Long fileId) {
        this.fileId = fileId;
        return this;
      }

      public StreamReaderBuilder setColumnIndex(int columnIndex) {
        this.columnIndex = columnIndex;
        return this;
      }

      public StreamReaderBuilder setPresentStream(ColumnStreamData presentStream) {
        this.presentStream = presentStream;
        return this;
      }

      public StreamReaderBuilder setDataStream(ColumnStreamData dataStream) {
        this.dataStream = dataStream;
        return this;
      }

      public StreamReaderBuilder setLengthStream(ColumnStreamData secondaryStream) {
        this.lengthStream = secondaryStream;
        return this;
      }

      public StreamReaderBuilder setCompressionCodec(CompressionCodec compressionCodec) {
        this.compressionCodec = compressionCodec;
        return this;
      }

      public StreamReaderBuilder setColumnEncoding(OrcProto.ColumnEncoding encoding) {
        this.columnEncoding = encoding;
        return this;
      }

      public BinaryStreamReader build() throws IOException {
        SettableUncompressedStream present = StreamUtils.createSettableUncompressedStream(
            OrcProto.Stream.Kind.PRESENT.name(), fileId, presentStream);

        SettableUncompressedStream data = StreamUtils.createSettableUncompressedStream(
            OrcProto.Stream.Kind.DATA.name(), fileId, dataStream);

        SettableUncompressedStream length = StreamUtils.createSettableUncompressedStream(
            OrcProto.Stream.Kind.LENGTH.name(), fileId, lengthStream);

        boolean isFileCompressed = compressionCodec != null;
        return new BinaryStreamReader(columnIndex, present, data, length, isFileCompressed,
            columnEncoding);
      }
    }

    public static StreamReaderBuilder builder() {
      return new StreamReaderBuilder();
    }
  }

  protected static class BooleanStreamReader extends BooleanTreeReader {
    private boolean _isFileCompressed;
    private SettableUncompressedStream _presentStream;
    private SettableUncompressedStream _dataStream;

    private BooleanStreamReader(int columnId, SettableUncompressedStream present,
        SettableUncompressedStream data, boolean isFileCompressed) throws IOException {
      super(columnId, present, data);
      this._isFileCompressed = isFileCompressed;
      this._presentStream = present;
      this._dataStream = data;
    }

    @Override
    public void seek(PositionProvider index) throws IOException {
      if (present != null) {
        if (_isFileCompressed) {
          index.getNext();
        }
        present.seek(index);
      }

      // data stream could be empty stream or already reached end of stream before present stream.
      // This can happen if all values in stream are nulls or last row group values are all null.
      if (_dataStream.available() > 0) {
        if (_isFileCompressed) {
          index.getNext();
        }
        reader.seek(index);
      }
    }

    @Override
    public void setBuffers(ColumnStreamData[] buffers, boolean sameStripe)
        throws IOException {
      super.setBuffers(buffers, sameStripe);
      if (_presentStream != null) {
        _presentStream.setBuffers(StreamUtils.createDiskRangeInfo(presentStreamBuffer));
      }
      if (_dataStream != null) {
        _dataStream.setBuffers(StreamUtils.createDiskRangeInfo(dataStreamBuffer));
      }
    }

    public static class StreamReaderBuilder {
      private Long fileId;
      private int columnIndex;
      private ColumnStreamData presentStream;
      private ColumnStreamData dataStream;
      private CompressionCodec compressionCodec;

      public StreamReaderBuilder setFileId(Long fileId) {
        this.fileId = fileId;
        return this;
      }

      public StreamReaderBuilder setColumnIndex(int columnIndex) {
        this.columnIndex = columnIndex;
        return this;
      }

      public StreamReaderBuilder setPresentStream(ColumnStreamData presentStream) {
        this.presentStream = presentStream;
        return this;
      }

      public StreamReaderBuilder setDataStream(ColumnStreamData dataStream) {
        this.dataStream = dataStream;
        return this;
      }

      public StreamReaderBuilder setCompressionCodec(CompressionCodec compressionCodec) {
        this.compressionCodec = compressionCodec;
        return this;
      }

      public BooleanStreamReader build() throws IOException {
        SettableUncompressedStream present = StreamUtils
            .createSettableUncompressedStream(OrcProto.Stream.Kind.PRESENT.name(),
                fileId, presentStream);

        SettableUncompressedStream data = StreamUtils
            .createSettableUncompressedStream(OrcProto.Stream.Kind.DATA.name(), fileId,
                dataStream);

        boolean isFileCompressed = compressionCodec != null;
        return new BooleanStreamReader(columnIndex, present, data, isFileCompressed);
      }
    }

    public static StreamReaderBuilder builder() {
      return new StreamReaderBuilder();
    }
  }

  public static TreeReader[] createEncodedTreeReader(int numCols,
      List<OrcProto.Type> types,
      List<OrcProto.ColumnEncoding> encodings,
      EncodedColumnBatch<OrcBatchKey> batch,
      CompressionCodec codec, boolean skipCorrupt) throws IOException {
    long file = batch.getBatchKey().file;
    TreeReader[] treeReaders = new TreeReader[numCols];
    for (int i = 0; i < numCols; i++) {
      int columnIndex = batch.getColumnIxs()[i];
      ColumnStreamData[] streamBuffers = batch.getColumnData()[i];
      OrcProto.Type columnType = types.get(columnIndex);

      // EncodedColumnBatch is already decompressed, we don't really need to pass codec.
      // But we need to know if the original data is compressed or not. This is used to skip
      // positions in row index properly. If the file is originally compressed,
      // then 1st position (compressed offset) in row index should be skipped to get
      // uncompressed offset, else 1st position should not be skipped.
      // TODO: there should be a better way to do this, code just needs to be modified
      OrcProto.ColumnEncoding columnEncoding = encodings.get(columnIndex);

      // stream buffers are arranged in enum order of stream kind
      ColumnStreamData present = null;
      ColumnStreamData data = null;
      ColumnStreamData dictionary = null;
      ColumnStreamData lengths = null;
      ColumnStreamData secondary = null;
      for (ColumnStreamData streamBuffer : streamBuffers) {
        switch (streamBuffer.getStreamKind()) {
          case 0:
            // PRESENT stream
            present = streamBuffer;
            break;
          case 1:
            // DATA stream
            data = streamBuffer;
            break;
          case 2:
            // LENGTH stream
            lengths = streamBuffer;
            break;
          case 3:
            // DICTIONARY_DATA stream
            dictionary = streamBuffer;
            break;
          case 5:
            // SECONDARY stream
            secondary = streamBuffer;
            break;
          default:
            throw new IOException("Unexpected stream kind: " + streamBuffer.getStreamKind());
        }
      }

      switch (columnType.getKind()) {
        case BINARY:
          treeReaders[i] = BinaryStreamReader.builder()
              .setFileId(file)
              .setColumnIndex(columnIndex)
              .setPresentStream(present)
              .setDataStream(data)
              .setLengthStream(lengths)
              .setCompressionCodec(codec)
              .setColumnEncoding(columnEncoding)
              .build();
          break;
        case BOOLEAN:
          treeReaders[i] = BooleanStreamReader.builder()
              .setFileId(file)
              .setColumnIndex(columnIndex)
              .setPresentStream(present)
              .setDataStream(data)
              .setCompressionCodec(codec)
              .build();
          break;
        case BYTE:
          treeReaders[i] = ByteStreamReader.builder()
              .setFileId(file)
              .setColumnIndex(columnIndex)
              .setPresentStream(present)
              .setDataStream(data)
              .setCompressionCodec(codec)
              .build();
          break;
        case SHORT:
          treeReaders[i] = ShortStreamReader.builder()
              .setFileId(file)
              .setColumnIndex(columnIndex)
              .setPresentStream(present)
              .setDataStream(data)
              .setCompressionCodec(codec)
              .setColumnEncoding(columnEncoding)
              .build();
          break;
        case INT:
          treeReaders[i] = IntStreamReader.builder()
              .setFileId(file)
              .setColumnIndex(columnIndex)
              .setPresentStream(present)
              .setDataStream(data)
              .setCompressionCodec(codec)
              .setColumnEncoding(columnEncoding)
              .build();
          break;
        case LONG:
          treeReaders[i] = LongStreamReader.builder()
              .setFileId(file)
              .setColumnIndex(columnIndex)
              .setPresentStream(present)
              .setDataStream(data)
              .setCompressionCodec(codec)
              .setColumnEncoding(columnEncoding)
              .skipCorrupt(skipCorrupt)
              .build();
          break;
        case FLOAT:
          treeReaders[i] = FloatStreamReader.builder()
              .setFileId(file)
              .setColumnIndex(columnIndex)
              .setPresentStream(present)
              .setDataStream(data)
              .setCompressionCodec(codec)
              .build();
          break;
        case DOUBLE:
          treeReaders[i] = DoubleStreamReader.builder()
              .setFileId(file)
              .setColumnIndex(columnIndex)
              .setPresentStream(present)
              .setDataStream(data)
              .setCompressionCodec(codec)
              .build();
          break;
        case CHAR:
          treeReaders[i] = CharStreamReader.builder()
              .setFileId(file)
              .setColumnIndex(columnIndex)
              .setMaxLength(columnType.getMaximumLength())
              .setPresentStream(present)
              .setDataStream(data)
              .setLengthStream(lengths)
              .setDictionaryStream(dictionary)
              .setCompressionCodec(codec)
              .setColumnEncoding(columnEncoding)
              .build();
          break;
        case VARCHAR:
          treeReaders[i] = VarcharStreamReader.builder()
              .setFileId(file)
              .setColumnIndex(columnIndex)
              .setMaxLength(columnType.getMaximumLength())
              .setPresentStream(present)
              .setDataStream(data)
              .setLengthStream(lengths)
              .setDictionaryStream(dictionary)
              .setCompressionCodec(codec)
              .setColumnEncoding(columnEncoding)
              .build();
          break;
        case STRING:
          treeReaders[i] = StringStreamReader.builder()
              .setFileId(file)
              .setColumnIndex(columnIndex)
              .setPresentStream(present)
              .setDataStream(data)
              .setLengthStream(lengths)
              .setDictionaryStream(dictionary)
              .setCompressionCodec(codec)
              .setColumnEncoding(columnEncoding)
              .build();
          break;
        case DECIMAL:
          treeReaders[i] = DecimalStreamReader.builder()
              .setFileId(file)
              .setColumnIndex(columnIndex)
              .setPrecision(columnType.getPrecision())
              .setScale(columnType.getScale())
              .setPresentStream(present)
              .setValueStream(data)
              .setScaleStream(secondary)
              .setCompressionCodec(codec)
              .setColumnEncoding(columnEncoding)
              .build();
          break;
        case TIMESTAMP:
          treeReaders[i] = TimestampStreamReader.builder()
              .setFileId(file)
              .setColumnIndex(columnIndex)
              .setPresentStream(present)
              .setSecondsStream(data)
              .setNanosStream(secondary)
              .setCompressionCodec(codec)
              .setColumnEncoding(columnEncoding)
              .skipCorrupt(skipCorrupt)
              .build();
          break;
        case DATE:
          treeReaders[i] = DateStreamReader.builder()
              .setFileId(file)
              .setColumnIndex(columnIndex)
              .setPresentStream(present)
              .setDataStream(data)
              .setCompressionCodec(codec)
              .setColumnEncoding(columnEncoding)
              .build();
          break;
        default:
          throw new UnsupportedOperationException("Data type not supported yet! " + columnType);
      }
    }

    return treeReaders;
  }
}
