/**
 * Autogenerated by Thrift Compiler (0.9.2)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *  @generated
 */
package org.apache.hive.service.cli.thrift;

import org.apache.thrift.scheme.IScheme;
import org.apache.thrift.scheme.SchemeFactory;
import org.apache.thrift.scheme.StandardScheme;

import org.apache.thrift.scheme.TupleScheme;
import org.apache.thrift.protocol.TTupleProtocol;
import org.apache.thrift.protocol.TProtocolException;
import org.apache.thrift.EncodingUtils;
import org.apache.thrift.TException;
import org.apache.thrift.async.AsyncMethodCallback;
import org.apache.thrift.server.AbstractNonblockingServer.*;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.EnumMap;
import java.util.Set;
import java.util.HashSet;
import java.util.EnumSet;
import java.util.Collections;
import java.util.BitSet;
import java.nio.ByteBuffer;
import java.util.Arrays;
import javax.annotation.Generated;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings({"cast", "rawtypes", "serial", "unchecked"})
@Generated(value = "Autogenerated by Thrift Compiler (0.9.2)", date = "2015-8-14")
public class TColumnDesc implements org.apache.thrift.TBase<TColumnDesc, TColumnDesc._Fields>, java.io.Serializable, Cloneable, Comparable<TColumnDesc> {
  private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct("TColumnDesc");

  private static final org.apache.thrift.protocol.TField COLUMN_NAME_FIELD_DESC = new org.apache.thrift.protocol.TField("columnName", org.apache.thrift.protocol.TType.STRING, (short)1);
  private static final org.apache.thrift.protocol.TField TYPE_DESC_FIELD_DESC = new org.apache.thrift.protocol.TField("typeDesc", org.apache.thrift.protocol.TType.STRUCT, (short)2);
  private static final org.apache.thrift.protocol.TField POSITION_FIELD_DESC = new org.apache.thrift.protocol.TField("position", org.apache.thrift.protocol.TType.I32, (short)3);
  private static final org.apache.thrift.protocol.TField COMMENT_FIELD_DESC = new org.apache.thrift.protocol.TField("comment", org.apache.thrift.protocol.TType.STRING, (short)4);

  private static final Map<Class<? extends IScheme>, SchemeFactory> schemes = new HashMap<Class<? extends IScheme>, SchemeFactory>();
  static {
    schemes.put(StandardScheme.class, new TColumnDescStandardSchemeFactory());
    schemes.put(TupleScheme.class, new TColumnDescTupleSchemeFactory());
  }

  private String columnName; // required
  private TTypeDesc typeDesc; // required
  private int position; // required
  private String comment; // optional

  /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
  public enum _Fields implements org.apache.thrift.TFieldIdEnum {
    COLUMN_NAME((short)1, "columnName"),
    TYPE_DESC((short)2, "typeDesc"),
    POSITION((short)3, "position"),
    COMMENT((short)4, "comment");

    private static final Map<String, _Fields> byName = new HashMap<String, _Fields>();

    static {
      for (_Fields field : EnumSet.allOf(_Fields.class)) {
        byName.put(field.getFieldName(), field);
      }
    }

    /**
     * Find the _Fields constant that matches fieldId, or null if its not found.
     */
    public static _Fields findByThriftId(int fieldId) {
      switch(fieldId) {
        case 1: // COLUMN_NAME
          return COLUMN_NAME;
        case 2: // TYPE_DESC
          return TYPE_DESC;
        case 3: // POSITION
          return POSITION;
        case 4: // COMMENT
          return COMMENT;
        default:
          return null;
      }
    }

    /**
     * Find the _Fields constant that matches fieldId, throwing an exception
     * if it is not found.
     */
    public static _Fields findByThriftIdOrThrow(int fieldId) {
      _Fields fields = findByThriftId(fieldId);
      if (fields == null) throw new IllegalArgumentException("Field " + fieldId + " doesn't exist!");
      return fields;
    }

    /**
     * Find the _Fields constant that matches name, or null if its not found.
     */
    public static _Fields findByName(String name) {
      return byName.get(name);
    }

    private final short _thriftId;
    private final String _fieldName;

    _Fields(short thriftId, String fieldName) {
      _thriftId = thriftId;
      _fieldName = fieldName;
    }

    public short getThriftFieldId() {
      return _thriftId;
    }

    public String getFieldName() {
      return _fieldName;
    }
  }

  // isset id assignments
  private static final int __POSITION_ISSET_ID = 0;
  private byte __isset_bitfield = 0;
  private static final _Fields optionals[] = {_Fields.COMMENT};
  public static final Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> metaDataMap;
  static {
    Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> tmpMap = new EnumMap<_Fields, org.apache.thrift.meta_data.FieldMetaData>(_Fields.class);
    tmpMap.put(_Fields.COLUMN_NAME, new org.apache.thrift.meta_data.FieldMetaData("columnName", org.apache.thrift.TFieldRequirementType.REQUIRED, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING)));
    tmpMap.put(_Fields.TYPE_DESC, new org.apache.thrift.meta_data.FieldMetaData("typeDesc", org.apache.thrift.TFieldRequirementType.REQUIRED, 
        new org.apache.thrift.meta_data.StructMetaData(org.apache.thrift.protocol.TType.STRUCT, TTypeDesc.class)));
    tmpMap.put(_Fields.POSITION, new org.apache.thrift.meta_data.FieldMetaData("position", org.apache.thrift.TFieldRequirementType.REQUIRED, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.I32)));
    tmpMap.put(_Fields.COMMENT, new org.apache.thrift.meta_data.FieldMetaData("comment", org.apache.thrift.TFieldRequirementType.OPTIONAL, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING)));
    metaDataMap = Collections.unmodifiableMap(tmpMap);
    org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(TColumnDesc.class, metaDataMap);
  }

  public TColumnDesc() {
  }

  public TColumnDesc(
    String columnName,
    TTypeDesc typeDesc,
    int position)
  {
    this();
    this.columnName = columnName;
    this.typeDesc = typeDesc;
    this.position = position;
    setPositionIsSet(true);
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public TColumnDesc(TColumnDesc other) {
    __isset_bitfield = other.__isset_bitfield;
    if (other.isSetColumnName()) {
      this.columnName = other.columnName;
    }
    if (other.isSetTypeDesc()) {
      this.typeDesc = new TTypeDesc(other.typeDesc);
    }
    this.position = other.position;
    if (other.isSetComment()) {
      this.comment = other.comment;
    }
  }

  public TColumnDesc deepCopy() {
    return new TColumnDesc(this);
  }

  @Override
  public void clear() {
    this.columnName = null;
    this.typeDesc = null;
    setPositionIsSet(false);
    this.position = 0;
    this.comment = null;
  }

  public String getColumnName() {
    return this.columnName;
  }

  public void setColumnName(String columnName) {
    this.columnName = columnName;
  }

  public void unsetColumnName() {
    this.columnName = null;
  }

  /** Returns true if field columnName is set (has been assigned a value) and false otherwise */
  public boolean isSetColumnName() {
    return this.columnName != null;
  }

  public void setColumnNameIsSet(boolean value) {
    if (!value) {
      this.columnName = null;
    }
  }

  public TTypeDesc getTypeDesc() {
    return this.typeDesc;
  }

  public void setTypeDesc(TTypeDesc typeDesc) {
    this.typeDesc = typeDesc;
  }

  public void unsetTypeDesc() {
    this.typeDesc = null;
  }

  /** Returns true if field typeDesc is set (has been assigned a value) and false otherwise */
  public boolean isSetTypeDesc() {
    return this.typeDesc != null;
  }

  public void setTypeDescIsSet(boolean value) {
    if (!value) {
      this.typeDesc = null;
    }
  }

  public int getPosition() {
    return this.position;
  }

  public void setPosition(int position) {
    this.position = position;
    setPositionIsSet(true);
  }

  public void unsetPosition() {
    __isset_bitfield = EncodingUtils.clearBit(__isset_bitfield, __POSITION_ISSET_ID);
  }

  /** Returns true if field position is set (has been assigned a value) and false otherwise */
  public boolean isSetPosition() {
    return EncodingUtils.testBit(__isset_bitfield, __POSITION_ISSET_ID);
  }

  public void setPositionIsSet(boolean value) {
    __isset_bitfield = EncodingUtils.setBit(__isset_bitfield, __POSITION_ISSET_ID, value);
  }

  public String getComment() {
    return this.comment;
  }

  public void setComment(String comment) {
    this.comment = comment;
  }

  public void unsetComment() {
    this.comment = null;
  }

  /** Returns true if field comment is set (has been assigned a value) and false otherwise */
  public boolean isSetComment() {
    return this.comment != null;
  }

  public void setCommentIsSet(boolean value) {
    if (!value) {
      this.comment = null;
    }
  }

  public void setFieldValue(_Fields field, Object value) {
    switch (field) {
    case COLUMN_NAME:
      if (value == null) {
        unsetColumnName();
      } else {
        setColumnName((String)value);
      }
      break;

    case TYPE_DESC:
      if (value == null) {
        unsetTypeDesc();
      } else {
        setTypeDesc((TTypeDesc)value);
      }
      break;

    case POSITION:
      if (value == null) {
        unsetPosition();
      } else {
        setPosition((Integer)value);
      }
      break;

    case COMMENT:
      if (value == null) {
        unsetComment();
      } else {
        setComment((String)value);
      }
      break;

    }
  }

  public Object getFieldValue(_Fields field) {
    switch (field) {
    case COLUMN_NAME:
      return getColumnName();

    case TYPE_DESC:
      return getTypeDesc();

    case POSITION:
      return Integer.valueOf(getPosition());

    case COMMENT:
      return getComment();

    }
    throw new IllegalStateException();
  }

  /** Returns true if field corresponding to fieldID is set (has been assigned a value) and false otherwise */
  public boolean isSet(_Fields field) {
    if (field == null) {
      throw new IllegalArgumentException();
    }

    switch (field) {
    case COLUMN_NAME:
      return isSetColumnName();
    case TYPE_DESC:
      return isSetTypeDesc();
    case POSITION:
      return isSetPosition();
    case COMMENT:
      return isSetComment();
    }
    throw new IllegalStateException();
  }

  @Override
  public boolean equals(Object that) {
    if (that == null)
      return false;
    if (that instanceof TColumnDesc)
      return this.equals((TColumnDesc)that);
    return false;
  }

  public boolean equals(TColumnDesc that) {
    if (that == null)
      return false;

    boolean this_present_columnName = true && this.isSetColumnName();
    boolean that_present_columnName = true && that.isSetColumnName();
    if (this_present_columnName || that_present_columnName) {
      if (!(this_present_columnName && that_present_columnName))
        return false;
      if (!this.columnName.equals(that.columnName))
        return false;
    }

    boolean this_present_typeDesc = true && this.isSetTypeDesc();
    boolean that_present_typeDesc = true && that.isSetTypeDesc();
    if (this_present_typeDesc || that_present_typeDesc) {
      if (!(this_present_typeDesc && that_present_typeDesc))
        return false;
      if (!this.typeDesc.equals(that.typeDesc))
        return false;
    }

    boolean this_present_position = true;
    boolean that_present_position = true;
    if (this_present_position || that_present_position) {
      if (!(this_present_position && that_present_position))
        return false;
      if (this.position != that.position)
        return false;
    }

    boolean this_present_comment = true && this.isSetComment();
    boolean that_present_comment = true && that.isSetComment();
    if (this_present_comment || that_present_comment) {
      if (!(this_present_comment && that_present_comment))
        return false;
      if (!this.comment.equals(that.comment))
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    List<Object> list = new ArrayList<Object>();

    boolean present_columnName = true && (isSetColumnName());
    list.add(present_columnName);
    if (present_columnName)
      list.add(columnName);

    boolean present_typeDesc = true && (isSetTypeDesc());
    list.add(present_typeDesc);
    if (present_typeDesc)
      list.add(typeDesc);

    boolean present_position = true;
    list.add(present_position);
    if (present_position)
      list.add(position);

    boolean present_comment = true && (isSetComment());
    list.add(present_comment);
    if (present_comment)
      list.add(comment);

    return list.hashCode();
  }

  @Override
  public int compareTo(TColumnDesc other) {
    if (!getClass().equals(other.getClass())) {
      return getClass().getName().compareTo(other.getClass().getName());
    }

    int lastComparison = 0;

    lastComparison = Boolean.valueOf(isSetColumnName()).compareTo(other.isSetColumnName());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetColumnName()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.columnName, other.columnName);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetTypeDesc()).compareTo(other.isSetTypeDesc());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetTypeDesc()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.typeDesc, other.typeDesc);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetPosition()).compareTo(other.isSetPosition());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetPosition()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.position, other.position);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetComment()).compareTo(other.isSetComment());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetComment()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.comment, other.comment);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    return 0;
  }

  public _Fields fieldForId(int fieldId) {
    return _Fields.findByThriftId(fieldId);
  }

  public void read(org.apache.thrift.protocol.TProtocol iprot) throws org.apache.thrift.TException {
    schemes.get(iprot.getScheme()).getScheme().read(iprot, this);
  }

  public void write(org.apache.thrift.protocol.TProtocol oprot) throws org.apache.thrift.TException {
    schemes.get(oprot.getScheme()).getScheme().write(oprot, this);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("TColumnDesc(");
    boolean first = true;

    sb.append("columnName:");
    if (this.columnName == null) {
      sb.append("null");
    } else {
      sb.append(this.columnName);
    }
    first = false;
    if (!first) sb.append(", ");
    sb.append("typeDesc:");
    if (this.typeDesc == null) {
      sb.append("null");
    } else {
      sb.append(this.typeDesc);
    }
    first = false;
    if (!first) sb.append(", ");
    sb.append("position:");
    sb.append(this.position);
    first = false;
    if (isSetComment()) {
      if (!first) sb.append(", ");
      sb.append("comment:");
      if (this.comment == null) {
        sb.append("null");
      } else {
        sb.append(this.comment);
      }
      first = false;
    }
    sb.append(")");
    return sb.toString();
  }

  public void validate() throws org.apache.thrift.TException {
    // check for required fields
    if (!isSetColumnName()) {
      throw new org.apache.thrift.protocol.TProtocolException("Required field 'columnName' is unset! Struct:" + toString());
    }

    if (!isSetTypeDesc()) {
      throw new org.apache.thrift.protocol.TProtocolException("Required field 'typeDesc' is unset! Struct:" + toString());
    }

    if (!isSetPosition()) {
      throw new org.apache.thrift.protocol.TProtocolException("Required field 'position' is unset! Struct:" + toString());
    }

    // check for sub-struct validity
    if (typeDesc != null) {
      typeDesc.validate();
    }
  }

  private void writeObject(java.io.ObjectOutputStream out) throws java.io.IOException {
    try {
      write(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(out)));
    } catch (org.apache.thrift.TException te) {
      throw new java.io.IOException(te);
    }
  }

  private void readObject(java.io.ObjectInputStream in) throws java.io.IOException, ClassNotFoundException {
    try {
      // it doesn't seem like you should have to do this, but java serialization is wacky, and doesn't call the default constructor.
      __isset_bitfield = 0;
      read(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(in)));
    } catch (org.apache.thrift.TException te) {
      throw new java.io.IOException(te);
    }
  }

  private static class TColumnDescStandardSchemeFactory implements SchemeFactory {
    public TColumnDescStandardScheme getScheme() {
      return new TColumnDescStandardScheme();
    }
  }

  private static class TColumnDescStandardScheme extends StandardScheme<TColumnDesc> {

    public void read(org.apache.thrift.protocol.TProtocol iprot, TColumnDesc struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TField schemeField;
      iprot.readStructBegin();
      while (true)
      {
        schemeField = iprot.readFieldBegin();
        if (schemeField.type == org.apache.thrift.protocol.TType.STOP) { 
          break;
        }
        switch (schemeField.id) {
          case 1: // COLUMN_NAME
            if (schemeField.type == org.apache.thrift.protocol.TType.STRING) {
              struct.columnName = iprot.readString();
              struct.setColumnNameIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 2: // TYPE_DESC
            if (schemeField.type == org.apache.thrift.protocol.TType.STRUCT) {
              struct.typeDesc = new TTypeDesc();
              struct.typeDesc.read(iprot);
              struct.setTypeDescIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 3: // POSITION
            if (schemeField.type == org.apache.thrift.protocol.TType.I32) {
              struct.position = iprot.readI32();
              struct.setPositionIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 4: // COMMENT
            if (schemeField.type == org.apache.thrift.protocol.TType.STRING) {
              struct.comment = iprot.readString();
              struct.setCommentIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          default:
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
        }
        iprot.readFieldEnd();
      }
      iprot.readStructEnd();
      struct.validate();
    }

    public void write(org.apache.thrift.protocol.TProtocol oprot, TColumnDesc struct) throws org.apache.thrift.TException {
      struct.validate();

      oprot.writeStructBegin(STRUCT_DESC);
      if (struct.columnName != null) {
        oprot.writeFieldBegin(COLUMN_NAME_FIELD_DESC);
        oprot.writeString(struct.columnName);
        oprot.writeFieldEnd();
      }
      if (struct.typeDesc != null) {
        oprot.writeFieldBegin(TYPE_DESC_FIELD_DESC);
        struct.typeDesc.write(oprot);
        oprot.writeFieldEnd();
      }
      oprot.writeFieldBegin(POSITION_FIELD_DESC);
      oprot.writeI32(struct.position);
      oprot.writeFieldEnd();
      if (struct.comment != null) {
        if (struct.isSetComment()) {
          oprot.writeFieldBegin(COMMENT_FIELD_DESC);
          oprot.writeString(struct.comment);
          oprot.writeFieldEnd();
        }
      }
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

  }

  private static class TColumnDescTupleSchemeFactory implements SchemeFactory {
    public TColumnDescTupleScheme getScheme() {
      return new TColumnDescTupleScheme();
    }
  }

  private static class TColumnDescTupleScheme extends TupleScheme<TColumnDesc> {

    @Override
    public void write(org.apache.thrift.protocol.TProtocol prot, TColumnDesc struct) throws org.apache.thrift.TException {
      TTupleProtocol oprot = (TTupleProtocol) prot;
      oprot.writeString(struct.columnName);
      struct.typeDesc.write(oprot);
      oprot.writeI32(struct.position);
      BitSet optionals = new BitSet();
      if (struct.isSetComment()) {
        optionals.set(0);
      }
      oprot.writeBitSet(optionals, 1);
      if (struct.isSetComment()) {
        oprot.writeString(struct.comment);
      }
    }

    @Override
    public void read(org.apache.thrift.protocol.TProtocol prot, TColumnDesc struct) throws org.apache.thrift.TException {
      TTupleProtocol iprot = (TTupleProtocol) prot;
      struct.columnName = iprot.readString();
      struct.setColumnNameIsSet(true);
      struct.typeDesc = new TTypeDesc();
      struct.typeDesc.read(iprot);
      struct.setTypeDescIsSet(true);
      struct.position = iprot.readI32();
      struct.setPositionIsSet(true);
      BitSet incoming = iprot.readBitSet(1);
      if (incoming.get(0)) {
        struct.comment = iprot.readString();
        struct.setCommentIsSet(true);
      }
    }
  }

}

