/**
 * Autogenerated by Thrift Compiler (0.9.3)
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
@Generated(value = "Autogenerated by Thrift Compiler (0.9.3)")
public class TGetTablesReq implements org.apache.thrift.TBase<TGetTablesReq, TGetTablesReq._Fields>, java.io.Serializable, Cloneable, Comparable<TGetTablesReq> {
  private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct("TGetTablesReq");

  private static final org.apache.thrift.protocol.TField SESSION_HANDLE_FIELD_DESC = new org.apache.thrift.protocol.TField("sessionHandle", org.apache.thrift.protocol.TType.STRUCT, (short)1);
  private static final org.apache.thrift.protocol.TField CATALOG_NAME_FIELD_DESC = new org.apache.thrift.protocol.TField("catalogName", org.apache.thrift.protocol.TType.STRING, (short)2);
  private static final org.apache.thrift.protocol.TField SCHEMA_NAME_FIELD_DESC = new org.apache.thrift.protocol.TField("schemaName", org.apache.thrift.protocol.TType.STRING, (short)3);
  private static final org.apache.thrift.protocol.TField TABLE_NAME_FIELD_DESC = new org.apache.thrift.protocol.TField("tableName", org.apache.thrift.protocol.TType.STRING, (short)4);
  private static final org.apache.thrift.protocol.TField TABLE_TYPES_FIELD_DESC = new org.apache.thrift.protocol.TField("tableTypes", org.apache.thrift.protocol.TType.LIST, (short)5);

  private static final Map<Class<? extends IScheme>, SchemeFactory> schemes = new HashMap<Class<? extends IScheme>, SchemeFactory>();
  static {
    schemes.put(StandardScheme.class, new TGetTablesReqStandardSchemeFactory());
    schemes.put(TupleScheme.class, new TGetTablesReqTupleSchemeFactory());
  }

  private TSessionHandle sessionHandle; // required
  private String catalogName; // optional
  private String schemaName; // optional
  private String tableName; // optional
  private List<String> tableTypes; // optional

  /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
  public enum _Fields implements org.apache.thrift.TFieldIdEnum {
    SESSION_HANDLE((short)1, "sessionHandle"),
    CATALOG_NAME((short)2, "catalogName"),
    SCHEMA_NAME((short)3, "schemaName"),
    TABLE_NAME((short)4, "tableName"),
    TABLE_TYPES((short)5, "tableTypes");

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
        case 1: // SESSION_HANDLE
          return SESSION_HANDLE;
        case 2: // CATALOG_NAME
          return CATALOG_NAME;
        case 3: // SCHEMA_NAME
          return SCHEMA_NAME;
        case 4: // TABLE_NAME
          return TABLE_NAME;
        case 5: // TABLE_TYPES
          return TABLE_TYPES;
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
  private static final _Fields optionals[] = {_Fields.CATALOG_NAME,_Fields.SCHEMA_NAME,_Fields.TABLE_NAME,_Fields.TABLE_TYPES};
  public static final Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> metaDataMap;
  static {
    Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> tmpMap = new EnumMap<_Fields, org.apache.thrift.meta_data.FieldMetaData>(_Fields.class);
    tmpMap.put(_Fields.SESSION_HANDLE, new org.apache.thrift.meta_data.FieldMetaData("sessionHandle", org.apache.thrift.TFieldRequirementType.REQUIRED, 
        new org.apache.thrift.meta_data.StructMetaData(org.apache.thrift.protocol.TType.STRUCT, TSessionHandle.class)));
    tmpMap.put(_Fields.CATALOG_NAME, new org.apache.thrift.meta_data.FieldMetaData("catalogName", org.apache.thrift.TFieldRequirementType.OPTIONAL, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING        , "TPatternOrIdentifier")));
    tmpMap.put(_Fields.SCHEMA_NAME, new org.apache.thrift.meta_data.FieldMetaData("schemaName", org.apache.thrift.TFieldRequirementType.OPTIONAL, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING        , "TPatternOrIdentifier")));
    tmpMap.put(_Fields.TABLE_NAME, new org.apache.thrift.meta_data.FieldMetaData("tableName", org.apache.thrift.TFieldRequirementType.OPTIONAL, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING        , "TPatternOrIdentifier")));
    tmpMap.put(_Fields.TABLE_TYPES, new org.apache.thrift.meta_data.FieldMetaData("tableTypes", org.apache.thrift.TFieldRequirementType.OPTIONAL, 
        new org.apache.thrift.meta_data.ListMetaData(org.apache.thrift.protocol.TType.LIST, 
            new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING))));
    metaDataMap = Collections.unmodifiableMap(tmpMap);
    org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(TGetTablesReq.class, metaDataMap);
  }

  public TGetTablesReq() {
  }

  public TGetTablesReq(
    TSessionHandle sessionHandle)
  {
    this();
    this.sessionHandle = sessionHandle;
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public TGetTablesReq(TGetTablesReq other) {
    if (other.isSetSessionHandle()) {
      this.sessionHandle = new TSessionHandle(other.sessionHandle);
    }
    if (other.isSetCatalogName()) {
      this.catalogName = other.catalogName;
    }
    if (other.isSetSchemaName()) {
      this.schemaName = other.schemaName;
    }
    if (other.isSetTableName()) {
      this.tableName = other.tableName;
    }
    if (other.isSetTableTypes()) {
      List<String> __this__tableTypes = new ArrayList<String>(other.tableTypes);
      this.tableTypes = __this__tableTypes;
    }
  }

  public TGetTablesReq deepCopy() {
    return new TGetTablesReq(this);
  }

  @Override
  public void clear() {
    this.sessionHandle = null;
    this.catalogName = null;
    this.schemaName = null;
    this.tableName = null;
    this.tableTypes = null;
  }

  public TSessionHandle getSessionHandle() {
    return this.sessionHandle;
  }

  public void setSessionHandle(TSessionHandle sessionHandle) {
    this.sessionHandle = sessionHandle;
  }

  public void unsetSessionHandle() {
    this.sessionHandle = null;
  }

  /** Returns true if field sessionHandle is set (has been assigned a value) and false otherwise */
  public boolean isSetSessionHandle() {
    return this.sessionHandle != null;
  }

  public void setSessionHandleIsSet(boolean value) {
    if (!value) {
      this.sessionHandle = null;
    }
  }

  public String getCatalogName() {
    return this.catalogName;
  }

  public void setCatalogName(String catalogName) {
    this.catalogName = catalogName;
  }

  public void unsetCatalogName() {
    this.catalogName = null;
  }

  /** Returns true if field catalogName is set (has been assigned a value) and false otherwise */
  public boolean isSetCatalogName() {
    return this.catalogName != null;
  }

  public void setCatalogNameIsSet(boolean value) {
    if (!value) {
      this.catalogName = null;
    }
  }

  public String getSchemaName() {
    return this.schemaName;
  }

  public void setSchemaName(String schemaName) {
    this.schemaName = schemaName;
  }

  public void unsetSchemaName() {
    this.schemaName = null;
  }

  /** Returns true if field schemaName is set (has been assigned a value) and false otherwise */
  public boolean isSetSchemaName() {
    return this.schemaName != null;
  }

  public void setSchemaNameIsSet(boolean value) {
    if (!value) {
      this.schemaName = null;
    }
  }

  public String getTableName() {
    return this.tableName;
  }

  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

  public void unsetTableName() {
    this.tableName = null;
  }

  /** Returns true if field tableName is set (has been assigned a value) and false otherwise */
  public boolean isSetTableName() {
    return this.tableName != null;
  }

  public void setTableNameIsSet(boolean value) {
    if (!value) {
      this.tableName = null;
    }
  }

  public int getTableTypesSize() {
    return (this.tableTypes == null) ? 0 : this.tableTypes.size();
  }

  public java.util.Iterator<String> getTableTypesIterator() {
    return (this.tableTypes == null) ? null : this.tableTypes.iterator();
  }

  public void addToTableTypes(String elem) {
    if (this.tableTypes == null) {
      this.tableTypes = new ArrayList<String>();
    }
    this.tableTypes.add(elem);
  }

  public List<String> getTableTypes() {
    return this.tableTypes;
  }

  public void setTableTypes(List<String> tableTypes) {
    this.tableTypes = tableTypes;
  }

  public void unsetTableTypes() {
    this.tableTypes = null;
  }

  /** Returns true if field tableTypes is set (has been assigned a value) and false otherwise */
  public boolean isSetTableTypes() {
    return this.tableTypes != null;
  }

  public void setTableTypesIsSet(boolean value) {
    if (!value) {
      this.tableTypes = null;
    }
  }

  public void setFieldValue(_Fields field, Object value) {
    switch (field) {
    case SESSION_HANDLE:
      if (value == null) {
        unsetSessionHandle();
      } else {
        setSessionHandle((TSessionHandle)value);
      }
      break;

    case CATALOG_NAME:
      if (value == null) {
        unsetCatalogName();
      } else {
        setCatalogName((String)value);
      }
      break;

    case SCHEMA_NAME:
      if (value == null) {
        unsetSchemaName();
      } else {
        setSchemaName((String)value);
      }
      break;

    case TABLE_NAME:
      if (value == null) {
        unsetTableName();
      } else {
        setTableName((String)value);
      }
      break;

    case TABLE_TYPES:
      if (value == null) {
        unsetTableTypes();
      } else {
        setTableTypes((List<String>)value);
      }
      break;

    }
  }

  public Object getFieldValue(_Fields field) {
    switch (field) {
    case SESSION_HANDLE:
      return getSessionHandle();

    case CATALOG_NAME:
      return getCatalogName();

    case SCHEMA_NAME:
      return getSchemaName();

    case TABLE_NAME:
      return getTableName();

    case TABLE_TYPES:
      return getTableTypes();

    }
    throw new IllegalStateException();
  }

  /** Returns true if field corresponding to fieldID is set (has been assigned a value) and false otherwise */
  public boolean isSet(_Fields field) {
    if (field == null) {
      throw new IllegalArgumentException();
    }

    switch (field) {
    case SESSION_HANDLE:
      return isSetSessionHandle();
    case CATALOG_NAME:
      return isSetCatalogName();
    case SCHEMA_NAME:
      return isSetSchemaName();
    case TABLE_NAME:
      return isSetTableName();
    case TABLE_TYPES:
      return isSetTableTypes();
    }
    throw new IllegalStateException();
  }

  @Override
  public boolean equals(Object that) {
    if (that == null)
      return false;
    if (that instanceof TGetTablesReq)
      return this.equals((TGetTablesReq)that);
    return false;
  }

  public boolean equals(TGetTablesReq that) {
    if (that == null)
      return false;

    boolean this_present_sessionHandle = true && this.isSetSessionHandle();
    boolean that_present_sessionHandle = true && that.isSetSessionHandle();
    if (this_present_sessionHandle || that_present_sessionHandle) {
      if (!(this_present_sessionHandle && that_present_sessionHandle))
        return false;
      if (!this.sessionHandle.equals(that.sessionHandle))
        return false;
    }

    boolean this_present_catalogName = true && this.isSetCatalogName();
    boolean that_present_catalogName = true && that.isSetCatalogName();
    if (this_present_catalogName || that_present_catalogName) {
      if (!(this_present_catalogName && that_present_catalogName))
        return false;
      if (!this.catalogName.equals(that.catalogName))
        return false;
    }

    boolean this_present_schemaName = true && this.isSetSchemaName();
    boolean that_present_schemaName = true && that.isSetSchemaName();
    if (this_present_schemaName || that_present_schemaName) {
      if (!(this_present_schemaName && that_present_schemaName))
        return false;
      if (!this.schemaName.equals(that.schemaName))
        return false;
    }

    boolean this_present_tableName = true && this.isSetTableName();
    boolean that_present_tableName = true && that.isSetTableName();
    if (this_present_tableName || that_present_tableName) {
      if (!(this_present_tableName && that_present_tableName))
        return false;
      if (!this.tableName.equals(that.tableName))
        return false;
    }

    boolean this_present_tableTypes = true && this.isSetTableTypes();
    boolean that_present_tableTypes = true && that.isSetTableTypes();
    if (this_present_tableTypes || that_present_tableTypes) {
      if (!(this_present_tableTypes && that_present_tableTypes))
        return false;
      if (!this.tableTypes.equals(that.tableTypes))
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    List<Object> list = new ArrayList<Object>();

    boolean present_sessionHandle = true && (isSetSessionHandle());
    list.add(present_sessionHandle);
    if (present_sessionHandle)
      list.add(sessionHandle);

    boolean present_catalogName = true && (isSetCatalogName());
    list.add(present_catalogName);
    if (present_catalogName)
      list.add(catalogName);

    boolean present_schemaName = true && (isSetSchemaName());
    list.add(present_schemaName);
    if (present_schemaName)
      list.add(schemaName);

    boolean present_tableName = true && (isSetTableName());
    list.add(present_tableName);
    if (present_tableName)
      list.add(tableName);

    boolean present_tableTypes = true && (isSetTableTypes());
    list.add(present_tableTypes);
    if (present_tableTypes)
      list.add(tableTypes);

    return list.hashCode();
  }

  @Override
  public int compareTo(TGetTablesReq other) {
    if (!getClass().equals(other.getClass())) {
      return getClass().getName().compareTo(other.getClass().getName());
    }

    int lastComparison = 0;

    lastComparison = Boolean.valueOf(isSetSessionHandle()).compareTo(other.isSetSessionHandle());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetSessionHandle()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.sessionHandle, other.sessionHandle);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetCatalogName()).compareTo(other.isSetCatalogName());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetCatalogName()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.catalogName, other.catalogName);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetSchemaName()).compareTo(other.isSetSchemaName());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetSchemaName()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.schemaName, other.schemaName);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetTableName()).compareTo(other.isSetTableName());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetTableName()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.tableName, other.tableName);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetTableTypes()).compareTo(other.isSetTableTypes());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetTableTypes()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.tableTypes, other.tableTypes);
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
    StringBuilder sb = new StringBuilder("TGetTablesReq(");
    boolean first = true;

    sb.append("sessionHandle:");
    if (this.sessionHandle == null) {
      sb.append("null");
    } else {
      sb.append(this.sessionHandle);
    }
    first = false;
    if (isSetCatalogName()) {
      if (!first) sb.append(", ");
      sb.append("catalogName:");
      if (this.catalogName == null) {
        sb.append("null");
      } else {
        sb.append(this.catalogName);
      }
      first = false;
    }
    if (isSetSchemaName()) {
      if (!first) sb.append(", ");
      sb.append("schemaName:");
      if (this.schemaName == null) {
        sb.append("null");
      } else {
        sb.append(this.schemaName);
      }
      first = false;
    }
    if (isSetTableName()) {
      if (!first) sb.append(", ");
      sb.append("tableName:");
      if (this.tableName == null) {
        sb.append("null");
      } else {
        sb.append(this.tableName);
      }
      first = false;
    }
    if (isSetTableTypes()) {
      if (!first) sb.append(", ");
      sb.append("tableTypes:");
      if (this.tableTypes == null) {
        sb.append("null");
      } else {
        sb.append(this.tableTypes);
      }
      first = false;
    }
    sb.append(")");
    return sb.toString();
  }

  public void validate() throws org.apache.thrift.TException {
    // check for required fields
    if (!isSetSessionHandle()) {
      throw new org.apache.thrift.protocol.TProtocolException("Required field 'sessionHandle' is unset! Struct:" + toString());
    }

    // check for sub-struct validity
    if (sessionHandle != null) {
      sessionHandle.validate();
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
      read(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(in)));
    } catch (org.apache.thrift.TException te) {
      throw new java.io.IOException(te);
    }
  }

  private static class TGetTablesReqStandardSchemeFactory implements SchemeFactory {
    public TGetTablesReqStandardScheme getScheme() {
      return new TGetTablesReqStandardScheme();
    }
  }

  private static class TGetTablesReqStandardScheme extends StandardScheme<TGetTablesReq> {

    public void read(org.apache.thrift.protocol.TProtocol iprot, TGetTablesReq struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TField schemeField;
      iprot.readStructBegin();
      while (true)
      {
        schemeField = iprot.readFieldBegin();
        if (schemeField.type == org.apache.thrift.protocol.TType.STOP) { 
          break;
        }
        switch (schemeField.id) {
          case 1: // SESSION_HANDLE
            if (schemeField.type == org.apache.thrift.protocol.TType.STRUCT) {
              struct.sessionHandle = new TSessionHandle();
              struct.sessionHandle.read(iprot);
              struct.setSessionHandleIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 2: // CATALOG_NAME
            if (schemeField.type == org.apache.thrift.protocol.TType.STRING) {
              struct.catalogName = iprot.readString();
              struct.setCatalogNameIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 3: // SCHEMA_NAME
            if (schemeField.type == org.apache.thrift.protocol.TType.STRING) {
              struct.schemaName = iprot.readString();
              struct.setSchemaNameIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 4: // TABLE_NAME
            if (schemeField.type == org.apache.thrift.protocol.TType.STRING) {
              struct.tableName = iprot.readString();
              struct.setTableNameIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 5: // TABLE_TYPES
            if (schemeField.type == org.apache.thrift.protocol.TType.LIST) {
              {
                org.apache.thrift.protocol.TList _list172 = iprot.readListBegin();
                struct.tableTypes = new ArrayList<String>(_list172.size);
                String _elem173;
                for (int _i174 = 0; _i174 < _list172.size; ++_i174)
                {
                  _elem173 = iprot.readString();
                  struct.tableTypes.add(_elem173);
                }
                iprot.readListEnd();
              }
              struct.setTableTypesIsSet(true);
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

    public void write(org.apache.thrift.protocol.TProtocol oprot, TGetTablesReq struct) throws org.apache.thrift.TException {
      struct.validate();

      oprot.writeStructBegin(STRUCT_DESC);
      if (struct.sessionHandle != null) {
        oprot.writeFieldBegin(SESSION_HANDLE_FIELD_DESC);
        struct.sessionHandle.write(oprot);
        oprot.writeFieldEnd();
      }
      if (struct.catalogName != null) {
        if (struct.isSetCatalogName()) {
          oprot.writeFieldBegin(CATALOG_NAME_FIELD_DESC);
          oprot.writeString(struct.catalogName);
          oprot.writeFieldEnd();
        }
      }
      if (struct.schemaName != null) {
        if (struct.isSetSchemaName()) {
          oprot.writeFieldBegin(SCHEMA_NAME_FIELD_DESC);
          oprot.writeString(struct.schemaName);
          oprot.writeFieldEnd();
        }
      }
      if (struct.tableName != null) {
        if (struct.isSetTableName()) {
          oprot.writeFieldBegin(TABLE_NAME_FIELD_DESC);
          oprot.writeString(struct.tableName);
          oprot.writeFieldEnd();
        }
      }
      if (struct.tableTypes != null) {
        if (struct.isSetTableTypes()) {
          oprot.writeFieldBegin(TABLE_TYPES_FIELD_DESC);
          {
            oprot.writeListBegin(new org.apache.thrift.protocol.TList(org.apache.thrift.protocol.TType.STRING, struct.tableTypes.size()));
            for (String _iter183 : struct.tableTypes)
            {
              oprot.writeString(_iter183);
            }
            oprot.writeListEnd();
          }
          oprot.writeFieldEnd();
        }
      }
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

  }

  private static class TGetTablesReqTupleSchemeFactory implements SchemeFactory {
    public TGetTablesReqTupleScheme getScheme() {
      return new TGetTablesReqTupleScheme();
    }
  }

  private static class TGetTablesReqTupleScheme extends TupleScheme<TGetTablesReq> {

    @Override
    public void write(org.apache.thrift.protocol.TProtocol prot, TGetTablesReq struct) throws org.apache.thrift.TException {
      TTupleProtocol oprot = (TTupleProtocol) prot;
      struct.sessionHandle.write(oprot);
      BitSet optionals = new BitSet();
      if (struct.isSetCatalogName()) {
        optionals.set(0);
      }
      if (struct.isSetSchemaName()) {
        optionals.set(1);
      }
      if (struct.isSetTableName()) {
        optionals.set(2);
      }
      if (struct.isSetTableTypes()) {
        optionals.set(3);
      }
      oprot.writeBitSet(optionals, 4);
      if (struct.isSetCatalogName()) {
        oprot.writeString(struct.catalogName);
      }
      if (struct.isSetSchemaName()) {
        oprot.writeString(struct.schemaName);
      }
      if (struct.isSetTableName()) {
        oprot.writeString(struct.tableName);
      }
      if (struct.isSetTableTypes()) {
        {
          oprot.writeI32(struct.tableTypes.size());
          for (String _iter184 : struct.tableTypes)
          {
            oprot.writeString(_iter184);
          }
        }
      }
    }

    @Override
    public void read(org.apache.thrift.protocol.TProtocol prot, TGetTablesReq struct) throws org.apache.thrift.TException {
      TTupleProtocol iprot = (TTupleProtocol) prot;
      struct.sessionHandle = new TSessionHandle();
      struct.sessionHandle.read(iprot);
      struct.setSessionHandleIsSet(true);
      BitSet incoming = iprot.readBitSet(4);
      if (incoming.get(0)) {
        struct.catalogName = iprot.readString();
        struct.setCatalogNameIsSet(true);
      }
      if (incoming.get(1)) {
        struct.schemaName = iprot.readString();
        struct.setSchemaNameIsSet(true);
      }
      if (incoming.get(2)) {
        struct.tableName = iprot.readString();
        struct.setTableNameIsSet(true);
      }
      if (incoming.get(3)) {
        {
          org.apache.thrift.protocol.TList _list177 = new org.apache.thrift.protocol.TList(org.apache.thrift.protocol.TType.STRING, iprot.readI32());
          struct.tableTypes = new ArrayList<String>(_list177.size);
          String _elem178;
          for (int _i179 = 0; _i179 < _list177.size; ++_i179)
          {
            _elem178 = iprot.readString();
            struct.tableTypes.add(_elem178);
          }
        }
        struct.setTableTypesIsSet(true);
      }
    }
  }

}

