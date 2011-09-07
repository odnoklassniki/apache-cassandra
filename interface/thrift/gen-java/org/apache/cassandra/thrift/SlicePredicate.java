/**
 * Autogenerated by Thrift
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 */
package org.apache.cassandra.thrift;

import org.apache.commons.lang.builder.HashCodeBuilder;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A SlicePredicate is similar to a mathematic predicate (see http://en.wikipedia.org/wiki/Predicate_(mathematical_logic)),
 * which is described as "a property that the elements of a set have in common."
 * 
 * SlicePredicate's in Cassandra are described with either a list of column_names or a SliceRange.  If column_names is
 * specified, slice_range is ignored.
 * 
 * @param column_name. A list of column names to retrieve. This can be used similar to Memcached's "multi-get" feature
 *                     to fetch N known column names. For instance, if you know you wish to fetch columns 'Joe', 'Jack',
 *                     and 'Jim' you can pass those column names as a list to fetch all three at once.
 * @param slice_range. A SliceRange describing how to range, order, and/or limit the slice.
 */
public class SlicePredicate implements org.apache.thrift.TBase<SlicePredicate, SlicePredicate._Fields>, java.io.Serializable, Cloneable {
  private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct("SlicePredicate");

  private static final org.apache.thrift.protocol.TField COLUMN_NAMES_FIELD_DESC = new org.apache.thrift.protocol.TField("column_names", org.apache.thrift.protocol.TType.LIST, (short)1);
  private static final org.apache.thrift.protocol.TField SLICE_RANGE_FIELD_DESC = new org.apache.thrift.protocol.TField("slice_range", org.apache.thrift.protocol.TType.STRUCT, (short)2);

  public List<ByteBuffer> column_names;
  public SliceRange slice_range;

  /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
  public enum _Fields implements org.apache.thrift.TFieldIdEnum {
    COLUMN_NAMES((short)1, "column_names"),
    SLICE_RANGE((short)2, "slice_range");

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
        case 1: // COLUMN_NAMES
          return COLUMN_NAMES;
        case 2: // SLICE_RANGE
          return SLICE_RANGE;
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

  public static final Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> metaDataMap;
  static {
    Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> tmpMap = new EnumMap<_Fields, org.apache.thrift.meta_data.FieldMetaData>(_Fields.class);
    tmpMap.put(_Fields.COLUMN_NAMES, new org.apache.thrift.meta_data.FieldMetaData("column_names", org.apache.thrift.TFieldRequirementType.OPTIONAL, 
        new org.apache.thrift.meta_data.ListMetaData(org.apache.thrift.protocol.TType.LIST, 
            new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING            , true))));
    tmpMap.put(_Fields.SLICE_RANGE, new org.apache.thrift.meta_data.FieldMetaData("slice_range", org.apache.thrift.TFieldRequirementType.OPTIONAL, 
        new org.apache.thrift.meta_data.StructMetaData(org.apache.thrift.protocol.TType.STRUCT, SliceRange.class)));
    metaDataMap = Collections.unmodifiableMap(tmpMap);
    org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(SlicePredicate.class, metaDataMap);
  }

  public SlicePredicate() {
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public SlicePredicate(SlicePredicate other) {
    if (other.isSetColumn_names()) {
      List<ByteBuffer> __this__column_names = new ArrayList<ByteBuffer>();
      for (ByteBuffer other_element : other.column_names) {
        ByteBuffer temp_binary_element = org.apache.thrift.TBaseHelper.copyBinary(other_element);
;
        __this__column_names.add(temp_binary_element);
      }
      this.column_names = __this__column_names;
    }
    if (other.isSetSlice_range()) {
      this.slice_range = new SliceRange(other.slice_range);
    }
  }

  public SlicePredicate deepCopy() {
    return new SlicePredicate(this);
  }

  @Override
  public void clear() {
    this.column_names = null;
    this.slice_range = null;
  }

  public int getColumn_namesSize() {
    return (this.column_names == null) ? 0 : this.column_names.size();
  }

  public java.util.Iterator<ByteBuffer> getColumn_namesIterator() {
    return (this.column_names == null) ? null : this.column_names.iterator();
  }

  public void addToColumn_names(ByteBuffer elem) {
    if (this.column_names == null) {
      this.column_names = new ArrayList<ByteBuffer>();
    }
    this.column_names.add(elem);
  }

  public List<ByteBuffer> getColumn_names() {
    return this.column_names;
  }

  public SlicePredicate setColumn_names(List<ByteBuffer> column_names) {
    this.column_names = column_names;
    return this;
  }

  public void unsetColumn_names() {
    this.column_names = null;
  }

  /** Returns true if field column_names is set (has been assigned a value) and false otherwise */
  public boolean isSetColumn_names() {
    return this.column_names != null;
  }

  public void setColumn_namesIsSet(boolean value) {
    if (!value) {
      this.column_names = null;
    }
  }

  public SliceRange getSlice_range() {
    return this.slice_range;
  }

  public SlicePredicate setSlice_range(SliceRange slice_range) {
    this.slice_range = slice_range;
    return this;
  }

  public void unsetSlice_range() {
    this.slice_range = null;
  }

  /** Returns true if field slice_range is set (has been assigned a value) and false otherwise */
  public boolean isSetSlice_range() {
    return this.slice_range != null;
  }

  public void setSlice_rangeIsSet(boolean value) {
    if (!value) {
      this.slice_range = null;
    }
  }

  public void setFieldValue(_Fields field, Object value) {
    switch (field) {
    case COLUMN_NAMES:
      if (value == null) {
        unsetColumn_names();
      } else {
        setColumn_names((List<ByteBuffer>)value);
      }
      break;

    case SLICE_RANGE:
      if (value == null) {
        unsetSlice_range();
      } else {
        setSlice_range((SliceRange)value);
      }
      break;

    }
  }

  public Object getFieldValue(_Fields field) {
    switch (field) {
    case COLUMN_NAMES:
      return getColumn_names();

    case SLICE_RANGE:
      return getSlice_range();

    }
    throw new IllegalStateException();
  }

  /** Returns true if field corresponding to fieldID is set (has been assigned a value) and false otherwise */
  public boolean isSet(_Fields field) {
    if (field == null) {
      throw new IllegalArgumentException();
    }

    switch (field) {
    case COLUMN_NAMES:
      return isSetColumn_names();
    case SLICE_RANGE:
      return isSetSlice_range();
    }
    throw new IllegalStateException();
  }

  @Override
  public boolean equals(Object that) {
    if (that == null)
      return false;
    if (that instanceof SlicePredicate)
      return this.equals((SlicePredicate)that);
    return false;
  }

  public boolean equals(SlicePredicate that) {
    if (that == null)
      return false;

    boolean this_present_column_names = true && this.isSetColumn_names();
    boolean that_present_column_names = true && that.isSetColumn_names();
    if (this_present_column_names || that_present_column_names) {
      if (!(this_present_column_names && that_present_column_names))
        return false;
      if (!this.column_names.equals(that.column_names))
        return false;
    }

    boolean this_present_slice_range = true && this.isSetSlice_range();
    boolean that_present_slice_range = true && that.isSetSlice_range();
    if (this_present_slice_range || that_present_slice_range) {
      if (!(this_present_slice_range && that_present_slice_range))
        return false;
      if (!this.slice_range.equals(that.slice_range))
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    HashCodeBuilder builder = new HashCodeBuilder();

    boolean present_column_names = true && (isSetColumn_names());
    builder.append(present_column_names);
    if (present_column_names)
      builder.append(column_names);

    boolean present_slice_range = true && (isSetSlice_range());
    builder.append(present_slice_range);
    if (present_slice_range)
      builder.append(slice_range);

    return builder.toHashCode();
  }

  public int compareTo(SlicePredicate other) {
    if (!getClass().equals(other.getClass())) {
      return getClass().getName().compareTo(other.getClass().getName());
    }

    int lastComparison = 0;
    SlicePredicate typedOther = (SlicePredicate)other;

    lastComparison = Boolean.valueOf(isSetColumn_names()).compareTo(typedOther.isSetColumn_names());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetColumn_names()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.column_names, typedOther.column_names);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetSlice_range()).compareTo(typedOther.isSetSlice_range());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetSlice_range()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.slice_range, typedOther.slice_range);
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
    org.apache.thrift.protocol.TField field;
    iprot.readStructBegin();
    while (true)
    {
      field = iprot.readFieldBegin();
      if (field.type == org.apache.thrift.protocol.TType.STOP) { 
        break;
      }
      switch (field.id) {
        case 1: // COLUMN_NAMES
          if (field.type == org.apache.thrift.protocol.TType.LIST) {
            {
              org.apache.thrift.protocol.TList _list4 = iprot.readListBegin();
              this.column_names = new ArrayList<ByteBuffer>(_list4.size);
              for (int _i5 = 0; _i5 < _list4.size; ++_i5)
              {
                ByteBuffer _elem6;
                _elem6 = iprot.readBinary();
                this.column_names.add(_elem6);
              }
              iprot.readListEnd();
            }
          } else { 
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot, field.type);
          }
          break;
        case 2: // SLICE_RANGE
          if (field.type == org.apache.thrift.protocol.TType.STRUCT) {
            this.slice_range = new SliceRange();
            this.slice_range.read(iprot);
          } else { 
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot, field.type);
          }
          break;
        default:
          org.apache.thrift.protocol.TProtocolUtil.skip(iprot, field.type);
      }
      iprot.readFieldEnd();
    }
    iprot.readStructEnd();

    // check for required fields of primitive type, which can't be checked in the validate method
    validate();
  }

  public void write(org.apache.thrift.protocol.TProtocol oprot) throws org.apache.thrift.TException {
    validate();

    oprot.writeStructBegin(STRUCT_DESC);
    if (this.column_names != null) {
      if (isSetColumn_names()) {
        oprot.writeFieldBegin(COLUMN_NAMES_FIELD_DESC);
        {
          oprot.writeListBegin(new org.apache.thrift.protocol.TList(org.apache.thrift.protocol.TType.STRING, this.column_names.size()));
          for (ByteBuffer _iter7 : this.column_names)
          {
            oprot.writeBinary(_iter7);
          }
          oprot.writeListEnd();
        }
        oprot.writeFieldEnd();
      }
    }
    if (this.slice_range != null) {
      if (isSetSlice_range()) {
        oprot.writeFieldBegin(SLICE_RANGE_FIELD_DESC);
        this.slice_range.write(oprot);
        oprot.writeFieldEnd();
      }
    }
    oprot.writeFieldStop();
    oprot.writeStructEnd();
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("SlicePredicate(");
    boolean first = true;

    if (isSetColumn_names()) {
      sb.append("column_names:");
      if (this.column_names == null) {
        sb.append("null");
      } else {
        sb.append(this.column_names);
      }
      first = false;
    }
    if (isSetSlice_range()) {
      if (!first) sb.append(", ");
      sb.append("slice_range:");
      if (this.slice_range == null) {
        sb.append("null");
      } else {
        sb.append(this.slice_range);
      }
      first = false;
    }
    sb.append(")");
    return sb.toString();
  }

  public void validate() throws org.apache.thrift.TException {
    // check for required fields
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

}

