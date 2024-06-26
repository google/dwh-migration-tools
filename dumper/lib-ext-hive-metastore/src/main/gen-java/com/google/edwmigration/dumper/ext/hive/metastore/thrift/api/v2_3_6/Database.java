/**
 * Autogenerated by Thrift Compiler (0.17.0)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *  @generated
 */
package com.google.edwmigration.dumper.ext.hive.metastore.thrift.api.v2_3_6;

@SuppressWarnings({"cast", "rawtypes", "serial", "unchecked", "unused"})
@javax.annotation.Generated(value = "Autogenerated by Thrift Compiler (0.17.0)", date = "2023-08-11")
public class Database implements org.apache.thrift.TBase<Database, Database._Fields>, java.io.Serializable, Cloneable, Comparable<Database> {
  private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct("Database");

  private static final org.apache.thrift.protocol.TField NAME_FIELD_DESC = new org.apache.thrift.protocol.TField("name", org.apache.thrift.protocol.TType.STRING, (short)1);
  private static final org.apache.thrift.protocol.TField DESCRIPTION_FIELD_DESC = new org.apache.thrift.protocol.TField("description", org.apache.thrift.protocol.TType.STRING, (short)2);
  private static final org.apache.thrift.protocol.TField LOCATION_URI_FIELD_DESC = new org.apache.thrift.protocol.TField("locationUri", org.apache.thrift.protocol.TType.STRING, (short)3);
  private static final org.apache.thrift.protocol.TField PARAMETERS_FIELD_DESC = new org.apache.thrift.protocol.TField("parameters", org.apache.thrift.protocol.TType.MAP, (short)4);
  private static final org.apache.thrift.protocol.TField PRIVILEGES_FIELD_DESC = new org.apache.thrift.protocol.TField("privileges", org.apache.thrift.protocol.TType.STRUCT, (short)5);
  private static final org.apache.thrift.protocol.TField OWNER_NAME_FIELD_DESC = new org.apache.thrift.protocol.TField("ownerName", org.apache.thrift.protocol.TType.STRING, (short)6);
  private static final org.apache.thrift.protocol.TField OWNER_TYPE_FIELD_DESC = new org.apache.thrift.protocol.TField("ownerType", org.apache.thrift.protocol.TType.I32, (short)7);

  private static final org.apache.thrift.scheme.SchemeFactory STANDARD_SCHEME_FACTORY = new DatabaseStandardSchemeFactory();
  private static final org.apache.thrift.scheme.SchemeFactory TUPLE_SCHEME_FACTORY = new DatabaseTupleSchemeFactory();

  public @org.apache.thrift.annotation.Nullable java.lang.String name; // required
  public @org.apache.thrift.annotation.Nullable java.lang.String description; // required
  public @org.apache.thrift.annotation.Nullable java.lang.String locationUri; // required
  public @org.apache.thrift.annotation.Nullable java.util.Map<java.lang.String,java.lang.String> parameters; // required
  public @org.apache.thrift.annotation.Nullable PrincipalPrivilegeSet privileges; // optional
  public @org.apache.thrift.annotation.Nullable java.lang.String ownerName; // optional
  /**
   * 
   * @see PrincipalType
   */
  public @org.apache.thrift.annotation.Nullable PrincipalType ownerType; // optional

  /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
  public enum _Fields implements org.apache.thrift.TFieldIdEnum {
    NAME((short)1, "name"),
    DESCRIPTION((short)2, "description"),
    LOCATION_URI((short)3, "locationUri"),
    PARAMETERS((short)4, "parameters"),
    PRIVILEGES((short)5, "privileges"),
    OWNER_NAME((short)6, "ownerName"),
    /**
     * 
     * @see PrincipalType
     */
    OWNER_TYPE((short)7, "ownerType");

    private static final java.util.Map<java.lang.String, _Fields> byName = new java.util.HashMap<java.lang.String, _Fields>();

    static {
      for (_Fields field : java.util.EnumSet.allOf(_Fields.class)) {
        byName.put(field.getFieldName(), field);
      }
    }

    /**
     * Find the _Fields constant that matches fieldId, or null if its not found.
     */
    @org.apache.thrift.annotation.Nullable
    public static _Fields findByThriftId(int fieldId) {
      switch(fieldId) {
        case 1: // NAME
          return NAME;
        case 2: // DESCRIPTION
          return DESCRIPTION;
        case 3: // LOCATION_URI
          return LOCATION_URI;
        case 4: // PARAMETERS
          return PARAMETERS;
        case 5: // PRIVILEGES
          return PRIVILEGES;
        case 6: // OWNER_NAME
          return OWNER_NAME;
        case 7: // OWNER_TYPE
          return OWNER_TYPE;
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
      if (fields == null) throw new java.lang.IllegalArgumentException("Field " + fieldId + " doesn't exist!");
      return fields;
    }

    /**
     * Find the _Fields constant that matches name, or null if its not found.
     */
    @org.apache.thrift.annotation.Nullable
    public static _Fields findByName(java.lang.String name) {
      return byName.get(name);
    }

    private final short _thriftId;
    private final java.lang.String _fieldName;

    _Fields(short thriftId, java.lang.String fieldName) {
      _thriftId = thriftId;
      _fieldName = fieldName;
    }

    @Override
    public short getThriftFieldId() {
      return _thriftId;
    }

    @Override
    public java.lang.String getFieldName() {
      return _fieldName;
    }
  }

  // isset id assignments
  private static final _Fields optionals[] = {_Fields.PRIVILEGES,_Fields.OWNER_NAME,_Fields.OWNER_TYPE};
  public static final java.util.Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> metaDataMap;
  static {
    java.util.Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> tmpMap = new java.util.EnumMap<_Fields, org.apache.thrift.meta_data.FieldMetaData>(_Fields.class);
    tmpMap.put(_Fields.NAME, new org.apache.thrift.meta_data.FieldMetaData("name", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING)));
    tmpMap.put(_Fields.DESCRIPTION, new org.apache.thrift.meta_data.FieldMetaData("description", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING)));
    tmpMap.put(_Fields.LOCATION_URI, new org.apache.thrift.meta_data.FieldMetaData("locationUri", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING)));
    tmpMap.put(_Fields.PARAMETERS, new org.apache.thrift.meta_data.FieldMetaData("parameters", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.MapMetaData(org.apache.thrift.protocol.TType.MAP, 
            new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING), 
            new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING))));
    tmpMap.put(_Fields.PRIVILEGES, new org.apache.thrift.meta_data.FieldMetaData("privileges", org.apache.thrift.TFieldRequirementType.OPTIONAL, 
        new org.apache.thrift.meta_data.StructMetaData(org.apache.thrift.protocol.TType.STRUCT, PrincipalPrivilegeSet.class)));
    tmpMap.put(_Fields.OWNER_NAME, new org.apache.thrift.meta_data.FieldMetaData("ownerName", org.apache.thrift.TFieldRequirementType.OPTIONAL, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING)));
    tmpMap.put(_Fields.OWNER_TYPE, new org.apache.thrift.meta_data.FieldMetaData("ownerType", org.apache.thrift.TFieldRequirementType.OPTIONAL, 
        new org.apache.thrift.meta_data.EnumMetaData(org.apache.thrift.protocol.TType.ENUM, PrincipalType.class)));
    metaDataMap = java.util.Collections.unmodifiableMap(tmpMap);
    org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(Database.class, metaDataMap);
  }

  public Database() {
  }

  public Database(
    java.lang.String name,
    java.lang.String description,
    java.lang.String locationUri,
    java.util.Map<java.lang.String,java.lang.String> parameters)
  {
    this();
    this.name = name;
    this.description = description;
    this.locationUri = locationUri;
    this.parameters = parameters;
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public Database(Database other) {
    if (other.isSetName()) {
      this.name = other.name;
    }
    if (other.isSetDescription()) {
      this.description = other.description;
    }
    if (other.isSetLocationUri()) {
      this.locationUri = other.locationUri;
    }
    if (other.isSetParameters()) {
      java.util.Map<java.lang.String,java.lang.String> __this__parameters = new java.util.HashMap<java.lang.String,java.lang.String>(other.parameters);
      this.parameters = __this__parameters;
    }
    if (other.isSetPrivileges()) {
      this.privileges = new PrincipalPrivilegeSet(other.privileges);
    }
    if (other.isSetOwnerName()) {
      this.ownerName = other.ownerName;
    }
    if (other.isSetOwnerType()) {
      this.ownerType = other.ownerType;
    }
  }

  @Override
  public Database deepCopy() {
    return new Database(this);
  }

  @Override
  public void clear() {
    this.name = null;
    this.description = null;
    this.locationUri = null;
    this.parameters = null;
    this.privileges = null;
    this.ownerName = null;
    this.ownerType = null;
  }

  @org.apache.thrift.annotation.Nullable
  public java.lang.String getName() {
    return this.name;
  }

  public Database setName(@org.apache.thrift.annotation.Nullable java.lang.String name) {
    this.name = name;
    return this;
  }

  public void unsetName() {
    this.name = null;
  }

  /** Returns true if field name is set (has been assigned a value) and false otherwise */
  public boolean isSetName() {
    return this.name != null;
  }

  public void setNameIsSet(boolean value) {
    if (!value) {
      this.name = null;
    }
  }

  @org.apache.thrift.annotation.Nullable
  public java.lang.String getDescription() {
    return this.description;
  }

  public Database setDescription(@org.apache.thrift.annotation.Nullable java.lang.String description) {
    this.description = description;
    return this;
  }

  public void unsetDescription() {
    this.description = null;
  }

  /** Returns true if field description is set (has been assigned a value) and false otherwise */
  public boolean isSetDescription() {
    return this.description != null;
  }

  public void setDescriptionIsSet(boolean value) {
    if (!value) {
      this.description = null;
    }
  }

  @org.apache.thrift.annotation.Nullable
  public java.lang.String getLocationUri() {
    return this.locationUri;
  }

  public Database setLocationUri(@org.apache.thrift.annotation.Nullable java.lang.String locationUri) {
    this.locationUri = locationUri;
    return this;
  }

  public void unsetLocationUri() {
    this.locationUri = null;
  }

  /** Returns true if field locationUri is set (has been assigned a value) and false otherwise */
  public boolean isSetLocationUri() {
    return this.locationUri != null;
  }

  public void setLocationUriIsSet(boolean value) {
    if (!value) {
      this.locationUri = null;
    }
  }

  public int getParametersSize() {
    return (this.parameters == null) ? 0 : this.parameters.size();
  }

  public void putToParameters(java.lang.String key, java.lang.String val) {
    if (this.parameters == null) {
      this.parameters = new java.util.HashMap<java.lang.String,java.lang.String>();
    }
    this.parameters.put(key, val);
  }

  @org.apache.thrift.annotation.Nullable
  public java.util.Map<java.lang.String,java.lang.String> getParameters() {
    return this.parameters;
  }

  public Database setParameters(@org.apache.thrift.annotation.Nullable java.util.Map<java.lang.String,java.lang.String> parameters) {
    this.parameters = parameters;
    return this;
  }

  public void unsetParameters() {
    this.parameters = null;
  }

  /** Returns true if field parameters is set (has been assigned a value) and false otherwise */
  public boolean isSetParameters() {
    return this.parameters != null;
  }

  public void setParametersIsSet(boolean value) {
    if (!value) {
      this.parameters = null;
    }
  }

  @org.apache.thrift.annotation.Nullable
  public PrincipalPrivilegeSet getPrivileges() {
    return this.privileges;
  }

  public Database setPrivileges(@org.apache.thrift.annotation.Nullable PrincipalPrivilegeSet privileges) {
    this.privileges = privileges;
    return this;
  }

  public void unsetPrivileges() {
    this.privileges = null;
  }

  /** Returns true if field privileges is set (has been assigned a value) and false otherwise */
  public boolean isSetPrivileges() {
    return this.privileges != null;
  }

  public void setPrivilegesIsSet(boolean value) {
    if (!value) {
      this.privileges = null;
    }
  }

  @org.apache.thrift.annotation.Nullable
  public java.lang.String getOwnerName() {
    return this.ownerName;
  }

  public Database setOwnerName(@org.apache.thrift.annotation.Nullable java.lang.String ownerName) {
    this.ownerName = ownerName;
    return this;
  }

  public void unsetOwnerName() {
    this.ownerName = null;
  }

  /** Returns true if field ownerName is set (has been assigned a value) and false otherwise */
  public boolean isSetOwnerName() {
    return this.ownerName != null;
  }

  public void setOwnerNameIsSet(boolean value) {
    if (!value) {
      this.ownerName = null;
    }
  }

  /**
   * 
   * @see PrincipalType
   */
  @org.apache.thrift.annotation.Nullable
  public PrincipalType getOwnerType() {
    return this.ownerType;
  }

  /**
   * 
   * @see PrincipalType
   */
  public Database setOwnerType(@org.apache.thrift.annotation.Nullable PrincipalType ownerType) {
    this.ownerType = ownerType;
    return this;
  }

  public void unsetOwnerType() {
    this.ownerType = null;
  }

  /** Returns true if field ownerType is set (has been assigned a value) and false otherwise */
  public boolean isSetOwnerType() {
    return this.ownerType != null;
  }

  public void setOwnerTypeIsSet(boolean value) {
    if (!value) {
      this.ownerType = null;
    }
  }

  @Override
  public void setFieldValue(_Fields field, @org.apache.thrift.annotation.Nullable java.lang.Object value) {
    switch (field) {
    case NAME:
      if (value == null) {
        unsetName();
      } else {
        setName((java.lang.String)value);
      }
      break;

    case DESCRIPTION:
      if (value == null) {
        unsetDescription();
      } else {
        setDescription((java.lang.String)value);
      }
      break;

    case LOCATION_URI:
      if (value == null) {
        unsetLocationUri();
      } else {
        setLocationUri((java.lang.String)value);
      }
      break;

    case PARAMETERS:
      if (value == null) {
        unsetParameters();
      } else {
        setParameters((java.util.Map<java.lang.String,java.lang.String>)value);
      }
      break;

    case PRIVILEGES:
      if (value == null) {
        unsetPrivileges();
      } else {
        setPrivileges((PrincipalPrivilegeSet)value);
      }
      break;

    case OWNER_NAME:
      if (value == null) {
        unsetOwnerName();
      } else {
        setOwnerName((java.lang.String)value);
      }
      break;

    case OWNER_TYPE:
      if (value == null) {
        unsetOwnerType();
      } else {
        setOwnerType((PrincipalType)value);
      }
      break;

    }
  }

  @org.apache.thrift.annotation.Nullable
  @Override
  public java.lang.Object getFieldValue(_Fields field) {
    switch (field) {
    case NAME:
      return getName();

    case DESCRIPTION:
      return getDescription();

    case LOCATION_URI:
      return getLocationUri();

    case PARAMETERS:
      return getParameters();

    case PRIVILEGES:
      return getPrivileges();

    case OWNER_NAME:
      return getOwnerName();

    case OWNER_TYPE:
      return getOwnerType();

    }
    throw new java.lang.IllegalStateException();
  }

  /** Returns true if field corresponding to fieldID is set (has been assigned a value) and false otherwise */
  @Override
  public boolean isSet(_Fields field) {
    if (field == null) {
      throw new java.lang.IllegalArgumentException();
    }

    switch (field) {
    case NAME:
      return isSetName();
    case DESCRIPTION:
      return isSetDescription();
    case LOCATION_URI:
      return isSetLocationUri();
    case PARAMETERS:
      return isSetParameters();
    case PRIVILEGES:
      return isSetPrivileges();
    case OWNER_NAME:
      return isSetOwnerName();
    case OWNER_TYPE:
      return isSetOwnerType();
    }
    throw new java.lang.IllegalStateException();
  }

  @Override
  public boolean equals(java.lang.Object that) {
    if (that instanceof Database)
      return this.equals((Database)that);
    return false;
  }

  public boolean equals(Database that) {
    if (that == null)
      return false;
    if (this == that)
      return true;

    boolean this_present_name = true && this.isSetName();
    boolean that_present_name = true && that.isSetName();
    if (this_present_name || that_present_name) {
      if (!(this_present_name && that_present_name))
        return false;
      if (!this.name.equals(that.name))
        return false;
    }

    boolean this_present_description = true && this.isSetDescription();
    boolean that_present_description = true && that.isSetDescription();
    if (this_present_description || that_present_description) {
      if (!(this_present_description && that_present_description))
        return false;
      if (!this.description.equals(that.description))
        return false;
    }

    boolean this_present_locationUri = true && this.isSetLocationUri();
    boolean that_present_locationUri = true && that.isSetLocationUri();
    if (this_present_locationUri || that_present_locationUri) {
      if (!(this_present_locationUri && that_present_locationUri))
        return false;
      if (!this.locationUri.equals(that.locationUri))
        return false;
    }

    boolean this_present_parameters = true && this.isSetParameters();
    boolean that_present_parameters = true && that.isSetParameters();
    if (this_present_parameters || that_present_parameters) {
      if (!(this_present_parameters && that_present_parameters))
        return false;
      if (!this.parameters.equals(that.parameters))
        return false;
    }

    boolean this_present_privileges = true && this.isSetPrivileges();
    boolean that_present_privileges = true && that.isSetPrivileges();
    if (this_present_privileges || that_present_privileges) {
      if (!(this_present_privileges && that_present_privileges))
        return false;
      if (!this.privileges.equals(that.privileges))
        return false;
    }

    boolean this_present_ownerName = true && this.isSetOwnerName();
    boolean that_present_ownerName = true && that.isSetOwnerName();
    if (this_present_ownerName || that_present_ownerName) {
      if (!(this_present_ownerName && that_present_ownerName))
        return false;
      if (!this.ownerName.equals(that.ownerName))
        return false;
    }

    boolean this_present_ownerType = true && this.isSetOwnerType();
    boolean that_present_ownerType = true && that.isSetOwnerType();
    if (this_present_ownerType || that_present_ownerType) {
      if (!(this_present_ownerType && that_present_ownerType))
        return false;
      if (!this.ownerType.equals(that.ownerType))
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    int hashCode = 1;

    hashCode = hashCode * 8191 + ((isSetName()) ? 131071 : 524287);
    if (isSetName())
      hashCode = hashCode * 8191 + name.hashCode();

    hashCode = hashCode * 8191 + ((isSetDescription()) ? 131071 : 524287);
    if (isSetDescription())
      hashCode = hashCode * 8191 + description.hashCode();

    hashCode = hashCode * 8191 + ((isSetLocationUri()) ? 131071 : 524287);
    if (isSetLocationUri())
      hashCode = hashCode * 8191 + locationUri.hashCode();

    hashCode = hashCode * 8191 + ((isSetParameters()) ? 131071 : 524287);
    if (isSetParameters())
      hashCode = hashCode * 8191 + parameters.hashCode();

    hashCode = hashCode * 8191 + ((isSetPrivileges()) ? 131071 : 524287);
    if (isSetPrivileges())
      hashCode = hashCode * 8191 + privileges.hashCode();

    hashCode = hashCode * 8191 + ((isSetOwnerName()) ? 131071 : 524287);
    if (isSetOwnerName())
      hashCode = hashCode * 8191 + ownerName.hashCode();

    hashCode = hashCode * 8191 + ((isSetOwnerType()) ? 131071 : 524287);
    if (isSetOwnerType())
      hashCode = hashCode * 8191 + ownerType.getValue();

    return hashCode;
  }

  @Override
  public int compareTo(Database other) {
    if (!getClass().equals(other.getClass())) {
      return getClass().getName().compareTo(other.getClass().getName());
    }

    int lastComparison = 0;

    lastComparison = java.lang.Boolean.compare(isSetName(), other.isSetName());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetName()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.name, other.name);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = java.lang.Boolean.compare(isSetDescription(), other.isSetDescription());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetDescription()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.description, other.description);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = java.lang.Boolean.compare(isSetLocationUri(), other.isSetLocationUri());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetLocationUri()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.locationUri, other.locationUri);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = java.lang.Boolean.compare(isSetParameters(), other.isSetParameters());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetParameters()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.parameters, other.parameters);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = java.lang.Boolean.compare(isSetPrivileges(), other.isSetPrivileges());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetPrivileges()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.privileges, other.privileges);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = java.lang.Boolean.compare(isSetOwnerName(), other.isSetOwnerName());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetOwnerName()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.ownerName, other.ownerName);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = java.lang.Boolean.compare(isSetOwnerType(), other.isSetOwnerType());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetOwnerType()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.ownerType, other.ownerType);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    return 0;
  }

  @org.apache.thrift.annotation.Nullable
  @Override
  public _Fields fieldForId(int fieldId) {
    return _Fields.findByThriftId(fieldId);
  }

  @Override
  public void read(org.apache.thrift.protocol.TProtocol iprot) throws org.apache.thrift.TException {
    scheme(iprot).read(iprot, this);
  }

  @Override
  public void write(org.apache.thrift.protocol.TProtocol oprot) throws org.apache.thrift.TException {
    scheme(oprot).write(oprot, this);
  }

  @Override
  public java.lang.String toString() {
    java.lang.StringBuilder sb = new java.lang.StringBuilder("Database(");
    boolean first = true;

    sb.append("name:");
    if (this.name == null) {
      sb.append("null");
    } else {
      sb.append(this.name);
    }
    first = false;
    if (!first) sb.append(", ");
    sb.append("description:");
    if (this.description == null) {
      sb.append("null");
    } else {
      sb.append(this.description);
    }
    first = false;
    if (!first) sb.append(", ");
    sb.append("locationUri:");
    if (this.locationUri == null) {
      sb.append("null");
    } else {
      sb.append(this.locationUri);
    }
    first = false;
    if (!first) sb.append(", ");
    sb.append("parameters:");
    if (this.parameters == null) {
      sb.append("null");
    } else {
      sb.append(this.parameters);
    }
    first = false;
    if (isSetPrivileges()) {
      if (!first) sb.append(", ");
      sb.append("privileges:");
      if (this.privileges == null) {
        sb.append("null");
      } else {
        sb.append(this.privileges);
      }
      first = false;
    }
    if (isSetOwnerName()) {
      if (!first) sb.append(", ");
      sb.append("ownerName:");
      if (this.ownerName == null) {
        sb.append("null");
      } else {
        sb.append(this.ownerName);
      }
      first = false;
    }
    if (isSetOwnerType()) {
      if (!first) sb.append(", ");
      sb.append("ownerType:");
      if (this.ownerType == null) {
        sb.append("null");
      } else {
        sb.append(this.ownerType);
      }
      first = false;
    }
    sb.append(")");
    return sb.toString();
  }

  public void validate() throws org.apache.thrift.TException {
    // check for required fields
    // check for sub-struct validity
    if (privileges != null) {
      privileges.validate();
    }
  }

  private void writeObject(java.io.ObjectOutputStream out) throws java.io.IOException {
    try {
      write(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(out)));
    } catch (org.apache.thrift.TException te) {
      throw new java.io.IOException(te);
    }
  }

  private void readObject(java.io.ObjectInputStream in) throws java.io.IOException, java.lang.ClassNotFoundException {
    try {
      read(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(in)));
    } catch (org.apache.thrift.TException te) {
      throw new java.io.IOException(te);
    }
  }

  private static class DatabaseStandardSchemeFactory implements org.apache.thrift.scheme.SchemeFactory {
    @Override
    public DatabaseStandardScheme getScheme() {
      return new DatabaseStandardScheme();
    }
  }

  private static class DatabaseStandardScheme extends org.apache.thrift.scheme.StandardScheme<Database> {

    @Override
    public void read(org.apache.thrift.protocol.TProtocol iprot, Database struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TField schemeField;
      iprot.readStructBegin();
      while (true)
      {
        schemeField = iprot.readFieldBegin();
        if (schemeField.type == org.apache.thrift.protocol.TType.STOP) { 
          break;
        }
        switch (schemeField.id) {
          case 1: // NAME
            if (schemeField.type == org.apache.thrift.protocol.TType.STRING) {
              struct.name = iprot.readString();
              struct.setNameIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 2: // DESCRIPTION
            if (schemeField.type == org.apache.thrift.protocol.TType.STRING) {
              struct.description = iprot.readString();
              struct.setDescriptionIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 3: // LOCATION_URI
            if (schemeField.type == org.apache.thrift.protocol.TType.STRING) {
              struct.locationUri = iprot.readString();
              struct.setLocationUriIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 4: // PARAMETERS
            if (schemeField.type == org.apache.thrift.protocol.TType.MAP) {
              {
                org.apache.thrift.protocol.TMap _map94 = iprot.readMapBegin();
                struct.parameters = new java.util.HashMap<java.lang.String,java.lang.String>(2*_map94.size);
                @org.apache.thrift.annotation.Nullable java.lang.String _key95;
                @org.apache.thrift.annotation.Nullable java.lang.String _val96;
                for (int _i97 = 0; _i97 < _map94.size; ++_i97)
                {
                  _key95 = iprot.readString();
                  _val96 = iprot.readString();
                  struct.parameters.put(_key95, _val96);
                }
                iprot.readMapEnd();
              }
              struct.setParametersIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 5: // PRIVILEGES
            if (schemeField.type == org.apache.thrift.protocol.TType.STRUCT) {
              struct.privileges = new PrincipalPrivilegeSet();
              struct.privileges.read(iprot);
              struct.setPrivilegesIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 6: // OWNER_NAME
            if (schemeField.type == org.apache.thrift.protocol.TType.STRING) {
              struct.ownerName = iprot.readString();
              struct.setOwnerNameIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 7: // OWNER_TYPE
            if (schemeField.type == org.apache.thrift.protocol.TType.I32) {
              struct.ownerType = com.google.edwmigration.dumper.ext.hive.metastore.thrift.api.v2_3_6.PrincipalType.findByValue(iprot.readI32());
              struct.setOwnerTypeIsSet(true);
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

      // check for required fields of primitive type, which can't be checked in the validate method
      struct.validate();
    }

    @Override
    public void write(org.apache.thrift.protocol.TProtocol oprot, Database struct) throws org.apache.thrift.TException {
      struct.validate();

      oprot.writeStructBegin(STRUCT_DESC);
      if (struct.name != null) {
        oprot.writeFieldBegin(NAME_FIELD_DESC);
        oprot.writeString(struct.name);
        oprot.writeFieldEnd();
      }
      if (struct.description != null) {
        oprot.writeFieldBegin(DESCRIPTION_FIELD_DESC);
        oprot.writeString(struct.description);
        oprot.writeFieldEnd();
      }
      if (struct.locationUri != null) {
        oprot.writeFieldBegin(LOCATION_URI_FIELD_DESC);
        oprot.writeString(struct.locationUri);
        oprot.writeFieldEnd();
      }
      if (struct.parameters != null) {
        oprot.writeFieldBegin(PARAMETERS_FIELD_DESC);
        {
          oprot.writeMapBegin(new org.apache.thrift.protocol.TMap(org.apache.thrift.protocol.TType.STRING, org.apache.thrift.protocol.TType.STRING, struct.parameters.size()));
          for (java.util.Map.Entry<java.lang.String, java.lang.String> _iter98 : struct.parameters.entrySet())
          {
            oprot.writeString(_iter98.getKey());
            oprot.writeString(_iter98.getValue());
          }
          oprot.writeMapEnd();
        }
        oprot.writeFieldEnd();
      }
      if (struct.privileges != null) {
        if (struct.isSetPrivileges()) {
          oprot.writeFieldBegin(PRIVILEGES_FIELD_DESC);
          struct.privileges.write(oprot);
          oprot.writeFieldEnd();
        }
      }
      if (struct.ownerName != null) {
        if (struct.isSetOwnerName()) {
          oprot.writeFieldBegin(OWNER_NAME_FIELD_DESC);
          oprot.writeString(struct.ownerName);
          oprot.writeFieldEnd();
        }
      }
      if (struct.ownerType != null) {
        if (struct.isSetOwnerType()) {
          oprot.writeFieldBegin(OWNER_TYPE_FIELD_DESC);
          oprot.writeI32(struct.ownerType.getValue());
          oprot.writeFieldEnd();
        }
      }
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

  }

  private static class DatabaseTupleSchemeFactory implements org.apache.thrift.scheme.SchemeFactory {
    @Override
    public DatabaseTupleScheme getScheme() {
      return new DatabaseTupleScheme();
    }
  }

  private static class DatabaseTupleScheme extends org.apache.thrift.scheme.TupleScheme<Database> {

    @Override
    public void write(org.apache.thrift.protocol.TProtocol prot, Database struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TTupleProtocol oprot = (org.apache.thrift.protocol.TTupleProtocol) prot;
      java.util.BitSet optionals = new java.util.BitSet();
      if (struct.isSetName()) {
        optionals.set(0);
      }
      if (struct.isSetDescription()) {
        optionals.set(1);
      }
      if (struct.isSetLocationUri()) {
        optionals.set(2);
      }
      if (struct.isSetParameters()) {
        optionals.set(3);
      }
      if (struct.isSetPrivileges()) {
        optionals.set(4);
      }
      if (struct.isSetOwnerName()) {
        optionals.set(5);
      }
      if (struct.isSetOwnerType()) {
        optionals.set(6);
      }
      oprot.writeBitSet(optionals, 7);
      if (struct.isSetName()) {
        oprot.writeString(struct.name);
      }
      if (struct.isSetDescription()) {
        oprot.writeString(struct.description);
      }
      if (struct.isSetLocationUri()) {
        oprot.writeString(struct.locationUri);
      }
      if (struct.isSetParameters()) {
        {
          oprot.writeI32(struct.parameters.size());
          for (java.util.Map.Entry<java.lang.String, java.lang.String> _iter99 : struct.parameters.entrySet())
          {
            oprot.writeString(_iter99.getKey());
            oprot.writeString(_iter99.getValue());
          }
        }
      }
      if (struct.isSetPrivileges()) {
        struct.privileges.write(oprot);
      }
      if (struct.isSetOwnerName()) {
        oprot.writeString(struct.ownerName);
      }
      if (struct.isSetOwnerType()) {
        oprot.writeI32(struct.ownerType.getValue());
      }
    }

    @Override
    public void read(org.apache.thrift.protocol.TProtocol prot, Database struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TTupleProtocol iprot = (org.apache.thrift.protocol.TTupleProtocol) prot;
      java.util.BitSet incoming = iprot.readBitSet(7);
      if (incoming.get(0)) {
        struct.name = iprot.readString();
        struct.setNameIsSet(true);
      }
      if (incoming.get(1)) {
        struct.description = iprot.readString();
        struct.setDescriptionIsSet(true);
      }
      if (incoming.get(2)) {
        struct.locationUri = iprot.readString();
        struct.setLocationUriIsSet(true);
      }
      if (incoming.get(3)) {
        {
          org.apache.thrift.protocol.TMap _map100 = iprot.readMapBegin(org.apache.thrift.protocol.TType.STRING, org.apache.thrift.protocol.TType.STRING); 
          struct.parameters = new java.util.HashMap<java.lang.String,java.lang.String>(2*_map100.size);
          @org.apache.thrift.annotation.Nullable java.lang.String _key101;
          @org.apache.thrift.annotation.Nullable java.lang.String _val102;
          for (int _i103 = 0; _i103 < _map100.size; ++_i103)
          {
            _key101 = iprot.readString();
            _val102 = iprot.readString();
            struct.parameters.put(_key101, _val102);
          }
        }
        struct.setParametersIsSet(true);
      }
      if (incoming.get(4)) {
        struct.privileges = new PrincipalPrivilegeSet();
        struct.privileges.read(iprot);
        struct.setPrivilegesIsSet(true);
      }
      if (incoming.get(5)) {
        struct.ownerName = iprot.readString();
        struct.setOwnerNameIsSet(true);
      }
      if (incoming.get(6)) {
        struct.ownerType = com.google.edwmigration.dumper.ext.hive.metastore.thrift.api.v2_3_6.PrincipalType.findByValue(iprot.readI32());
        struct.setOwnerTypeIsSet(true);
      }
    }
  }

  private static <S extends org.apache.thrift.scheme.IScheme> S scheme(org.apache.thrift.protocol.TProtocol proto) {
    return (org.apache.thrift.scheme.StandardScheme.class.equals(proto.getScheme()) ? STANDARD_SCHEME_FACTORY : TUPLE_SCHEME_FACTORY).getScheme();
  }
}

