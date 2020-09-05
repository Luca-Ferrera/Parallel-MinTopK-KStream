/**
 * Autogenerated by Avro
 *
 * DO NOT EDIT DIRECTLY
 */
package myapp.avro;

import org.apache.avro.generic.GenericArray;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.util.Utf8;
import org.apache.avro.message.BinaryMessageEncoder;
import org.apache.avro.message.BinaryMessageDecoder;
import org.apache.avro.message.SchemaStore;

@org.apache.avro.specific.AvroGenerated
public class MovieIncome extends org.apache.avro.specific.SpecificRecordBase implements org.apache.avro.specific.SpecificRecord {
  private static final long serialVersionUID = -5297508671515776095L;
  public static final org.apache.avro.Schema SCHEMA$ = new org.apache.avro.Schema.Parser().parse("{\"type\":\"record\",\"name\":\"MovieIncome\",\"namespace\":\"myapp.avro\",\"fields\":[{\"name\":\"id\",\"type\":\"long\"},{\"name\":\"title\",\"type\":\"string\"},{\"name\":\"income\",\"type\":\"double\"},{\"name\":\"modified\",\"type\":\"long\",\"logicalType\":\"timestamp-millis\"}]}");
  public static org.apache.avro.Schema getClassSchema() { return SCHEMA$; }

  private static SpecificData MODEL$ = new SpecificData();

  private static final BinaryMessageEncoder<MovieIncome> ENCODER =
      new BinaryMessageEncoder<MovieIncome>(MODEL$, SCHEMA$);

  private static final BinaryMessageDecoder<MovieIncome> DECODER =
      new BinaryMessageDecoder<MovieIncome>(MODEL$, SCHEMA$);

  /**
   * Return the BinaryMessageEncoder instance used by this class.
   * @return the message encoder used by this class
   */
  public static BinaryMessageEncoder<MovieIncome> getEncoder() {
    return ENCODER;
  }

  /**
   * Return the BinaryMessageDecoder instance used by this class.
   * @return the message decoder used by this class
   */
  public static BinaryMessageDecoder<MovieIncome> getDecoder() {
    return DECODER;
  }

  /**
   * Create a new BinaryMessageDecoder instance for this class that uses the specified {@link SchemaStore}.
   * @param resolver a {@link SchemaStore} used to find schemas by fingerprint
   * @return a BinaryMessageDecoder instance for this class backed by the given SchemaStore
   */
  public static BinaryMessageDecoder<MovieIncome> createDecoder(SchemaStore resolver) {
    return new BinaryMessageDecoder<MovieIncome>(MODEL$, SCHEMA$, resolver);
  }

  /**
   * Serializes this MovieIncome to a ByteBuffer.
   * @return a buffer holding the serialized data for this instance
   * @throws java.io.IOException if this instance could not be serialized
   */
  public java.nio.ByteBuffer toByteBuffer() throws java.io.IOException {
    return ENCODER.encode(this);
  }

  /**
   * Deserializes a MovieIncome from a ByteBuffer.
   * @param b a byte buffer holding serialized data for an instance of this class
   * @return a MovieIncome instance decoded from the given buffer
   * @throws java.io.IOException if the given bytes could not be deserialized into an instance of this class
   */
  public static MovieIncome fromByteBuffer(
      java.nio.ByteBuffer b) throws java.io.IOException {
    return DECODER.decode(b);
  }

  @Deprecated public long id;
  @Deprecated public java.lang.CharSequence title;
  @Deprecated public double income;
  @Deprecated public long modified;

  /**
   * Default constructor.  Note that this does not initialize fields
   * to their default values from the schema.  If that is desired then
   * one should use <code>newBuilder()</code>.
   */
  public MovieIncome() {}

  /**
   * All-args constructor.
   * @param id The new value for id
   * @param title The new value for title
   * @param income The new value for income
   * @param modified The new value for modified
   */
  public MovieIncome(java.lang.Long id, java.lang.CharSequence title, java.lang.Double income, java.lang.Long modified) {
    this.id = id;
    this.title = title;
    this.income = income;
    this.modified = modified;
  }

  public org.apache.avro.specific.SpecificData getSpecificData() { return MODEL$; }
  public org.apache.avro.Schema getSchema() { return SCHEMA$; }
  // Used by DatumWriter.  Applications should not call.
  public java.lang.Object get(int field$) {
    switch (field$) {
    case 0: return id;
    case 1: return title;
    case 2: return income;
    case 3: return modified;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }

  // Used by DatumReader.  Applications should not call.
  @SuppressWarnings(value="unchecked")
  public void put(int field$, java.lang.Object value$) {
    switch (field$) {
    case 0: id = (java.lang.Long)value$; break;
    case 1: title = (java.lang.CharSequence)value$; break;
    case 2: income = (java.lang.Double)value$; break;
    case 3: modified = (java.lang.Long)value$; break;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }

  /**
   * Gets the value of the 'id' field.
   * @return The value of the 'id' field.
   */
  public long getId() {
    return id;
  }


  /**
   * Sets the value of the 'id' field.
   * @param value the value to set.
   */
  public void setId(long value) {
    this.id = value;
  }

  /**
   * Gets the value of the 'title' field.
   * @return The value of the 'title' field.
   */
  public java.lang.CharSequence getTitle() {
    return title;
  }


  /**
   * Sets the value of the 'title' field.
   * @param value the value to set.
   */
  public void setTitle(java.lang.CharSequence value) {
    this.title = value;
  }

  /**
   * Gets the value of the 'income' field.
   * @return The value of the 'income' field.
   */
  public double getIncome() {
    return income;
  }


  /**
   * Sets the value of the 'income' field.
   * @param value the value to set.
   */
  public void setIncome(double value) {
    this.income = value;
  }

  /**
   * Gets the value of the 'modified' field.
   * @return The value of the 'modified' field.
   */
  public long getModified() {
    return modified;
  }


  /**
   * Sets the value of the 'modified' field.
   * @param value the value to set.
   */
  public void setModified(long value) {
    this.modified = value;
  }

  /**
   * Creates a new MovieIncome RecordBuilder.
   * @return A new MovieIncome RecordBuilder
   */
  public static myapp.avro.MovieIncome.Builder newBuilder() {
    return new myapp.avro.MovieIncome.Builder();
  }

  /**
   * Creates a new MovieIncome RecordBuilder by copying an existing Builder.
   * @param other The existing builder to copy.
   * @return A new MovieIncome RecordBuilder
   */
  public static myapp.avro.MovieIncome.Builder newBuilder(myapp.avro.MovieIncome.Builder other) {
    if (other == null) {
      return new myapp.avro.MovieIncome.Builder();
    } else {
      return new myapp.avro.MovieIncome.Builder(other);
    }
  }

  /**
   * Creates a new MovieIncome RecordBuilder by copying an existing MovieIncome instance.
   * @param other The existing instance to copy.
   * @return A new MovieIncome RecordBuilder
   */
  public static myapp.avro.MovieIncome.Builder newBuilder(myapp.avro.MovieIncome other) {
    if (other == null) {
      return new myapp.avro.MovieIncome.Builder();
    } else {
      return new myapp.avro.MovieIncome.Builder(other);
    }
  }

  /**
   * RecordBuilder for MovieIncome instances.
   */
  @org.apache.avro.specific.AvroGenerated
  public static class Builder extends org.apache.avro.specific.SpecificRecordBuilderBase<MovieIncome>
    implements org.apache.avro.data.RecordBuilder<MovieIncome> {

    private long id;
    private java.lang.CharSequence title;
    private double income;
    private long modified;

    /** Creates a new Builder */
    private Builder() {
      super(SCHEMA$);
    }

    /**
     * Creates a Builder by copying an existing Builder.
     * @param other The existing Builder to copy.
     */
    private Builder(myapp.avro.MovieIncome.Builder other) {
      super(other);
      if (isValidValue(fields()[0], other.id)) {
        this.id = data().deepCopy(fields()[0].schema(), other.id);
        fieldSetFlags()[0] = other.fieldSetFlags()[0];
      }
      if (isValidValue(fields()[1], other.title)) {
        this.title = data().deepCopy(fields()[1].schema(), other.title);
        fieldSetFlags()[1] = other.fieldSetFlags()[1];
      }
      if (isValidValue(fields()[2], other.income)) {
        this.income = data().deepCopy(fields()[2].schema(), other.income);
        fieldSetFlags()[2] = other.fieldSetFlags()[2];
      }
      if (isValidValue(fields()[3], other.modified)) {
        this.modified = data().deepCopy(fields()[3].schema(), other.modified);
        fieldSetFlags()[3] = other.fieldSetFlags()[3];
      }
    }

    /**
     * Creates a Builder by copying an existing MovieIncome instance
     * @param other The existing instance to copy.
     */
    private Builder(myapp.avro.MovieIncome other) {
      super(SCHEMA$);
      if (isValidValue(fields()[0], other.id)) {
        this.id = data().deepCopy(fields()[0].schema(), other.id);
        fieldSetFlags()[0] = true;
      }
      if (isValidValue(fields()[1], other.title)) {
        this.title = data().deepCopy(fields()[1].schema(), other.title);
        fieldSetFlags()[1] = true;
      }
      if (isValidValue(fields()[2], other.income)) {
        this.income = data().deepCopy(fields()[2].schema(), other.income);
        fieldSetFlags()[2] = true;
      }
      if (isValidValue(fields()[3], other.modified)) {
        this.modified = data().deepCopy(fields()[3].schema(), other.modified);
        fieldSetFlags()[3] = true;
      }
    }

    /**
      * Gets the value of the 'id' field.
      * @return The value.
      */
    public long getId() {
      return id;
    }


    /**
      * Sets the value of the 'id' field.
      * @param value The value of 'id'.
      * @return This builder.
      */
    public myapp.avro.MovieIncome.Builder setId(long value) {
      validate(fields()[0], value);
      this.id = value;
      fieldSetFlags()[0] = true;
      return this;
    }

    /**
      * Checks whether the 'id' field has been set.
      * @return True if the 'id' field has been set, false otherwise.
      */
    public boolean hasId() {
      return fieldSetFlags()[0];
    }


    /**
      * Clears the value of the 'id' field.
      * @return This builder.
      */
    public myapp.avro.MovieIncome.Builder clearId() {
      fieldSetFlags()[0] = false;
      return this;
    }

    /**
      * Gets the value of the 'title' field.
      * @return The value.
      */
    public java.lang.CharSequence getTitle() {
      return title;
    }


    /**
      * Sets the value of the 'title' field.
      * @param value The value of 'title'.
      * @return This builder.
      */
    public myapp.avro.MovieIncome.Builder setTitle(java.lang.CharSequence value) {
      validate(fields()[1], value);
      this.title = value;
      fieldSetFlags()[1] = true;
      return this;
    }

    /**
      * Checks whether the 'title' field has been set.
      * @return True if the 'title' field has been set, false otherwise.
      */
    public boolean hasTitle() {
      return fieldSetFlags()[1];
    }


    /**
      * Clears the value of the 'title' field.
      * @return This builder.
      */
    public myapp.avro.MovieIncome.Builder clearTitle() {
      title = null;
      fieldSetFlags()[1] = false;
      return this;
    }

    /**
      * Gets the value of the 'income' field.
      * @return The value.
      */
    public double getIncome() {
      return income;
    }


    /**
      * Sets the value of the 'income' field.
      * @param value The value of 'income'.
      * @return This builder.
      */
    public myapp.avro.MovieIncome.Builder setIncome(double value) {
      validate(fields()[2], value);
      this.income = value;
      fieldSetFlags()[2] = true;
      return this;
    }

    /**
      * Checks whether the 'income' field has been set.
      * @return True if the 'income' field has been set, false otherwise.
      */
    public boolean hasIncome() {
      return fieldSetFlags()[2];
    }


    /**
      * Clears the value of the 'income' field.
      * @return This builder.
      */
    public myapp.avro.MovieIncome.Builder clearIncome() {
      fieldSetFlags()[2] = false;
      return this;
    }

    /**
      * Gets the value of the 'modified' field.
      * @return The value.
      */
    public long getModified() {
      return modified;
    }


    /**
      * Sets the value of the 'modified' field.
      * @param value The value of 'modified'.
      * @return This builder.
      */
    public myapp.avro.MovieIncome.Builder setModified(long value) {
      validate(fields()[3], value);
      this.modified = value;
      fieldSetFlags()[3] = true;
      return this;
    }

    /**
      * Checks whether the 'modified' field has been set.
      * @return True if the 'modified' field has been set, false otherwise.
      */
    public boolean hasModified() {
      return fieldSetFlags()[3];
    }


    /**
      * Clears the value of the 'modified' field.
      * @return This builder.
      */
    public myapp.avro.MovieIncome.Builder clearModified() {
      fieldSetFlags()[3] = false;
      return this;
    }

    @Override
    @SuppressWarnings("unchecked")
    public MovieIncome build() {
      try {
        MovieIncome record = new MovieIncome();
        record.id = fieldSetFlags()[0] ? this.id : (java.lang.Long) defaultValue(fields()[0]);
        record.title = fieldSetFlags()[1] ? this.title : (java.lang.CharSequence) defaultValue(fields()[1]);
        record.income = fieldSetFlags()[2] ? this.income : (java.lang.Double) defaultValue(fields()[2]);
        record.modified = fieldSetFlags()[3] ? this.modified : (java.lang.Long) defaultValue(fields()[3]);
        return record;
      } catch (org.apache.avro.AvroMissingFieldException e) {
        throw e;
      } catch (java.lang.Exception e) {
        throw new org.apache.avro.AvroRuntimeException(e);
      }
    }
  }

  @SuppressWarnings("unchecked")
  private static final org.apache.avro.io.DatumWriter<MovieIncome>
    WRITER$ = (org.apache.avro.io.DatumWriter<MovieIncome>)MODEL$.createDatumWriter(SCHEMA$);

  @Override public void writeExternal(java.io.ObjectOutput out)
    throws java.io.IOException {
    WRITER$.write(this, SpecificData.getEncoder(out));
  }

  @SuppressWarnings("unchecked")
  private static final org.apache.avro.io.DatumReader<MovieIncome>
    READER$ = (org.apache.avro.io.DatumReader<MovieIncome>)MODEL$.createDatumReader(SCHEMA$);

  @Override public void readExternal(java.io.ObjectInput in)
    throws java.io.IOException {
    READER$.read(this, SpecificData.getDecoder(in));
  }

  @Override protected boolean hasCustomCoders() { return true; }

  @Override public void customEncode(org.apache.avro.io.Encoder out)
    throws java.io.IOException
  {
    out.writeLong(this.id);

    out.writeString(this.title);

    out.writeDouble(this.income);

    out.writeLong(this.modified);

  }

  @Override public void customDecode(org.apache.avro.io.ResolvingDecoder in)
    throws java.io.IOException
  {
    org.apache.avro.Schema.Field[] fieldOrder = in.readFieldOrderIfDiff();
    if (fieldOrder == null) {
      this.id = in.readLong();

      this.title = in.readString(this.title instanceof Utf8 ? (Utf8)this.title : null);

      this.income = in.readDouble();

      this.modified = in.readLong();

    } else {
      for (int i = 0; i < 4; i++) {
        switch (fieldOrder[i].pos()) {
        case 0:
          this.id = in.readLong();
          break;

        case 1:
          this.title = in.readString(this.title instanceof Utf8 ? (Utf8)this.title : null);
          break;

        case 2:
          this.income = in.readDouble();
          break;

        case 3:
          this.modified = in.readLong();
          break;

        default:
          throw new java.io.IOException("Corrupt ResolvingDecoder.");
        }
      }
    }
  }
}










