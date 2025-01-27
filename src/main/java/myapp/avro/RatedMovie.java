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
public class RatedMovie extends org.apache.avro.specific.SpecificRecordBase implements org.apache.avro.specific.SpecificRecord {
  private static final Long serialVersionUID = 5782858015694322892L;
  public static final org.apache.avro.Schema SCHEMA$ = new org.apache.avro.Schema.Parser().parse("{\"type\":\"record\",\"name\":\"RatedMovie\",\"namespace\":\"myapp.avro\",\"fields\":[{\"name\":\"id\",\"type\":\"long\"},{\"name\":\"title\",\"type\":\"string\"},{\"name\":\"release_year\",\"type\":\"int\"},{\"name\":\"rating\",\"type\":\"double\"}]}");
  public static org.apache.avro.Schema getClassSchema() { return SCHEMA$; }

  private static SpecificData MODEL$ = new SpecificData();

  private static final BinaryMessageEncoder<RatedMovie> ENCODER =
      new BinaryMessageEncoder<RatedMovie>(MODEL$, SCHEMA$);

  private static final BinaryMessageDecoder<RatedMovie> DECODER =
      new BinaryMessageDecoder<RatedMovie>(MODEL$, SCHEMA$);

  /**
   * Return the BinaryMessageEncoder instance used by this class.
   * @return the message encoder used by this class
   */
  public static BinaryMessageEncoder<RatedMovie> getEncoder() {
    return ENCODER;
  }

  /**
   * Return the BinaryMessageDecoder instance used by this class.
   * @return the message decoder used by this class
   */
  public static BinaryMessageDecoder<RatedMovie> getDecoder() {
    return DECODER;
  }

  /**
   * Create a new BinaryMessageDecoder instance for this class that uses the specified {@link SchemaStore}.
   * @param resolver a {@link SchemaStore} used to find schemas by fingerprint
   * @return a BinaryMessageDecoder instance for this class backed by the given SchemaStore
   */
  public static BinaryMessageDecoder<RatedMovie> createDecoder(SchemaStore resolver) {
    return new BinaryMessageDecoder<RatedMovie>(MODEL$, SCHEMA$, resolver);
  }

  /**
   * Serializes this RatedMovie to a ByteBuffer.
   * @return a buffer holding the serialized data for this instance
   * @throws java.io.IOException if this instance could not be serialized
   */
  public java.nio.ByteBuffer toByteBuffer() throws java.io.IOException {
    return ENCODER.encode(this);
  }

  /**
   * Deserializes a RatedMovie from a ByteBuffer.
   * @param b a byte buffer holding serialized data for an instance of this class
   * @return a RatedMovie instance decoded from the given buffer
   * @throws java.io.IOException if the given bytes could not be deserialized into an instance of this class
   */
  public static RatedMovie fromByteBuffer(
      java.nio.ByteBuffer b) throws java.io.IOException {
    return DECODER.decode(b);
  }

  @Deprecated public Long id;
  @Deprecated public java.lang.CharSequence title;
  @Deprecated public int release_year;
  @Deprecated public double rating;

  /**
   * Default constructor.  Note that this does not initialize fields
   * to their default values from the schema.  If that is desired then
   * one should use <code>newBuilder()</code>.
   */
  public RatedMovie() {}

  /**
   * All-args constructor.
   * @param id The new value for id
   * @param title The new value for title
   * @param release_year The new value for release_year
   * @param rating The new value for rating
   */
  public RatedMovie(java.lang.Long id, java.lang.CharSequence title, java.lang.Integer release_year, java.lang.Double rating) {
    this.id = id;
    this.title = title;
    this.release_year = release_year;
    this.rating = rating;
  }

  public org.apache.avro.specific.SpecificData getSpecificData() { return MODEL$; }
  public org.apache.avro.Schema getSchema() { return SCHEMA$; }
  // Used by DatumWriter.  Applications should not call.
  public java.lang.Object get(int field$) {
    switch (field$) {
    case 0: return id;
    case 1: return title;
    case 2: return release_year;
    case 3: return rating;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }

  // Used by DatumReader.  Applications should not call.
  @SuppressWarnings(value="unchecked")
  public void put(int field$, java.lang.Object value$) {
    switch (field$) {
    case 0: id = (java.lang.Long)value$; break;
    case 1: title = (java.lang.CharSequence)value$; break;
    case 2: release_year = (java.lang.Integer)value$; break;
    case 3: rating = (java.lang.Double)value$; break;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }

  /**
   * Gets the value of the 'id' field.
   * @return The value of the 'id' field.
   */
  public Long getId() {
    return id;
  }


  /**
   * Sets the value of the 'id' field.
   * @param value the value to set.
   */
  public void setId(Long value) {
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
   * Gets the value of the 'release_year' field.
   * @return The value of the 'release_year' field.
   */
  public int getReleaseYear() {
    return release_year;
  }


  /**
   * Sets the value of the 'release_year' field.
   * @param value the value to set.
   */
  public void setReleaseYear(int value) {
    this.release_year = value;
  }

  /**
   * Gets the value of the 'rating' field.
   * @return The value of the 'rating' field.
   */
  public double getRating() {
    return rating;
  }


  /**
   * Sets the value of the 'rating' field.
   * @param value the value to set.
   */
  public void setRating(double value) {
    this.rating = value;
  }

  /**
   * Creates a new RatedMovie RecordBuilder.
   * @return A new RatedMovie RecordBuilder
   */
  public static myapp.avro.RatedMovie.Builder newBuilder() {
    return new myapp.avro.RatedMovie.Builder();
  }

  /**
   * Creates a new RatedMovie RecordBuilder by copying an existing Builder.
   * @param other The existing builder to copy.
   * @return A new RatedMovie RecordBuilder
   */
  public static myapp.avro.RatedMovie.Builder newBuilder(myapp.avro.RatedMovie.Builder other) {
    if (other == null) {
      return new myapp.avro.RatedMovie.Builder();
    } else {
      return new myapp.avro.RatedMovie.Builder(other);
    }
  }

  /**
   * Creates a new RatedMovie RecordBuilder by copying an existing RatedMovie instance.
   * @param other The existing instance to copy.
   * @return A new RatedMovie RecordBuilder
   */
  public static myapp.avro.RatedMovie.Builder newBuilder(myapp.avro.RatedMovie other) {
    if (other == null) {
      return new myapp.avro.RatedMovie.Builder();
    } else {
      return new myapp.avro.RatedMovie.Builder(other);
    }
  }

  /**
   * RecordBuilder for RatedMovie instances.
   */
  @org.apache.avro.specific.AvroGenerated
  public static class Builder extends org.apache.avro.specific.SpecificRecordBuilderBase<RatedMovie>
    implements org.apache.avro.data.RecordBuilder<RatedMovie> {

    private Long id;
    private java.lang.CharSequence title;
    private int release_year;
    private double rating;

    /** Creates a new Builder */
    private Builder() {
      super(SCHEMA$);
    }

    /**
     * Creates a Builder by copying an existing Builder.
     * @param other The existing Builder to copy.
     */
    private Builder(myapp.avro.RatedMovie.Builder other) {
      super(other);
      if (isValidValue(fields()[0], other.id)) {
        this.id = data().deepCopy(fields()[0].schema(), other.id);
        fieldSetFlags()[0] = other.fieldSetFlags()[0];
      }
      if (isValidValue(fields()[1], other.title)) {
        this.title = data().deepCopy(fields()[1].schema(), other.title);
        fieldSetFlags()[1] = other.fieldSetFlags()[1];
      }
      if (isValidValue(fields()[2], other.release_year)) {
        this.release_year = data().deepCopy(fields()[2].schema(), other.release_year);
        fieldSetFlags()[2] = other.fieldSetFlags()[2];
      }
      if (isValidValue(fields()[3], other.rating)) {
        this.rating = data().deepCopy(fields()[3].schema(), other.rating);
        fieldSetFlags()[3] = other.fieldSetFlags()[3];
      }
    }

    /**
     * Creates a Builder by copying an existing RatedMovie instance
     * @param other The existing instance to copy.
     */
    private Builder(myapp.avro.RatedMovie other) {
      super(SCHEMA$);
      if (isValidValue(fields()[0], other.id)) {
        this.id = data().deepCopy(fields()[0].schema(), other.id);
        fieldSetFlags()[0] = true;
      }
      if (isValidValue(fields()[1], other.title)) {
        this.title = data().deepCopy(fields()[1].schema(), other.title);
        fieldSetFlags()[1] = true;
      }
      if (isValidValue(fields()[2], other.release_year)) {
        this.release_year = data().deepCopy(fields()[2].schema(), other.release_year);
        fieldSetFlags()[2] = true;
      }
      if (isValidValue(fields()[3], other.rating)) {
        this.rating = data().deepCopy(fields()[3].schema(), other.rating);
        fieldSetFlags()[3] = true;
      }
    }

    /**
      * Gets the value of the 'id' field.
      * @return The value.
      */
    public Long getId() {
      return id;
    }


    /**
      * Sets the value of the 'id' field.
      * @param value The value of 'id'.
      * @return This builder.
      */
    public myapp.avro.RatedMovie.Builder setId(Long value) {
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
    public myapp.avro.RatedMovie.Builder clearId() {
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
    public myapp.avro.RatedMovie.Builder setTitle(java.lang.CharSequence value) {
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
    public myapp.avro.RatedMovie.Builder clearTitle() {
      title = null;
      fieldSetFlags()[1] = false;
      return this;
    }

    /**
      * Gets the value of the 'release_year' field.
      * @return The value.
      */
    public int getReleaseYear() {
      return release_year;
    }


    /**
      * Sets the value of the 'release_year' field.
      * @param value The value of 'release_year'.
      * @return This builder.
      */
    public myapp.avro.RatedMovie.Builder setReleaseYear(int value) {
      validate(fields()[2], value);
      this.release_year = value;
      fieldSetFlags()[2] = true;
      return this;
    }

    /**
      * Checks whether the 'release_year' field has been set.
      * @return True if the 'release_year' field has been set, false otherwise.
      */
    public boolean hasReleaseYear() {
      return fieldSetFlags()[2];
    }


    /**
      * Clears the value of the 'release_year' field.
      * @return This builder.
      */
    public myapp.avro.RatedMovie.Builder clearReleaseYear() {
      fieldSetFlags()[2] = false;
      return this;
    }

    /**
      * Gets the value of the 'rating' field.
      * @return The value.
      */
    public double getRating() {
      return rating;
    }


    /**
      * Sets the value of the 'rating' field.
      * @param value The value of 'rating'.
      * @return This builder.
      */
    public myapp.avro.RatedMovie.Builder setRating(double value) {
      validate(fields()[3], value);
      this.rating = value;
      fieldSetFlags()[3] = true;
      return this;
    }

    /**
      * Checks whether the 'rating' field has been set.
      * @return True if the 'rating' field has been set, false otherwise.
      */
    public boolean hasRating() {
      return fieldSetFlags()[3];
    }


    /**
      * Clears the value of the 'rating' field.
      * @return This builder.
      */
    public myapp.avro.RatedMovie.Builder clearRating() {
      fieldSetFlags()[3] = false;
      return this;
    }

    @Override
    @SuppressWarnings("unchecked")
    public RatedMovie build() {
      try {
        RatedMovie record = new RatedMovie();
        record.id = fieldSetFlags()[0] ? this.id : (java.lang.Long) defaultValue(fields()[0]);
        record.title = fieldSetFlags()[1] ? this.title : (java.lang.CharSequence) defaultValue(fields()[1]);
        record.release_year = fieldSetFlags()[2] ? this.release_year : (java.lang.Integer) defaultValue(fields()[2]);
        record.rating = fieldSetFlags()[3] ? this.rating : (java.lang.Double) defaultValue(fields()[3]);
        return record;
      } catch (org.apache.avro.AvroMissingFieldException e) {
        throw e;
      } catch (java.lang.Exception e) {
        throw new org.apache.avro.AvroRuntimeException(e);
      }
    }
  }

  @SuppressWarnings("unchecked")
  private static final org.apache.avro.io.DatumWriter<RatedMovie>
    WRITER$ = (org.apache.avro.io.DatumWriter<RatedMovie>)MODEL$.createDatumWriter(SCHEMA$);

  @Override public void writeExternal(java.io.ObjectOutput out)
    throws java.io.IOException {
    WRITER$.write(this, SpecificData.getEncoder(out));
  }

  @SuppressWarnings("unchecked")
  private static final org.apache.avro.io.DatumReader<RatedMovie>
    READER$ = (org.apache.avro.io.DatumReader<RatedMovie>)MODEL$.createDatumReader(SCHEMA$);

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

    out.writeInt(this.release_year);

    out.writeDouble(this.rating);

  }

  @Override public void customDecode(org.apache.avro.io.ResolvingDecoder in)
    throws java.io.IOException
  {
    org.apache.avro.Schema.Field[] fieldOrder = in.readFieldOrderIfDiff();
    if (fieldOrder == null) {
      this.id = in.readLong();

      this.title = in.readString(this.title instanceof Utf8 ? (Utf8)this.title : null);

      this.release_year = in.readInt();

      this.rating = in.readDouble();

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
          this.release_year = in.readInt();
          break;

        case 3:
          this.rating = in.readDouble();
          break;

        default:
          throw new java.io.IOException("Corrupt ResolvingDecoder.");
        }
      }
    }
  }
}










