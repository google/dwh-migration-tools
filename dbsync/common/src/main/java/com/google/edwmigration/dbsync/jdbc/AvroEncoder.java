package com.google.edwmigration.dbsync.jdbc;

import com.google.common.base.CharMatcher;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.sql.JDBCType;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Arrays;
import java.util.Map;
import java.util.TreeMap;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.SchemaBuilder.BaseTypeBuilder;
import org.apache.avro.SchemaBuilder.FieldAssembler;
import org.apache.avro.SchemaBuilder.FieldBuilder;
import org.apache.avro.SchemaBuilder.NullDefault;
import org.apache.avro.SchemaBuilder.UnionAccumulator;
import org.apache.avro.file.DataFileConstants;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.util.NonCopyingByteArrayOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AvroEncoder implements JdbcEncoder {

  private static final Logger LOG = LoggerFactory.getLogger(AvroEncoder.class);
  private static final boolean DEBUG = false;

  // Sync must be fixed, or we can't rsync.
  private static final byte[] SYNC = new byte[]{
      1, 2, 3, 4,
      42, 43, 44, 45,
      5, 6, 7, 8,
      49, 48, 47, 46
  };

  private static final CharMatcher AVRO_ILLEGAL_CHARS =
      CharMatcher.inRange('a', 'z')
          .or(CharMatcher.inRange('A', 'Z'))
          .or(CharMatcher.inRange('0', '9'))
          // .or(CharMatcher.anyOf("_"))  // Legal, but we want it to collapse.
          .negate()
          .precomputed();

  private static String legalize(String in) {
    return AVRO_ILLEGAL_CHARS.collapseFrom(in, '_');
  }

  private static interface Transfer {

    public abstract void define(
        FieldAssembler<Schema> builder,
        ResultSetMetaData md,
        int columnIndex) throws SQLException, IOException;

    public abstract void transfer(
        Encoder out,
        ResultSet rs,
        int columnIndex)
        throws SQLException, IOException;
  }

  private static abstract class AbstractTransfer implements Transfer {

    public FieldBuilder<Schema> defineBase(
        FieldAssembler<Schema> builder,
        ResultSetMetaData md,
        int columnIndex
    ) throws SQLException, IOException {
      String columnName = md.getColumnLabel(columnIndex + 1);
      int columnType = md.getColumnType(columnIndex + 1);
      JDBCType columnJdbcType = JDBCType.valueOf(columnType); // TODO: May throw.
      return builder.name(legalize(columnName))
          .prop("jdbcName", columnName)
          .prop("jdbcType", columnType)
          .prop("jdbcSymbolicType", columnJdbcType)
          .prop("jdbcClass", md.getColumnClassName(columnIndex + 1));
    }

    public BaseTypeBuilder<UnionAccumulator<NullDefault<Schema>>> defineNullable(
        FieldBuilder<Schema> builder,
        ResultSetMetaData md,
        int columnIndex
    ) throws SQLException, IOException {
      return builder.type().unionOf().nullBuilder().endNull().and();
    }

    @Override
    public String toString() {
      return getClass().getSimpleName();
    }
  }

  private static class LongTransfer extends AbstractTransfer {

    public static final LongTransfer INSTANCE = new LongTransfer();

    @Override
    public void define(FieldAssembler<Schema> builder, ResultSetMetaData md, int columnIndex)
        throws SQLException, IOException {
      FieldBuilder<Schema> base = defineBase(builder, md, columnIndex);
      base.type().longType().noDefault();
    }

    @Override
    public void transfer(Encoder out, ResultSet rs, int columnIndex)
        throws SQLException, IOException {
      long value = rs.getLong(columnIndex + 1);
      out.writeLong(value);
    }
  }

  private static class NullableLongTransfer extends AbstractTransfer {

    public static final NullableLongTransfer INSTANCE = new NullableLongTransfer();

    @Override
    public void define(FieldAssembler<Schema> builder, ResultSetMetaData md, int columnIndex)
        throws SQLException, IOException {
      FieldBuilder<Schema> base = defineBase(builder, md, columnIndex);
      BaseTypeBuilder<UnionAccumulator<NullDefault<Schema>>> union = defineNullable(base, md,
          columnIndex);
      union.longType().endUnion().nullDefault();
    }

    @Override
    public void transfer(Encoder out, ResultSet rs, int columnIndex)
        throws SQLException, IOException {
      long value = rs.getLong(columnIndex + 1);
      if (rs.wasNull()) {
        out.writeIndex(0);
        out.writeNull();
      } else {
        out.writeIndex(1);
        out.writeLong(value);
      }
    }
  }

  private static Transfer[] newTransfers(ResultSetMetaData md) throws SQLException {
    int columnCount = md.getColumnCount();
    Transfer[] transfers = new Transfer[columnCount];
    for (int columnIndex = 0; columnIndex < columnCount; columnIndex++) {
      int columnType = md.getColumnType(columnIndex + 1);
      boolean columnNullable = md.isNullable(columnIndex + 1) == ResultSetMetaData.columnNullable;
      switch (columnType) {
        case Types.TINYINT:
        case Types.SMALLINT:
        case Types.INTEGER:
        case Types.BIGINT:
          transfers[columnIndex] =
              columnNullable ? NullableLongTransfer.INSTANCE : LongTransfer.INSTANCE;
          break;
        default:
          throw new IllegalArgumentException(
              "Unsupported transfer type " + JDBCType.valueOf(columnType));
      }
    }
    return transfers;
  }

  private static class BinaryBuffer extends NonCopyingByteArrayOutputStream {

    public BinaryBuffer() {
      super((int) (DataFileConstants.DEFAULT_SYNC_INTERVAL * 1.25));
    }

    public byte[] getData() {
      return buf;
    }
  }

  private void open(BinaryEncoder fileEncoder, Schema schema) throws IOException {
    Map<String, byte[]> meta = new TreeMap<>();
    if (DEBUG) {
      LOG.debug("Schema is {}", schema);
    }
    meta.put(DataFileConstants.SCHEMA, schema.toString().getBytes(StandardCharsets.UTF_8));

    fileEncoder.writeFixed(DataFileConstants.MAGIC); // write magic

    // Write the metadata
    fileEncoder.writeMapStart();
    fileEncoder.setItemCount(meta.size());
    for (Map.Entry<String, byte[]> entry : meta.entrySet()) {
      fileEncoder.startItem();
      fileEncoder.writeString(entry.getKey());
      fileEncoder.writeBytes(entry.getValue());
    }
    fileEncoder.writeMapEnd();

    // Write sync
    fileEncoder.writeFixed(SYNC);
  }

  private void flush(BinaryEncoder fileEncoder, BinaryBuffer blockBuffer, int blockRecordCount)
      throws IOException {
    fileEncoder.writeLong(blockRecordCount);
    fileEncoder.writeLong(blockBuffer.size());
    fileEncoder.writeFixed(blockBuffer.getData(), 0, blockBuffer.size());
    fileEncoder.writeFixed(SYNC);
    blockBuffer.reset();
  }

  @Override
  public void encodeTo(OutputStream out, ResultSet rs) throws IOException, SQLException {
    ResultSetMetaData md = rs.getMetaData();
    Transfer[] transfers = newTransfers(md);
    if (DEBUG) {
      LOG.debug("Transfers are " + Arrays.toString(transfers));
    }

    SchemaBuilder.FieldAssembler<Schema> builder = SchemaBuilder.record("MyRecordName")
        .namespace("MyNamespaceName")
        .fields();
    for (int i = 0; i < transfers.length; i++) {
      transfers[i].define(builder, md, i);
    }
    Schema schema = builder.endRecord();

    BinaryEncoder fileEncoder = EncoderFactory.get().directBinaryEncoder(out, null);
    open(fileEncoder, schema);

    BinaryBuffer blockBuffer = new BinaryBuffer();
    BinaryEncoder blockEncoder = EncoderFactory.get().directBinaryEncoder(blockBuffer, null);
    int blockRecordCount = 0;

    // DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema);
    // try (DataFileWriter<GenericRecord> fileWriter = new DataFileWriter<>(datumWriter)) {
    // fileWriter.create(schema, out, SYNC);
    while (rs.next()) {
      for (int i = 0; i < transfers.length; i++) {
        transfers[i].transfer(blockEncoder, rs, i);
      }
      blockRecordCount++;

      if (blockBuffer.size() > DataFileConstants.DEFAULT_SYNC_INTERVAL) {
        flush(fileEncoder, blockBuffer, blockRecordCount);
        blockRecordCount = 0;
      }
    }

    if (blockRecordCount > 0) {
      flush(fileEncoder, blockBuffer, blockRecordCount);
      blockRecordCount = 0;
    }
    // }
    //
    // fileEncoder.flush();
  }
}
