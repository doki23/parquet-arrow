package io.github.doki23.parquet.arrow;

import io.github.doki23.parquet.util.ValidatingIntConvertor;
import org.apache.arrow.vector.IntVector;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.ColumnReader;
import org.apache.parquet.column.impl.ColumnReaderImpl;
import org.apache.parquet.column.page.PageReader;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.io.LocalInputFile;
import org.apache.parquet.schema.MessageType;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Path;

import static io.github.doki23.parquet.arrow.VectorizedArrowReader.DEFAULT_BATCH_SIZE;


public class VectorizedArrowReaderTest {
  @Test
  public void readPlainIntVectorTest() throws IOException {
    Path filePath = new File(
        getClass().getResource("/parquet-testing/data/alltypes_tiny_pages_plain.parquet").getPath()).toPath();
    ParquetFileReader reader;
    try {
      reader = ParquetFileReader.open(new LocalInputFile(filePath));
    } catch (FileNotFoundException e) {
      throw new FileNotFoundException(e.getMessage() + ", please execute git submodule update --init");
    }
    try {
        MessageType schema = reader.getFileMetaData().getSchema();
        ColumnDescriptor col = schema.getColumnDescription(new String[] {"id"});
      VectorizedArrowReader arrowReader = new VectorizedArrowReader(col, ArrowAllocation.rootAllocator(), true);
      arrowReader.setBatchSize(DEFAULT_BATCH_SIZE);
      ColumnChunkMetaData columnChunkMetaData = reader.getRowGroups()
          .get(0)
          .getColumns()
          .get(schema.getFieldIndex("id"));
      arrowReader.setRowGroupInfo(reader.readRowGroup(0), columnChunkMetaData);
      try {
        PageReader pageReader = reader.readRowGroup(0).getPageReader(col);
        int valueCount = (int) pageReader.getTotalValueCount();
        int batchSize = DEFAULT_BATCH_SIZE;
        VectorHolder vectorHolder = null;
        for (int remaining = valueCount; remaining > 0; remaining -= batchSize) {
          vectorHolder = arrowReader.read(vectorHolder, Math.min(batchSize, remaining));
          IntVector vector = (IntVector) vectorHolder.vector();

          ValidatingIntConvertor convertor = new ValidatingIntConvertor(vector);
          ColumnReader columnReader = new ColumnReaderImpl(col, pageReader, convertor, null);

          convertor.reset();
          for (int i = 0; i < vector.getValueCount(); i++) {
            columnReader.writeCurrentValueToConverter();
            columnReader.consume();
          }
        }
      } finally {
        arrowReader.close();
      }
    } finally {
      reader.close();
    }
  }
}
