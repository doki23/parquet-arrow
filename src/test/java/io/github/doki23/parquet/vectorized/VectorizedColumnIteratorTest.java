/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package io.github.doki23.parquet.vectorized;

import io.github.doki23.parquet.arrow.ArrowAllocation;
import io.github.doki23.parquet.util.ValidatingIntConvertor;
import org.apache.arrow.vector.IntVector;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.ColumnReader;
import org.apache.parquet.column.impl.ColumnReaderImpl;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.column.page.PageReader;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.io.LocalInputFile;
import org.apache.parquet.schema.MessageType;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Path;

public class VectorizedColumnIteratorTest {
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
      PageReadStore pageReadStore1 = reader.readRowGroup(0);
      PageReader pageReader1 = pageReadStore1.getPageReader(col);
      int valueCount = (int) pageReader1.getTotalValueCount();
      PageReadStore pageReadStore2 = reader.readRowGroup(0);
      PageReader pageReader2 = pageReadStore2.getPageReader(col);

      int batchSize = 1024;
      VectorizedColumnIterator vectorizedColumnIterator = new VectorizedColumnIterator(col, null, true);
      vectorizedColumnIterator.setRowGroupInfo(pageReader1, false);
      vectorizedColumnIterator.setBatchSize(batchSize);

      try (IntVector idVec = new IntVector("id", ArrowAllocation.rootAllocator())) {
        ValidatingIntConvertor convertor = new ValidatingIntConvertor(idVec);
        ColumnReader columnReader = new ColumnReaderImpl(col, pageReader2, convertor, null);

        idVec.allocateNew();
        int remaining = valueCount;
        while (vectorizedColumnIterator.hasNext()) {
          vectorizedColumnIterator.integerBatchReader().nextBatch(idVec, 4, new NullabilityHolder(valueCount));
          remaining -= idVec.getValueCount();
          convertor.reset();
          for (int i = 0; i < Math.min(remaining, idVec.getValueCount()); i++) {
            columnReader.writeCurrentValueToConverter();
            columnReader.consume();
          }
        }
      }
    } finally {
      reader.close();
    }
  }


}
