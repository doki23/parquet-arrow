package io.github.doki23.parquet.arrow;

import org.apache.arrow.vector.FieldVector;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.LocalInputFile;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class ArrowParquetReaderTest {
    @Test
    public void testRead() throws IOException {
        Path filePath = new File(Objects.requireNonNull(
                getClass().getResource("/parquet-testing/data/alltypes_tiny_pages_plain.parquet")).getPath()).toPath();
        InputFile inputFile = new LocalInputFile(filePath);
        List<ColumnarBatch> columnarBatches = new ArrayList<>();
        try (ArrowParquetReader reader = new ArrowParquetReader(inputFile, 5)) {
            int count = 0;
            for (ColumnarBatch columnarBatch : reader) {
                count += columnarBatch.numRows();
                System.out.println();
                for (int i = 0; i < columnarBatch.numCols(); i++) {
                    FieldVector colVector = columnarBatch.column(i).getArrowVector();
                    System.out.println(colVector.getField().getName() + ":" + colVector);
                }
                columnarBatches.add(columnarBatch);
            }
            Assertions.assertEquals(7300, count);
        }
    }
}
