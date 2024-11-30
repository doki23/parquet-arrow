# parquet-arrow
A Parquet reader/writer implementation based on Apache Arrow. It is inspired by the implementation of Apache Iceberg's parquet reader. Currently, only the reader for primitive type definition levels is implemented.

This repo is still under development.

# Usage Example
```java
public void testRead() throws IOException {
    Path filePath = new File(
            Objects.requireNonNull(getClass().getResource("/parquet-testing/data/alltypes_tiny_pages_plain.parquet"))
                    .getPath()).toPath();
    InputFile inputFile = new LocalInputFile(filePath);
    try (ArrowParquetReader reader = new ArrowParquetReader(inputFile, 100)) {
        int count = 0;
        for (ColumnarBatch columnarBatch : reader) {
            count += columnarBatch.numRows();
            System.out.println();
            for (int i = 0; i < columnarBatch.numCols(); i++) {
                FieldVector colVector = columnarBatch.column(i).getArrowVector();
                System.out.println(colVector.getField().getName() + ":" + colVector);
            }
        }
        Assertions.assertEquals(7300, count);
    }
}
```