package io.github.doki23.parquet.arrow;

import io.github.doki23.parquet.vectorized.VectorizedReader;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.parquet.Preconditions;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.io.InputFile;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

public class ArrowParquetReader implements Iterable<ColumnarBatch>, Iterator<ColumnarBatch>, Closeable {
    private final InputFile parquetFile;
    private final int batchSize;
    private final BufferAllocator bufferAllocator;

    private ParquetFileReader parquetFileReader;
    private List<BlockMetaData> rowGroups;
    private ArrowBatchReader arrowBatchReader;
    private long currentRowGroupRowsRemaining;
    private int nextRowGroupIndex;
    private ColumnarBatch reuse;

    public ArrowParquetReader(InputFile parquetFile, int batchSize) {
        this(parquetFile, batchSize, null);
    }

    public ArrowParquetReader(InputFile parquetFile, int batchSize, BufferAllocator bufferAllocator) {
        Preconditions.checkArgument(batchSize > 0, "batchSize must be greater than 0");
        this.parquetFile = Objects.requireNonNull(parquetFile, "parquetFile cannot be null");
        this.batchSize = batchSize;
        this.bufferAllocator = bufferAllocator != null ? bufferAllocator : ArrowAllocation.rootAllocator();
    }

    private void readRowGroups() throws IOException {
        if (parquetFileReader == null) {
            parquetFileReader = ParquetFileReader.open(parquetFile);
            rowGroups = parquetFileReader.getRowGroups();
            nextRowGroupIndex = 0;
            currentRowGroupRowsRemaining = 0;
        }
    }

    private void readNextRowGroup() throws IOException {
        readRowGroups();
        if (currentRowGroupRowsRemaining == 0 && nextRowGroupIndex < rowGroups.size()) {
            List<ColumnChunkMetaData> columnChunks = rowGroups.get(nextRowGroupIndex).getColumns();
            PageReadStore rowGroupReader = parquetFileReader.readRowGroup(nextRowGroupIndex);
            List<ColumnDescriptor> columns = parquetFileReader.getFileMetaData().getSchema().getColumns();
            if (arrowBatchReader != null) {
                arrowBatchReader.close();
            }
            List<VectorizedReader<?>> columnReaders = new ArrayList<>(columns.size());
            for (int i = 0; i < columnChunks.size(); i++) {
                ColumnDescriptor column = columns.get(i);
                VectorizedArrowReader columnReader = new VectorizedArrowReader(column, bufferAllocator, true);
                columnReaders.add(columnReader);
            }
            arrowBatchReader = new ArrowBatchReader(columnReaders);
            arrowBatchReader.setBatchSize(batchSize);
            arrowBatchReader.setRowGroupInfo(rowGroupReader, columnChunks.toArray(ColumnChunkMetaData[]::new));
            currentRowGroupRowsRemaining = rowGroupReader.getRowCount();
            nextRowGroupIndex++;
        }
    }

    @Override
    public Iterator<ColumnarBatch> iterator() {
        return new ArrowParquetReader(parquetFile, batchSize, bufferAllocator);
    }

    @Override
    public boolean hasNext() {
        if (currentRowGroupRowsRemaining > 0) {
            return true;
        }
        try {
            readNextRowGroup();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        boolean hasNext = currentRowGroupRowsRemaining > 0;
        if (!hasNext) {
            try {
                close();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
        return hasNext;
    }

    @Override
    public ColumnarBatch next() {
        Preconditions.checkArgument(currentRowGroupRowsRemaining > 0, "No rows available!");
        int numsToRead = Math.min((int) currentRowGroupRowsRemaining, batchSize);
        reuse = arrowBatchReader.read(reuse, numsToRead);
        currentRowGroupRowsRemaining -= numsToRead;
        return reuse;
    }

    @Override
    public void close() throws IOException {
        if (parquetFileReader != null) {
            parquetFileReader.close();
        }
        if (arrowBatchReader != null) {
            arrowBatchReader.close();
        }
    }
}
