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
package io.github.doki23.parquet.arrow;

import com.google.common.base.Preconditions;
import io.github.doki23.parquet.ParquetUtil;
import io.github.doki23.parquet.vectorized.NullabilityHolder;
import io.github.doki23.parquet.vectorized.VectorizedColumnIterator;
import io.github.doki23.parquet.vectorized.VectorizedReader;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.FixedSizeBinaryVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.TimeMicroVector;
import org.apache.arrow.vector.TimeStampMicroVector;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.parquet.arrow.schema.SchemaConverter;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.Dictionary;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;

/**
 * {@link VectorizedReader VectorReader(s)} that read in a batch of values into Arrow vectors. It
 * also takes care of allocating the right kind of Arrow vectors depending on the corresponding
 * Parquet data types.
 */
public class VectorizedArrowReader implements VectorizedReader<VectorHolder> {
    public static final int DEFAULT_BATCH_SIZE = 5000;
    private static final Integer UNKNOWN_WIDTH = null;
    private static final int AVERAGE_VARIABLE_WIDTH_RECORD_SIZE = 10;

    private final ColumnDescriptor columnDescriptor;
    private final VectorizedColumnIterator vectorizedColumnIterator;
    private final BufferAllocator rootAlloc;
    private final Field arrowField;

    private int batchSize;
    private FieldVector vec;
    private Integer typeWidth;
    private ReadType readType;
    private NullabilityHolder nullabilityHolder;

    // In cases when Parquet employs fall back to plain encoding, we eagerly decode the dictionary
    // encoded pages
    // before storing the values in the Arrow vector. This means even if the dictionary is present,
    // data
    // present in the vector may not necessarily be dictionary encoded.
    private Dictionary dictionary;

    public VectorizedArrowReader(ColumnDescriptor desc, BufferAllocator ra, boolean setArrowValidityVector) {
        this.columnDescriptor = desc;
        this.rootAlloc = ra;
        this.vectorizedColumnIterator = new VectorizedColumnIterator(desc, "", setArrowValidityVector);
        String fieldName = getColumnName();
        Type fieldParquetType = getParquetType();
        this.arrowField = new SchemaConverter().fromParquet(new MessageType(fieldName, fieldParquetType))
                .getArrowSchema()
                .findField(fieldName);
    }

    private VectorizedArrowReader() {
        this.batchSize = DEFAULT_BATCH_SIZE;
        this.columnDescriptor = null;
        this.rootAlloc = null;
        this.vectorizedColumnIterator = null;
        this.arrowField = null;
    }

    public static VectorizedArrowReader nulls() {
        return NullVectorReader.INSTANCE;
    }

    private String getColumnName() {
        String[] path = columnDescriptor.getPath();
        return path[path.length - 1];
    }

    private Type getParquetType() {
        return columnDescriptor.getPrimitiveType();
    }

    private boolean isColumnOptional() {
        PrimitiveType type = columnDescriptor.getPrimitiveType();
        return type.isRepetition(Type.Repetition.OPTIONAL);
    }

    @Override
    public void setBatchSize(int batchSize) {
        this.batchSize = (batchSize == 0) ? DEFAULT_BATCH_SIZE : batchSize;
        this.vectorizedColumnIterator.setBatchSize(batchSize);
    }

    @Override
    public VectorHolder read(VectorHolder reuse, int numValsToRead) {
        boolean dictEncoded = vectorizedColumnIterator.producesDictionaryEncodedVector();
        if (reuse == null || (!dictEncoded && readType == ReadType.DICTIONARY) || (dictEncoded && readType != ReadType.DICTIONARY)) {
            allocateFieldVector(dictEncoded);
            nullabilityHolder = new NullabilityHolder(batchSize);
        } else {
            vec.setValueCount(0);
            nullabilityHolder.reset();
        }
        if (vectorizedColumnIterator.hasNext()) {
            if (dictEncoded) {
                vectorizedColumnIterator.dictionaryBatchReader().nextBatch(vec, -1, nullabilityHolder);
            } else {
                switch (readType) {
                    case VARBINARY:
                    case VARCHAR:
                        vectorizedColumnIterator.varWidthTypeBatchReader().nextBatch(vec, -1, nullabilityHolder);
                        break;
                    case BOOLEAN:
                        vectorizedColumnIterator.booleanBatchReader().nextBatch(vec, -1, nullabilityHolder);
                        break;
                    case INT:
                    case INT_BACKED_DECIMAL:
                        vectorizedColumnIterator.integerBatchReader().nextBatch(vec, typeWidth, nullabilityHolder);
                        break;
                    case LONG:
                    case LONG_BACKED_DECIMAL:
                        vectorizedColumnIterator.longBatchReader().nextBatch(vec, typeWidth, nullabilityHolder);
                        break;
                    case FLOAT:
                        vectorizedColumnIterator.floatBatchReader().nextBatch(vec, typeWidth, nullabilityHolder);
                        break;
                    case DOUBLE:
                        vectorizedColumnIterator.doubleBatchReader().nextBatch(vec, typeWidth, nullabilityHolder);
                        break;
                    case TIMESTAMP_MILLIS:
                        vectorizedColumnIterator.timestampMillisBatchReader()
                                .nextBatch(vec, typeWidth, nullabilityHolder);
                        break;
                    case TIMESTAMP_INT96:
                        vectorizedColumnIterator.timestampInt96BatchReader()
                                .nextBatch(vec, typeWidth, nullabilityHolder);
                        break;
                    case FIXED_WIDTH_BINARY:
                    case FIXED_LENGTH_DECIMAL:
                        vectorizedColumnIterator.fixedSizeBinaryBatchReader()
                                .nextBatch(vec, typeWidth, nullabilityHolder);
                        break;
                }
            }
        }
        Preconditions.checkState(vec.getValueCount() == numValsToRead,
                "Number of values read, %s, does not equal expected, %s", vec.getValueCount(), numValsToRead);
        return new VectorHolder(columnDescriptor, vec, dictEncoded, dictionary, nullabilityHolder);
    }

    private void allocateFieldVector(boolean dictionaryEncodedVector) {
        if (dictionaryEncodedVector) {
            allocateDictEncodedVector();
        } else {
            if (columnDescriptor.getPrimitiveType().getOriginalType() != null) {
                allocateVectorBasedOnOriginalType(columnDescriptor.getPrimitiveType());
            } else {
                allocateVectorBasedOnTypeName(columnDescriptor.getPrimitiveType());
            }
        }
    }

    private void allocateDictEncodedVector() {
        Field field = new Field(getColumnName(),
                new FieldType(isColumnOptional(), new ArrowType.Int(Integer.SIZE, true), null, null), null);
        this.vec = field.createVector(rootAlloc);
        ((IntVector) vec).allocateNew(batchSize);
        this.typeWidth = (int) IntVector.TYPE_WIDTH;
        this.readType = ReadType.DICTIONARY;
    }

    private void allocateVectorBasedOnOriginalType(PrimitiveType primitive) {
        switch (primitive.getOriginalType()) {
            case ENUM:
            case JSON:
            case UTF8:
            case BSON:
                this.vec = arrowField.createVector(rootAlloc);
                // TODO: Possibly use the uncompressed page size info to set the initial capacity
                vec.setInitialCapacity(batchSize * AVERAGE_VARIABLE_WIDTH_RECORD_SIZE);
                vec.allocateNewSafe();
                this.readType = ReadType.VARCHAR;
                this.typeWidth = UNKNOWN_WIDTH;
                break;
            case INT_8:
            case INT_16:
            case INT_32:
                this.vec = arrowField.createVector(rootAlloc);
                ((IntVector) vec).allocateNew(batchSize);
                this.readType = ReadType.INT;
                this.typeWidth = (int) IntVector.TYPE_WIDTH;
                break;
            case DATE:
                this.vec = arrowField.createVector(rootAlloc);
                ((DateDayVector) vec).allocateNew(batchSize);
                this.readType = ReadType.INT;
                this.typeWidth = (int) IntVector.TYPE_WIDTH;
                break;
            case INT_64:
                this.vec = arrowField.createVector(rootAlloc);
                ((BigIntVector) vec).allocateNew(batchSize);
                this.readType = ReadType.LONG;
                this.typeWidth = (int) BigIntVector.TYPE_WIDTH;
                break;
            case TIMESTAMP_MILLIS:
                this.vec = arrowField.createVector(rootAlloc);
                ((BigIntVector) vec).allocateNew(batchSize);
                this.readType = ReadType.TIMESTAMP_MILLIS;
                this.typeWidth = (int) BigIntVector.TYPE_WIDTH;
                break;
            case TIMESTAMP_MICROS:
                this.vec = arrowField.createVector(rootAlloc);
                ((TimeStampMicroVector) vec).allocateNew(batchSize);
                this.readType = ReadType.LONG;
                this.typeWidth = (int) BigIntVector.TYPE_WIDTH;
                break;
            case TIME_MICROS:
                this.vec = arrowField.createVector(rootAlloc);
                ((TimeMicroVector) vec).allocateNew(batchSize);
                this.readType = ReadType.LONG;
                this.typeWidth = (int) TimeMicroVector.TYPE_WIDTH;
                break;
            case DECIMAL:
                this.vec = arrowField.createVector(rootAlloc);
                switch (primitive.getPrimitiveTypeName()) {
                    case BINARY:
                    case FIXED_LEN_BYTE_ARRAY:
                        ((FixedSizeBinaryVector) vec).allocateNew(batchSize);
                        this.readType = ReadType.FIXED_LENGTH_DECIMAL;
                        this.typeWidth = primitive.getTypeLength();
                        break;
                    case INT64:
                        ((BigIntVector) vec).allocateNew(batchSize);
                        this.readType = ReadType.LONG_BACKED_DECIMAL;
                        this.typeWidth = (int) BigIntVector.TYPE_WIDTH;
                        break;
                    case INT32:
                        ((IntVector) vec).allocateNew(batchSize);
                        this.readType = ReadType.INT_BACKED_DECIMAL;
                        this.typeWidth = (int) IntVector.TYPE_WIDTH;
                        break;
                    default:
                        throw new UnsupportedOperationException(
                                "Unsupported base type for decimal: " + primitive.getPrimitiveTypeName());
                }
                break;
            default:
                throw new UnsupportedOperationException("Unsupported logical type: " + primitive.getOriginalType());
        }
    }

    private void allocateVectorBasedOnTypeName(PrimitiveType primitive) {
        switch (primitive.getPrimitiveTypeName()) {
            case FIXED_LEN_BYTE_ARRAY:
                this.readType = ReadType.FIXED_WIDTH_BINARY;
                this.vec = arrowField.createVector(rootAlloc);
                int len = primitive.getTypeLength();
                vec.setInitialCapacity(batchSize * len);
                vec.allocateNew();
                this.typeWidth = len;
                break;
            case BINARY:
                this.vec = arrowField.createVector(rootAlloc);
                // TODO: Possibly use the uncompressed page size info to set the initial capacity
                vec.setInitialCapacity(batchSize * AVERAGE_VARIABLE_WIDTH_RECORD_SIZE);
                vec.allocateNewSafe();
                this.readType = ReadType.VARBINARY;
                this.typeWidth = UNKNOWN_WIDTH;
                break;
            case INT32:
                Field intField = new Field(getColumnName(),
                        new FieldType(isColumnOptional(), new ArrowType.Int(Integer.SIZE, true), null, null), null);
                this.vec = intField.createVector(rootAlloc);
                ((IntVector) vec).allocateNew(batchSize);
                this.readType = ReadType.INT;
                this.typeWidth = (int) IntVector.TYPE_WIDTH;
                break;
            case INT96:
                // Impala & Spark used to write timestamps as INT96 by default. For backwards
                // compatibility we try to read INT96 as timestamps. But INT96 is not recommended
                // and deprecated (see https://issues.apache.org/jira/browse/PARQUET-323)
                int length = BigIntVector.TYPE_WIDTH;
                this.readType = ReadType.TIMESTAMP_INT96;
                this.vec = arrowField.createVector(rootAlloc);
                vec.setInitialCapacity(batchSize * length);
                vec.allocateNew();
                this.typeWidth = length;
                break;
            case FLOAT:
                Field floatField = new Field(getColumnName(),
                        new FieldType(isColumnOptional(), new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE),
                                null, null), null);
                this.vec = floatField.createVector(rootAlloc);
                ((Float4Vector) vec).allocateNew(batchSize);
                this.readType = ReadType.FLOAT;
                this.typeWidth = (int) Float4Vector.TYPE_WIDTH;
                break;
            case BOOLEAN:
                this.vec = arrowField.createVector(rootAlloc);
                ((BitVector) vec).allocateNew(batchSize);
                this.readType = ReadType.BOOLEAN;
                this.typeWidth = UNKNOWN_WIDTH;
                break;
            case INT64:
                this.vec = arrowField.createVector(rootAlloc);
                ((BigIntVector) vec).allocateNew(batchSize);
                this.readType = ReadType.LONG;
                this.typeWidth = (int) BigIntVector.TYPE_WIDTH;
                break;
            case DOUBLE:
                this.vec = arrowField.createVector(rootAlloc);
                ((Float8Vector) vec).allocateNew(batchSize);
                this.readType = ReadType.DOUBLE;
                this.typeWidth = (int) Float8Vector.TYPE_WIDTH;
                break;
            default:
                throw new UnsupportedOperationException("Unsupported type: " + primitive);
        }
    }

    @Override
    public void setRowGroupInfo(PageReadStore source, ColumnChunkMetaData columnChunkMetaData, long rowPosition) {
        this.dictionary = vectorizedColumnIterator.setRowGroupInfo(source.getPageReader(columnDescriptor),
                !ParquetUtil.hasNonDictionaryPages(columnChunkMetaData));
    }

    @Override
    public void close() {
        if (vec != null) {
            vec.close();
        }
    }

    @Override
    public String toString() {
        return columnDescriptor.toString();
    }

    private enum ReadType {
        FIXED_LENGTH_DECIMAL,
        INT_BACKED_DECIMAL,
        LONG_BACKED_DECIMAL,
        VARCHAR,
        VARBINARY,
        FIXED_WIDTH_BINARY,
        BOOLEAN,
        INT,
        LONG,
        FLOAT,
        DOUBLE,
        TIMESTAMP_MILLIS,
        TIMESTAMP_INT96,
        TIME_MICROS,
        DICTIONARY
    }

    private static final class NullVectorReader extends VectorizedArrowReader {
        private static final NullVectorReader INSTANCE = new NullVectorReader();

        @Override
        public VectorHolder read(VectorHolder reuse, int numValsToRead) {
            return VectorHolder.dummyHolder(numValsToRead);
        }

        @Override
        public void setRowGroupInfo(PageReadStore source, ColumnChunkMetaData columnChunkMetaData, long rowPosition) {}

        @Override
        public String toString() {
            return "NullReader";
        }

        @Override
        public void setBatchSize(int batchSize) {}
    }

    /**
     * A Dummy Vector Reader which doesn't actually read files, instead it returns a dummy
     * VectorHolder which indicates the constant value which should be used for this column.
     *
     * @param <T> The constant value to use
     */
    public static class ConstantVectorReader<T> extends VectorizedArrowReader {
        private final T value;

        public ConstantVectorReader(Type icebergField, T value) {
            super();
            this.value = value;
        }

        @Override
        public VectorHolder read(VectorHolder reuse, int numValsToRead) {
            return VectorHolder.constantHolder(numValsToRead, value);
        }

        @Override
        public void setRowGroupInfo(PageReadStore source, ColumnChunkMetaData columnChunkMetaData, long rowPosition) {}

        @Override
        public String toString() {
            return String.format("ConstantReader: %s", value);
        }

        @Override
        public void setBatchSize(int batchSize) {}
    }
}
