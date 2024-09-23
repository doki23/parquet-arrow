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

import io.github.doki23.parquet.vectorized.NullabilityHolder;
import org.apache.arrow.vector.FieldVector;

import java.math.BigDecimal;

/**
 * This class is inspired by Spark's {@code ColumnVector}. It wraps an arrow {@link FieldVector} and
 * provides simple accessors for the row values. Advanced users can access the {@link FieldVector}.
 */
public class ColumnVector implements AutoCloseable {
    private final VectorHolder vectorHolder;
    private final ArrowVectorAccessor<?, String, ?, ?> accessor;
    private final NullabilityHolder nullabilityHolder;

    ColumnVector(VectorHolder vectorHolder) {
        this.vectorHolder = vectorHolder;
        this.nullabilityHolder = vectorHolder.nullabilityHolder();
        this.accessor = getVectorAccessor(vectorHolder);
    }

    private static ArrowVectorAccessor<?, String, ?, ?> getVectorAccessor(VectorHolder holder) {
        return ArrowVectorAccessors.getVectorAccessor(holder);
    }

    /**
     * Returns the potentially dict-encoded {@link FieldVector}.
     *
     * @return instance of {@link FieldVector}
     */
    public FieldVector getFieldVector() {
        return vectorHolder.vector();
    }

    /**
     * Decodes a dict-encoded vector and returns the actual arrow vector.
     *
     * @return instance of {@link FieldVector}
     */
    public FieldVector getArrowVector() {
        return DictEncodedArrowConverter.toArrowVector(vectorHolder, accessor);
    }

    public boolean hasNull() {
        return nullabilityHolder.hasNulls();
    }

    public int numNulls() {
        return nullabilityHolder.numNulls();
    }

    @Override
    public void close() {
        accessor.close();
    }

    public boolean isNullAt(int rowId) {
        return nullabilityHolder.isNullAt(rowId) == 1;
    }

    public boolean getBoolean(int rowId) {
        return accessor.getBoolean(rowId);
    }

    public byte getByte(int rowId) {
        return accessor.getByte(rowId);
    }

    public short getShort(int rowId) {
        return accessor.getShort(rowId);
    }

    public int getInt(int rowId) {
        return accessor.getInt(rowId);
    }

    public long getLong(int rowId) {
        return accessor.getLong(rowId);
    }

    public float getFloat(int rowId) {
        return accessor.getFloat(rowId);
    }

    public double getDouble(int rowId) {
        return accessor.getDouble(rowId);
    }

    public String getString(int rowId) {
        if (isNullAt(rowId)) {
            return null;
        }
        return accessor.getUTF8String(rowId);
    }

    public byte[] getBinary(int rowId) {
        if (isNullAt(rowId)) {
            return null;
        }
        return accessor.getBinary(rowId);
    }

    public BigDecimal getDecimal(int rowId, int precision, int scale) {
        if (isNullAt(rowId)) {
            return null;
        }
        return (BigDecimal) accessor.getDecimal(rowId, precision, scale);
    }
}
