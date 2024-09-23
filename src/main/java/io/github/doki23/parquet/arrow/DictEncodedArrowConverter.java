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
import org.apache.arrow.vector.BaseFixedWidthVector;
import org.apache.arrow.vector.BaseVariableWidthVector;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.TimeMicroVector;
import org.apache.arrow.vector.TimeStampMilliTZVector;
import org.apache.arrow.vector.TimeStampVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.ArrowType.Binary;
import org.apache.arrow.vector.types.pojo.ArrowType.Decimal;
import org.apache.arrow.vector.types.pojo.ArrowType.Int;
import org.apache.arrow.vector.types.pojo.ArrowType.Timestamp;
import org.apache.arrow.vector.types.pojo.ArrowType.Utf8;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.function.IntConsumer;

import static org.apache.arrow.vector.types.pojo.ArrowType.FloatingPoint;

/**
 * This converts dictionary encoded arrow vectors to a correctly typed arrow vector.
 */
public class DictEncodedArrowConverter {

    private DictEncodedArrowConverter() {}

    @SuppressWarnings("checkstyle:CyclomaticComplexity")
    public static FieldVector toArrowVector(VectorHolder vectorHolder, ArrowVectorAccessor<?, String, ?, ?> accessor) {
        Preconditions.checkArgument(null != vectorHolder, "Invalid vector holder: null");
        Preconditions.checkArgument(null != accessor, "Invalid arrow vector accessor: null");

        ArrowType arrowType = vectorHolder.field().getType();
        if (vectorHolder.isDictionaryEncoded()) {
            if (arrowType instanceof Decimal) {
                return toDecimalVector(vectorHolder, accessor);
            } else if (arrowType instanceof Timestamp) {
                return toTimestampVector(vectorHolder, accessor);
            } else if (arrowType instanceof Int && ((Int) arrowType).getBitWidth() == 64) {
                return toBigIntVector(vectorHolder, accessor);
            } else if (arrowType instanceof FloatingPoint && ((FloatingPoint) arrowType).getPrecision() == FloatingPointPrecision.SINGLE) {
                return toFloat4Vector(vectorHolder, accessor);
            } else if (arrowType instanceof FloatingPoint && ((FloatingPoint) arrowType).getPrecision() == FloatingPointPrecision.DOUBLE) {
                return toFloat8Vector(vectorHolder, accessor);
            } else if (arrowType instanceof Utf8) {
                return toVarCharVector(vectorHolder, accessor);
            } else if (arrowType instanceof Binary) {
                return toVarBinaryVector(vectorHolder, accessor);
            }

            throw new IllegalArgumentException(String.format(
                    "Cannot convert dict encoded field '%s' of type '%s' to Arrow " + "vector as it is currently not supported",
                    vectorHolder.field().getName(),
                    vectorHolder.field().getType()));
        }

        return vectorHolder.vector();
    }

    private static DecimalVector toDecimalVector(VectorHolder vectorHolder,
                                                 ArrowVectorAccessor<?, String, ?, ?> accessor) {
        Decimal decimalType = (Decimal) vectorHolder.field().getType();
        int precision = decimalType.getPrecision();
        int scale = decimalType.getScale();

        DecimalVector vector = new DecimalVector(vectorHolder.vector().getName(),
                vectorHolder.field().getFieldType(),
                vectorHolder.vector().getAllocator());

        initVector(vector,
                vectorHolder,
                idx -> vector.set(idx, (BigDecimal) accessor.getDecimal(idx, precision, scale)));
        return vector;
    }

    private static TimeStampVector toTimestampVector(VectorHolder vectorHolder,
                                                     ArrowVectorAccessor<?, String, ?, ?> accessor) {
        TimeStampVector vector = new TimeStampMilliTZVector(vectorHolder.vector().getName(),
                vectorHolder.field().getFieldType(),
                vectorHolder.vector().getAllocator());
        initVector(vector, vectorHolder, idx -> vector.set(idx, accessor.getLong(idx)));
        return vector;
    }

    private static BigIntVector toBigIntVector(VectorHolder vectorHolder,
                                               ArrowVectorAccessor<?, String, ?, ?> accessor) {
        BigIntVector vector = new BigIntVector(vectorHolder.vector().getName(),
                vectorHolder.field().getFieldType(),
                vectorHolder.vector().getAllocator());

        initVector(vector, vectorHolder, idx -> vector.set(idx, accessor.getLong(idx)));
        return vector;
    }

    private static Float4Vector toFloat4Vector(VectorHolder vectorHolder,
                                               ArrowVectorAccessor<?, String, ?, ?> accessor) {
        Float4Vector vector = new Float4Vector(vectorHolder.vector().getName(),
                vectorHolder.field().getFieldType(),
                vectorHolder.vector().getAllocator());

        initVector(vector, vectorHolder, idx -> vector.set(idx, accessor.getFloat(idx)));
        return vector;
    }

    private static Float8Vector toFloat8Vector(VectorHolder vectorHolder,
                                               ArrowVectorAccessor<?, String, ?, ?> accessor) {
        Float8Vector vector = new Float8Vector(vectorHolder.vector().getName(),
                vectorHolder.field().getFieldType(),
                vectorHolder.vector().getAllocator());

        initVector(vector, vectorHolder, idx -> vector.set(idx, accessor.getDouble(idx)));
        return vector;
    }

    private static VarCharVector toVarCharVector(VectorHolder vectorHolder,
                                                 ArrowVectorAccessor<?, String, ?, ?> accessor) {
        VarCharVector vector = new VarCharVector(vectorHolder.vector().getName(),
                vectorHolder.field().getFieldType(),
                vectorHolder.vector().getAllocator());

        initVector(vector,
                vectorHolder,
                idx -> vector.setSafe(idx, accessor.getUTF8String(idx).getBytes(StandardCharsets.UTF_8)));
        return vector;
    }

    private static VarBinaryVector toVarBinaryVector(VectorHolder vectorHolder,
                                                     ArrowVectorAccessor<?, String, ?, ?> accessor) {
        VarBinaryVector vector = new VarBinaryVector(vectorHolder.vector().getName(),
                vectorHolder.field().getFieldType(),
                vectorHolder.vector().getAllocator());

        initVector(vector, vectorHolder, idx -> vector.setSafe(idx, accessor.getBinary(idx)));
        return vector;
    }

    private static TimeMicroVector toTimeMicroVector(VectorHolder vectorHolder,
                                                     ArrowVectorAccessor<?, String, ?, ?> accessor) {
        TimeMicroVector vector = new TimeMicroVector(vectorHolder.vector().getName(),
                vectorHolder.field().getFieldType(),
                vectorHolder.vector().getAllocator());

        initVector(vector, vectorHolder, idx -> vector.set(idx, accessor.getLong(idx)));
        return vector;
    }

    private static void initVector(BaseFixedWidthVector vector, VectorHolder vectorHolder, IntConsumer consumer) {
        vector.allocateNew(vectorHolder.vector().getValueCount());
        init(vector, vectorHolder, consumer, vectorHolder.vector().getValueCount());
    }

    private static void initVector(BaseVariableWidthVector vector, VectorHolder vectorHolder, IntConsumer consumer) {
        vector.allocateNew(vectorHolder.vector().getValueCount());
        init(vector, vectorHolder, consumer, vectorHolder.vector().getValueCount());
    }

    private static void init(FieldVector vector, VectorHolder vectorHolder, IntConsumer consumer, int valueCount) {
        for (int i = 0; i < valueCount; i++) {
            if (isNullAt(vectorHolder, i)) {
                vector.setNull(i);
            } else {
                consumer.accept(i);
            }
        }

        vector.setValueCount(valueCount);
    }

    private static boolean isNullAt(VectorHolder vectorHolder, int idx) {
        return vectorHolder.nullabilityHolder().isNullAt(idx) == 1;
    }
}
