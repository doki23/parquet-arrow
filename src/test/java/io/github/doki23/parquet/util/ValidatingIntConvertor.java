package io.github.doki23.parquet.util;

import org.apache.arrow.vector.IntVector;
import org.apache.parquet.io.api.PrimitiveConverter;
import org.junit.jupiter.api.Assertions;

public class ValidatingIntConvertor extends PrimitiveConverter {
    private IntVector vector;
    private int i = 0;

    public ValidatingIntConvertor(IntVector vector) {
        this.vector = vector;
    }

    public void reset() {
        i = 0;
        vector.setValueCount(0);
    }

    @Override
    public void addInt(int value) {
        Assertions.assertEquals(value, vector.get(i++), () -> "value miss match at index " + i);
    }
}
