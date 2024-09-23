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
import io.github.doki23.parquet.vectorized.NullabilityHolder;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.Dictionary;

/**
 * Container class for holding the Arrow vector storing a batch of values along with other state
 * needed for reading values out of it.
 */
public class VectorHolder {
  private final ColumnDescriptor columnDescriptor;
  private final FieldVector vector;
  private final boolean isDictionaryEncoded;
  private final Dictionary dictionary;
  private final NullabilityHolder nullabilityHolder;

  public VectorHolder(ColumnDescriptor columnDescriptor, FieldVector vector, boolean isDictionaryEncoded,
                      Dictionary dictionary, NullabilityHolder holder) {
    // All the fields except dictionary are not nullable unless it is a dummy holder
    Preconditions.checkNotNull(columnDescriptor, "ColumnDescriptor cannot be null");
    Preconditions.checkNotNull(vector, "Vector cannot be null");
    Preconditions.checkNotNull(holder, "NullabilityHolder cannot be null");
    this.columnDescriptor = columnDescriptor;
    this.vector = vector;
    this.isDictionaryEncoded = isDictionaryEncoded;
    this.dictionary = dictionary;
    this.nullabilityHolder = holder;
  }

  /**
   * A constructor used for typed constant holders.
   */
  private VectorHolder() {
    columnDescriptor = null;
    vector = null;
    isDictionaryEncoded = false;
    dictionary = null;
    nullabilityHolder = null;
  }

  private VectorHolder(FieldVector vec, NullabilityHolder nulls) {
    columnDescriptor = null;
    vector = vec;
    isDictionaryEncoded = false;
    dictionary = null;
    nullabilityHolder = nulls;
  }

  public static <T> VectorHolder constantHolder(int numRows, T constantValue) {
    return new ConstantVectorHolder<>(numRows, constantValue);
  }

  public static VectorHolder dummyHolder(int numRows) {
    return new ConstantVectorHolder<>(numRows);
  }

  public ColumnDescriptor descriptor() {
    return columnDescriptor;
  }

  public FieldVector vector() {
    return vector;
  }

  public Field field() {
    return vector.getField();
  }

  public boolean isDictionaryEncoded() {
    return isDictionaryEncoded;
  }

  public Dictionary dictionary() {
    return dictionary;
  }

  public NullabilityHolder nullabilityHolder() {
    return nullabilityHolder;
  }

  public int numValues() {
    return vector.getValueCount();
  }

  public boolean isDummy() {
    return vector == null;
  }

  /**
   * A Vector Holder which does not actually produce values, consumers of this class should use the
   * constantValue to populate their ColumnVector implementation.
   */
  public static class ConstantVectorHolder<T> extends VectorHolder {
    private final T constantValue;
    private final int numRows;

    public ConstantVectorHolder(int numRows) {
      this.numRows = numRows;
      this.constantValue = null;
    }

    public ConstantVectorHolder(int numRows, T constantValue) {
      super();
      this.numRows = numRows;
      this.constantValue = constantValue;
    }

    @Override
    public int numValues() {
      return this.numRows;
    }

    public Object getConstant() {
      return constantValue;
    }
  }
}
