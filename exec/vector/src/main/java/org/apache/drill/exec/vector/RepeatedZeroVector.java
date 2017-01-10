/*
* Licensed to the Apache Software Foundation (ASF) under one or more
* contributor license agreements.  See the NOTICE file distributed with
* this work for additional information regarding copyright ownership.
* The ASF licenses this file to you under the Apache License, Version 2.0
* (the "License"); you may not use this file except in compliance with
* the License.  You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
package org.apache.drill.exec.vector;

import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.TransferPair;
import org.apache.drill.exec.vector.complex.BaseRepeatedValueVector;
import org.apache.drill.exec.vector.complex.RepeatedFixedWidthVectorLike;
import org.apache.drill.exec.vector.complex.impl.NullReader;
import org.apache.drill.exec.vector.complex.reader.FieldReader;

import java.util.Collections;
import java.util.List;

public class RepeatedZeroVector extends BaseRepeatedValueVector implements RepeatedFixedWidthVectorLike {

  public RepeatedZeroVector(MaterializedField field, BufferAllocator allocator) {
    super(field, allocator);
  }

  private static final RepeatedAccessor ACCESSOR = new RepeatedAccessor() {
    @Override
    public int getInnerValueCount() {
      return 0;
    }

    @Override
    public int getInnerValueCountAt(int index) {
      return 0;
    }

    @Override
    public boolean isEmpty(int index) {
      return true;
    }

    @Override
    public List<Integer> getObject(int index) {
      return Collections.emptyList();
    }

    @Override
    public int getValueCount() {
      return 1;
    }

    @Override
    public boolean isNull(int index) {
      return true;
    }
  };

  private final RepeatedMutator mutator = new BaseRepeatedValueVector.BaseRepeatedMutator() {};

  @Override
  public RepeatedMutator getMutator() {
    return mutator;
  }

  @Override
  public RepeatedAccessor getAccessor() {
    return ACCESSOR;
  }

  @Override
  public FieldReader getReader() {
    return NullReader.INSTANCE;
  }

  @Override
  public ZeroVector getDataVector() {
    return ZeroVector.INSTANCE;
  }

  @Override
  public TransferPair getTransferPair(BufferAllocator allocator) {
    return new RepeatedZeroVector.TransferImpl(getField(), allocator);
  }

  @Override
  public TransferPair getTransferPair(String ref, BufferAllocator allocator){
    return new RepeatedZeroVector.TransferImpl(getField().withPath(ref), allocator);
  }

  @Override
  public TransferPair makeTransferPair(ValueVector to) {
    return new RepeatedZeroVector.TransferImpl((RepeatedZeroVector) to);
  }

  public void transferTo(RepeatedZeroVector target) {
    target.clear();
    offsets.transferTo(target.offsets);
    clear();
  }

  private class TransferImpl implements TransferPair {
    final RepeatedZeroVector to;

    public TransferImpl(MaterializedField field, BufferAllocator allocator) {
      this.to = new RepeatedZeroVector(field, allocator);
    }

    public TransferImpl(RepeatedZeroVector to) {
      this.to = to;
    }

    @Override
    public RepeatedZeroVector getTo() {
      return to;
    }

    @Override
    public void transfer() {
      transferTo(to);
    }

    @Override
    public void splitAndTransfer(int startIndex, int length) {

    }

    @Override
    public void copyValueSafe(int fromIndex, int toIndex) {

    }
  }

  @Override
  public boolean allocateNewSafe() {
    return true;
  }

  @Override
  public void allocateNew() {

  }

  @Override
  public void allocateNew(int valueCount, int innerValueCount) {

  }
}
