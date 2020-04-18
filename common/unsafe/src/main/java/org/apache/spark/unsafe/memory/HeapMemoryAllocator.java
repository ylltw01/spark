/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.unsafe.memory;

import javax.annotation.concurrent.GuardedBy;
import java.lang.ref.WeakReference;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

import org.apache.spark.unsafe.Platform;

/**
 * A simple {@link MemoryAllocator} that can allocate up to 16GB using a JVM long primitive array.
 */
public class HeapMemoryAllocator implements MemoryAllocator {

  @GuardedBy("this")
  private final Map<Long, LinkedList<WeakReference<long[]>>> bufferPoolsBySize = new HashMap<>();

  private static final int POOLING_THRESHOLD_BYTES = 1024 * 1024;

  /**
   * Returns true if allocations of the given size should go through the pooling mechanism and
   * false otherwise.
   */
  private boolean shouldPool(long size) {
    // Very small allocations are less likely to benefit from pooling.
    return size >= POOLING_THRESHOLD_BYTES;
  }

  @Override
  public MemoryBlock allocate(long size) throws OutOfMemoryError {
    // 以8字节对齐的方式申请长度为 ((size + 7) / 8) 的 long 数组，得到 array
    int numWords = (int) ((size + 7) / 8);
    long alignedSize = numWords * 8L;
    assert (alignedSize >= size);
    // 小数据块可以从 pool 中获取
    if (shouldPool(alignedSize)) {
      synchronized (this) {
        // 在  bufferPoolsBySize 中获取所申请的大小的数据块
        final LinkedList<WeakReference<long[]>> pool = bufferPoolsBySize.get(alignedSize);
        if (pool != null) {
          while (!pool.isEmpty()) {
            // 获取弱引用的数据块 long[]
            final WeakReference<long[]> arrayReference = pool.pop();
            final long[] array = arrayReference.get();
            if (array != null) {
              assert (array.length * 8L >= size);
              // 使用 pool 中获取到的 long 数组，初始化 MemoryBlock，数组会包含头部，长度为 Platform.LONG_ARRAY_OFFSET 真正的数据开始位置是在头部之后
              MemoryBlock memory = new MemoryBlock(array, Platform.LONG_ARRAY_OFFSET, size);
              if (MemoryAllocator.MEMORY_DEBUG_FILL_ENABLED) {
                memory.fill(MemoryAllocator.MEMORY_DEBUG_FILL_CLEAN_VALUE);
              }
              return memory;
            }
          }
          // 该内存块已经使用过了, 则移除 bufferPoolsBySize
          bufferPoolsBySize.remove(alignedSize);
        }
      }
    }
    // 直接在 jvm 堆中创建大小为 numWords 的 long 数组，numWords = (int) ((size + 7) / 8) 一个 long 占用 8 个字节
    long[] array = new long[numWords];
    // 初始化 MemoryBlock，数组会包含头部，长度为 Platform.LONG_ARRAY_OFFSET 真正的数据开始位置是在头部之后
    MemoryBlock memory = new MemoryBlock(array, Platform.LONG_ARRAY_OFFSET, size);
    if (MemoryAllocator.MEMORY_DEBUG_FILL_ENABLED) {
      memory.fill(MemoryAllocator.MEMORY_DEBUG_FILL_CLEAN_VALUE);
    }
    return memory;
  }

  @Override
  public void free(MemoryBlock memory) {
    assert (memory.obj != null) :
      "baseObject was null; are you trying to use the on-heap allocator to free off-heap memory?";
    assert (memory.pageNumber != MemoryBlock.FREED_IN_ALLOCATOR_PAGE_NUMBER) :
      "page has already been freed";
    assert ((memory.pageNumber == MemoryBlock.NO_PAGE_NUMBER)
            || (memory.pageNumber == MemoryBlock.FREED_IN_TMM_PAGE_NUMBER)) :
      "TMM-allocated pages must first be freed via TMM.freePage(), not directly in allocator " +
        "free()";

    final long size = memory.size();
    if (MemoryAllocator.MEMORY_DEBUG_FILL_ENABLED) {
      memory.fill(MemoryAllocator.MEMORY_DEBUG_FILL_FREED_VALUE);
    }

    // Mark the page as freed (so we can detect double-frees).
    // 标记该 MemoryBlock 的 pageNumber 为 已经释放
    memory.pageNumber = MemoryBlock.FREED_IN_ALLOCATOR_PAGE_NUMBER;

    // As an additional layer of defense against use-after-free bugs, we mutate the
    // MemoryBlock to null out its reference to the long[] array.
    long[] array = (long[]) memory.obj;
    // 将 MemoryBlock 设置为 null，且将偏移量设置为 0
    memory.setObjAndOffset(null, 0);

    long alignedSize = ((size + 7) / 8) * 8;
    // 小数据块可以缓存在 pool 中，allocate 方法便可获取
    if (shouldPool(alignedSize)) {
      synchronized (this) {
        LinkedList<WeakReference<long[]>> pool = bufferPoolsBySize.get(alignedSize);
        if (pool == null) {
          pool = new LinkedList<>();
          bufferPoolsBySize.put(alignedSize, pool);
        }
        pool.add(new WeakReference<>(array));
      }
    } else {
      // Do nothing
    }
  }
}
