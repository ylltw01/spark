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

package org.apache.spark.shuffle.sort;

import javax.annotation.Nullable;
import java.io.*;
import java.nio.channels.FileChannel;
import java.util.Iterator;

import scala.Option;
import scala.Product2;
import scala.collection.JavaConverters;
import scala.reflect.ClassTag;
import scala.reflect.ClassTag$;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.io.ByteStreams;
import com.google.common.io.Closeables;
import com.google.common.io.Files;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.spark.*;
import org.apache.spark.annotation.Private;
import org.apache.spark.internal.config.package$;
import org.apache.spark.io.CompressionCodec;
import org.apache.spark.io.CompressionCodec$;
import org.apache.spark.io.NioBufferedFileInputStream;
import org.apache.commons.io.output.CloseShieldOutputStream;
import org.apache.commons.io.output.CountingOutputStream;
import org.apache.spark.memory.TaskMemoryManager;
import org.apache.spark.network.util.LimitedInputStream;
import org.apache.spark.scheduler.MapStatus;
import org.apache.spark.scheduler.MapStatus$;
import org.apache.spark.shuffle.ShuffleWriteMetricsReporter;
import org.apache.spark.serializer.SerializationStream;
import org.apache.spark.serializer.SerializerInstance;
import org.apache.spark.shuffle.IndexShuffleBlockResolver;
import org.apache.spark.shuffle.ShuffleWriter;
import org.apache.spark.storage.BlockManager;
import org.apache.spark.storage.TimeTrackingOutputStream;
import org.apache.spark.unsafe.Platform;
import org.apache.spark.util.Utils;

@Private
public class UnsafeShuffleWriter<K, V> extends ShuffleWriter<K, V> {

  private static final Logger logger = LoggerFactory.getLogger(UnsafeShuffleWriter.class);

  private static final ClassTag<Object> OBJECT_CLASS_TAG = ClassTag$.MODULE$.Object();

  @VisibleForTesting
  static final int DEFAULT_INITIAL_SORT_BUFFER_SIZE = 4096;
  static final int DEFAULT_INITIAL_SER_BUFFER_SIZE = 1024 * 1024;

  private final BlockManager blockManager;
  private final IndexShuffleBlockResolver shuffleBlockResolver;
  private final TaskMemoryManager memoryManager;
  private final SerializerInstance serializer;
  private final Partitioner partitioner;
  private final ShuffleWriteMetricsReporter writeMetrics;
  private final int shuffleId;
  private final int mapId;
  private final TaskContext taskContext;
  private final SparkConf sparkConf;
  private final boolean transferToEnabled;
  private final int initialSortBufferSize;
  private final int inputBufferSizeInBytes;
  private final int outputBufferSizeInBytes;

  @Nullable private MapStatus mapStatus;
  @Nullable private ShuffleExternalSorter sorter;
  private long peakMemoryUsedBytes = 0;

  /** Subclass of ByteArrayOutputStream that exposes `buf` directly. */
  private static final class MyByteArrayOutputStream extends ByteArrayOutputStream {
    MyByteArrayOutputStream(int size) { super(size); }
    public byte[] getBuf() { return buf; }
  }

  private MyByteArrayOutputStream serBuffer;
  private SerializationStream serOutputStream;

  /**
   * Are we in the process of stopping? Because map tasks can call stop() with success = true
   * and then call stop() with success = false if they get an exception, we want to make sure
   * we don't try deleting files, etc twice.
   */
  private boolean stopping = false;

  private class CloseAndFlushShieldOutputStream extends CloseShieldOutputStream {

    CloseAndFlushShieldOutputStream(OutputStream outputStream) {
      super(outputStream);
    }

    @Override
    public void flush() {
      // do nothing
    }
  }

  public UnsafeShuffleWriter(
      BlockManager blockManager,
      IndexShuffleBlockResolver shuffleBlockResolver,
      TaskMemoryManager memoryManager,
      SerializedShuffleHandle<K, V> handle,
      int mapId,
      TaskContext taskContext,
      SparkConf sparkConf,
      ShuffleWriteMetricsReporter writeMetrics) throws IOException {
    final int numPartitions = handle.dependency().partitioner().numPartitions();
    if (numPartitions > SortShuffleManager.MAX_SHUFFLE_OUTPUT_PARTITIONS_FOR_SERIALIZED_MODE()) {
      throw new IllegalArgumentException(
        "UnsafeShuffleWriter can only be used for shuffles with at most " +
        SortShuffleManager.MAX_SHUFFLE_OUTPUT_PARTITIONS_FOR_SERIALIZED_MODE() +
        " reduce partitions");
    }
    this.blockManager = blockManager;
    this.shuffleBlockResolver = shuffleBlockResolver;
    this.memoryManager = memoryManager;
    this.mapId = mapId;
    final ShuffleDependency<K, V, V> dep = handle.dependency();
    this.shuffleId = dep.shuffleId();
    this.serializer = dep.serializer().newInstance();
    this.partitioner = dep.partitioner();
    this.writeMetrics = writeMetrics;
    this.taskContext = taskContext;
    this.sparkConf = sparkConf;
    // spark.file.transferTo    该参数指定 UnsafeShuffleWriter 是否使用 nio 方式执行 spill 文件的 merge
    this.transferToEnabled = sparkConf.getBoolean("spark.file.transferTo", true);
    // spark.shuffle.sort.initialBufferSize    4096byte
    this.initialSortBufferSize =
      (int) (long) sparkConf.get(package$.MODULE$.SHUFFLE_SORT_INIT_BUFFER_SIZE());
    // spark.shuffle.file.buffer    32k write task 写buffer缓冲大小，大于该值溢写磁盘
    this.inputBufferSizeInBytes =
      (int) (long) sparkConf.get(package$.MODULE$.SHUFFLE_FILE_BUFFER_SIZE()) * 1024;
    // spark.shuffle.unsafe.file.output.buffer   merge文件输出的缓冲大小   32k
    this.outputBufferSizeInBytes =
      (int) (long) sparkConf.get(package$.MODULE$.SHUFFLE_UNSAFE_FILE_OUTPUT_BUFFER_SIZE()) * 1024;
    open();
  }

  private void updatePeakMemoryUsed() {
    // sorter can be null if this writer is closed
    if (sorter != null) {
      long mem = sorter.getPeakMemoryUsedBytes();
      if (mem > peakMemoryUsedBytes) {
        peakMemoryUsedBytes = mem;
      }
    }
  }

  /**
   * Return the peak memory used so far, in bytes.
   */
  public long getPeakMemoryUsedBytes() {
    updatePeakMemoryUsed();
    return peakMemoryUsedBytes;
  }

  /**
   * This convenience method should only be called in test code.
   */
  @VisibleForTesting
  public void write(Iterator<Product2<K, V>> records) throws IOException {
    write(JavaConverters.asScalaIteratorConverter(records).asScala());
  }

  @Override
  public void write(scala.collection.Iterator<Product2<K, V>> records) throws IOException {
    // Keep track of success so we know if we encountered an exception
    // We do this rather than a standard try/catch/re-throw to handle
    // generic throwables.
    boolean success = false;
    try {
      while (records.hasNext()) {
        // 排序并 spill 文件
        insertRecordIntoSorter(records.next());
      }
      // merge spill 文件, 并输出
      closeAndWriteOutput();
      success = true;
    } finally {
      if (sorter != null) {
        try {
          sorter.cleanupResources();
        } catch (Exception e) {
          // Only throw this error if we won't be masking another
          // error.
          if (success) {
            throw e;
          } else {
            logger.error("In addition to a failure during writing, we failed during " +
                         "cleanup.", e);
          }
        }
      }
    }
  }

  private void open() {
    assert (sorter == null);
    sorter = new ShuffleExternalSorter(
      memoryManager,
      blockManager,
      taskContext,
      initialSortBufferSize,
      partitioner.numPartitions(),
      sparkConf,
      writeMetrics);
    // 序列化输出流, 就是一个 java.io.ByteArrayOutputStream, 默认为 1024 * 1024
    serBuffer = new MyByteArrayOutputStream(DEFAULT_INITIAL_SER_BUFFER_SIZE);
    // 指定序列化输出流
    serOutputStream = serializer.serializeStream(serBuffer);
  }

  @VisibleForTesting
  void closeAndWriteOutput() throws IOException {
    assert(sorter != null);
    updatePeakMemoryUsed();
    serBuffer = null;
    serOutputStream = null;
    // 将还存在内存中的数据 spill 至磁盘并返回 shuffle 所有 spill 信息
    final SpillInfo[] spills = sorter.closeAndGetSpills();
    sorter = null;
    final long[] partitionLengths;
    // 生成此次 shuffle 的 File
    final File output = shuffleBlockResolver.getDataFile(shuffleId, mapId);
    // 生成临时 File
    final File tmp = Utils.tempFileWith(output);
    try {
      try {
        // 核心方法, 归并所有 spill 文件, 合并为一个 temp 文件, 并返回每个分区所在该文件的 offset
        partitionLengths = mergeSpills(spills, tmp);
      } finally {
        // 清理 spill 的文件
        for (SpillInfo spill : spills) {
          if (spill.file.exists() && ! spill.file.delete()) {
            logger.error("Error while deleting spill file {}", spill.file.getPath());
          }
        }
      }
      // 写包含每个分区的索引文件和修改 tmp 文件为 output 文件
      shuffleBlockResolver.writeIndexFileAndCommit(shuffleId, mapId, partitionLengths, tmp);
    } finally {
      // 清理 tmp 文件
      if (tmp.exists() && !tmp.delete()) {
        logger.error("Error while deleting temp file {}", tmp.getAbsolutePath());
      }
    }
    mapStatus = MapStatus$.MODULE$.apply(blockManager.shuffleServerId(), partitionLengths);
  }

  @VisibleForTesting
  void insertRecordIntoSorter(Product2<K, V> record) throws IOException {
    assert(sorter != null);
    final K key = record._1();
    // 根据输入的 key 以及分区方法得到该 key 所对应的 partitionId
    final int partitionId = partitioner.getPartition(key);
    // 重置 ByteArrayOutputStream , 重用已分配的内存
    serBuffer.reset();
    // 序列化 key, value 并写入 序列化 buffer
    serOutputStream.writeKey(key, OBJECT_CLASS_TAG);
    serOutputStream.writeValue(record._2(), OBJECT_CLASS_TAG);
    serOutputStream.flush();
    // 返回当前 OutputStream 中 buff 的大小
    final int serializedRecordSize = serBuffer.size();
    assert (serializedRecordSize > 0);
    // 排序数据, 序列化后数据 byte,      byte[] 对象头长度 ,序列化后大小,        分区 id
    sorter.insertRecord(
      serBuffer.getBuf(), Platform.BYTE_ARRAY_OFFSET, serializedRecordSize, partitionId);
  }

  @VisibleForTesting
  void forceSorterToSpill() throws IOException {
    assert (sorter != null);
    sorter.spill();
  }

  /**
   * Merge zero or more spill files together, choosing the fastest merging strategy based on the
   * number of spills and the IO compression codec.
   * outputFile 临时文件
   * @return the partition lengths in the merged file.
   */
  private long[] mergeSpills(SpillInfo[] spills, File outputFile) throws IOException {
    // 是否开启shuffle 压缩   spark.shuffle.compress  默认为 true 开启后将使用 spark.io.compression.codec
    final boolean compressionEnabled = (boolean) sparkConf.get(package$.MODULE$.SHUFFLE_COMPRESS());
    // 获取压缩类, 即 spark.io.compression.codec 配置, 默认为 lz4 ( LZ4CompressionCodec 类 )
    final CompressionCodec compressionCodec = CompressionCodec$.MODULE$.createCodec(sparkConf);
    // spark.shuffle.unsafe.fastMergeEnabled 是否启用 fast merge 默认为 true
    final boolean fastMergeEnabled =
      (boolean) sparkConf.get(package$.MODULE$.SHUFFLE_UNDAFE_FAST_MERGE_ENABLE());
    // 压缩类, 是否支持 fast merge, 目前 Spark 的 4 种压缩都支持
    final boolean fastMergeIsSupported = !compressionEnabled ||
      CompressionCodec$.MODULE$.supportsConcatenationOfSerializedStreams(compressionCodec);
    // 是否启用 io 加密  spark.io.encryption.enabled  默认 false
    final boolean encryptionEnabled = blockManager.serializerManager().encryptionEnabled();
    try {
      if (spills.length == 0) {
        // spill 文件为空, 则生成一个空文件
        new FileOutputStream(outputFile).close(); // Create an empty file
        return new long[partitioner.numPartitions()];
      } else if (spills.length == 1) {
        // Here, we don't need to perform any metrics updates because the bytes written to this
        // output file would have already been counted as shuffle bytes written.
        // spill 文件仅一个, 则直接将其移动到输出文件. 并返回该文件中每个分区的偏移量
        Files.move(spills[0].file, outputFile);
        return spills[0].partitionLengths;
      } else {
        final long[] partitionLengths;
        // There are multiple spills to merge, so none of these spill files' lengths were counted
        // towards our shuffle write count or shuffle write time. If we use the slow merge path,
        // then the final output file's size won't necessarily be equal to the sum of the spill
        // files' sizes. To guard against this case, we look at the output file's actual size when
        // computing shuffle bytes written.
        //
        // We allow the individual merge methods to report their own IO times since different merge
        // strategies use different IO techniques.  We count IO during merge towards the shuffle
        // shuffle write time, which appears to be consistent with the "not bypassing merge-sort"
        // branch in ExternalSorter.
        // 开启了 fastMerge 且 压缩类支持 fastMerge
        if (fastMergeEnabled && fastMergeIsSupported) {
          // Compression is disabled or we are using an IO compression codec that supports
          // decompression of concatenated compressed streams, so we can perform a fast spill merge
          // that doesn't need to interpret the spilled bytes.
          // 使用 nio transferTo 且 非 io 加密
          if (transferToEnabled && !encryptionEnabled) {
            logger.debug("Using transferTo-based fast merge");
            // 使用 nio  transferTo 按分区进行 merge 所有 spill 文件至 outputFile
            partitionLengths = mergeSpillsWithTransferTo(spills, outputFile);
          } else {
            logger.debug("Using fileStream-based fast merge");
            // 使用 nio  transferTo 按分区进行 merge 所有 spill 文件至 outputFile
            partitionLengths = mergeSpillsWithFileStream(spills, outputFile, null);
          }
        } else {
          logger.debug("Using slow merge");
          // 使用 nio  transferTo 按分区进行 merge 所有 spill 文件至 outputFile
          partitionLengths = mergeSpillsWithFileStream(spills, outputFile, compressionCodec);
        }
        // When closing an UnsafeShuffleExternalSorter that has already spilled once but also has
        // in-memory records, we write out the in-memory records to a file but do not count that
        // final write as bytes spilled (instead, it's accounted as shuffle write). The merge needs
        // to be counted as shuffle write, but this will lead to double-counting of the final
        // SpillInfo's bytes.
        writeMetrics.decBytesWritten(spills[spills.length - 1].file.length());
        writeMetrics.incBytesWritten(outputFile.length());
        return partitionLengths;
      }
    } catch (IOException e) {
      if (outputFile.exists() && !outputFile.delete()) {
        logger.error("Unable to delete output file {}", outputFile.getPath());
      }
      throw e;
    }
  }

  /**
   * Merges spill files using Java FileStreams. This code path is typically slower than
   * the NIO-based merge, {@link UnsafeShuffleWriter#mergeSpillsWithTransferTo(SpillInfo[],
   * File)}, and it's mostly used in cases where the IO compression codec does not support
   * concatenation of compressed data, when encryption is enabled, or when users have
   * explicitly disabled use of {@code transferTo} in order to work around kernel bugs.
   * This code path might also be faster in cases where individual partition size in a spill
   * is small and UnsafeShuffleWriter#mergeSpillsWithTransferTo method performs many small
   * disk ios which is inefficient. In those case, Using large buffers for input and output
   * files helps reducing the number of disk ios, making the file merging faster.
   *
   * @param spills the spills to merge.
   * @param outputFile the file to write the merged data to.
   * @param compressionCodec the IO compression codec, or null if shuffle compression is disabled.
   * @return the partition lengths in the merged file.
   */
  private long[] mergeSpillsWithFileStream(
      SpillInfo[] spills,
      File outputFile,
      @Nullable CompressionCodec compressionCodec) throws IOException {
    assert (spills.length >= 2);
    // 分区数
    final int numPartitions = partitioner.numPartitions();
    // 初始化用于封装返回每个 partition 在输出的文件偏移量
    final long[] partitionLengths = new long[numPartitions];
    // 初始化 spill 文件 InputStream 数组
    final InputStream[] spillInputStreams = new InputStream[spills.length];
    // 初始化 outputFile OutputStream, outputBufferSizeInBytes = spark.shuffle.unsafe.file.output.buffer   merge文件输出的缓冲大小   32k
    final OutputStream bos = new BufferedOutputStream(
            new FileOutputStream(outputFile),
            outputBufferSizeInBytes);
    // Use a counting output stream to avoid having to close the underlying file and ask
    // the file system for its size after each partition is written.
    // 使用 CountingOutputStream 包装 OutputStream, CountingOutputStream 提供对输出流数据的统计
    final CountingOutputStream mergedFileOutputStream = new CountingOutputStream(bos);

    boolean threwException = true;
    try {
      for (int i = 0; i < spills.length; i++) {
        // 使用   NioBufferedFileInputStream 包装类初始化每个 spill 文件 InputStream
        spillInputStreams[i] = new NioBufferedFileInputStream(
            spills[i].file,
            inputBufferSizeInBytes);
      }
      for (int partition = 0; partition < numPartitions; partition++) {
        final long initialFileLength = mergedFileOutputStream.getByteCount();
        // Shield the underlying output stream from close() and flush() calls, so that we can close
        // the higher level streams to make sure all data is really flushed and internal state is
        // cleaned.
        // 封装 mergedFileOutputStream 为 CloseAndFlushShieldOutputStream 该类复写了 close 和 flush 方法
        OutputStream partitionOutput = new CloseAndFlushShieldOutputStream(
          new TimeTrackingOutputStream(writeMetrics, mergedFileOutputStream));
        // 如果启用了加密, 则提供加密输出
        partitionOutput = blockManager.serializerManager().wrapForEncryption(partitionOutput);
        if (compressionCodec != null) {
          // 如果启用了压缩, 则包装压缩类
          partitionOutput = compressionCodec.compressedOutputStream(partitionOutput);
        }
        for (int i = 0; i < spills.length; i++) {
          // 获取当前 partition 在该 spill[i] 文件中的偏移量
          final long partitionLengthInSpill = spills[i].partitionLengths[partition];
          if (partitionLengthInSpill > 0) {
            // 读取 spill 中当前 partition 的偏移量
            InputStream partitionInputStream = new LimitedInputStream(spillInputStreams[i],
              partitionLengthInSpill, false);
            try {
              // 加密
              partitionInputStream = blockManager.serializerManager().wrapForEncryption(
                partitionInputStream);
              if (compressionCodec != null) {
                // 压缩
                partitionInputStream = compressionCodec.compressedInputStream(partitionInputStream);
              }
              // copy 至 outputStream
              ByteStreams.copy(partitionInputStream, partitionOutput);
            } finally {
              partitionInputStream.close();
            }
          }
        }
        partitionOutput.flush();
        partitionOutput.close();
        partitionLengths[partition] = (mergedFileOutputStream.getByteCount() - initialFileLength);
      }
      threwException = false;
    } finally {
      // To avoid masking exceptions that caused us to prematurely enter the finally block, only
      // throw exceptions during cleanup if threwException == false.
      for (InputStream stream : spillInputStreams) {
        Closeables.close(stream, threwException);
      }
      Closeables.close(mergedFileOutputStream, threwException);
    }
    return partitionLengths;
  }

  /**
   * Merges spill files by using NIO's transferTo to concatenate spill partitions' bytes.
   * This is only safe when the IO compression codec and serializer support concatenation of
   * serialized streams.
   *
   * @return the partition lengths in the merged file.
   */
  private long[] mergeSpillsWithTransferTo(SpillInfo[] spills, File outputFile) throws IOException {
    assert (spills.length >= 2);
    // 分区数
    final int numPartitions = partitioner.numPartitions();
    // 初始化每个 partition 在 outputFile 文件中的偏移量的数组
    final long[] partitionLengths = new long[numPartitions];
    // 初始化每个 spill 文件的 channel 数组
    final FileChannel[] spillInputChannels = new FileChannel[spills.length];
    // 初始化每个 spill 文件的 offset 计数器数组
    final long[] spillInputChannelPositions = new long[spills.length];
    FileChannel mergedFileOutputChannel = null;

    boolean threwException = true;
    try {
      // 初始化每个 spill 文件的 channel 对象
      for (int i = 0; i < spills.length; i++) {
        spillInputChannels[i] = new FileInputStream(spills[i].file).getChannel();
      }
      // This file needs to opened in append mode in order to work around a Linux kernel bug that
      // affects transferTo; see SPARK-3948 for more details.
      // 初始化输出文件 channel 对象
      mergedFileOutputChannel = new FileOutputStream(outputFile, true).getChannel();
      // 输出文件的长度计数器
      long bytesWrittenToMergedFile = 0;
      for (int partition = 0; partition < numPartitions; partition++) {
        for (int i = 0; i < spills.length; i++) {
          // 获取该 partition 在该 spill 文件中的长度
          final long partitionLengthInSpill = spills[i].partitionLengths[partition];
          final FileChannel spillInputChannel = spillInputChannels[i];
          final long writeStartTime = System.nanoTime();
          // 使用 nio transferTo 拷贝 spill 文件中指定长度的数据至输出文件 channel
          Utils.copyFileStreamNIO(
            spillInputChannel,
            mergedFileOutputChannel,
            spillInputChannelPositions[i],
            partitionLengthInSpill);
          // spill 文件长度计数器累加
          spillInputChannelPositions[i] += partitionLengthInSpill;
          writeMetrics.incWriteTime(System.nanoTime() - writeStartTime);
          // 输出文件的长度计数器累加
          bytesWrittenToMergedFile += partitionLengthInSpill;
          // 记录输出文件中, 每个 partition 的长度累加
          partitionLengths[partition] += partitionLengthInSpill;
        }
      }
      // Check the position after transferTo loop to see if it is in the right position and raise an
      // exception if it is incorrect. The position will not be increased to the expected length
      // after calling transferTo in kernel version 2.6.32. This issue is described at
      // https://bugs.openjdk.java.net/browse/JDK-7052359 and SPARK-3948.
      // 校验输出的文件 position 是否与计数器的一致
      if (mergedFileOutputChannel.position() != bytesWrittenToMergedFile) {
        throw new IOException(
          "Current position " + mergedFileOutputChannel.position() + " does not equal expected " +
            "position " + bytesWrittenToMergedFile + " after transferTo. Please check your kernel" +
            " version to see if it is 2.6.32, as there is a kernel bug which will lead to " +
            "unexpected behavior when using transferTo. You can set spark.file.transferTo=false " +
            "to disable this NIO feature."
        );
      }
      threwException = false;
    } finally {
      // To avoid masking exceptions that caused us to prematurely enter the finally block, only
      // throw exceptions during cleanup if threwException == false.
      // 校验每个 spill 文件position 是否与每个 spill 文件输出的计数器长度一致, 并关闭所有 spill channel
      for (int i = 0; i < spills.length; i++) {
        assert(spillInputChannelPositions[i] == spills[i].file.length());
        Closeables.close(spillInputChannels[i], threwException);
      }
      Closeables.close(mergedFileOutputChannel, threwException);
    }
    return partitionLengths;
  }

  @Override
  public Option<MapStatus> stop(boolean success) {
    try {
      taskContext.taskMetrics().incPeakExecutionMemory(getPeakMemoryUsedBytes());

      if (stopping) {
        return Option.apply(null);
      } else {
        stopping = true;
        if (success) {
          if (mapStatus == null) {
            throw new IllegalStateException("Cannot call stop(true) without having called write()");
          }
          return Option.apply(mapStatus);
        } else {
          return Option.apply(null);
        }
      }
    } finally {
      if (sorter != null) {
        // If sorter is non-null, then this implies that we called stop() in response to an error,
        // so we need to clean up memory and spill files created by the sorter
        sorter.cleanupResources();
      }
    }
  }
}
