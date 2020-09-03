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

package org.apache.spark.shuffle.sort

import java.io.File
import java.net.URI
import java.util.UUID

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import org.mockito.{Mock, MockitoAnnotations}
import org.mockito.Answers.RETURNS_SMART_NULLS
import org.mockito.Matchers._
import org.mockito.Mockito._
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.scalatest.BeforeAndAfterEach

import org.apache.spark._
import org.apache.spark.executor.{ShuffleWriteMetrics, TaskMetrics}
import org.apache.spark.serializer.{JavaSerializer, SerializerInstance, SerializerManager}
import org.apache.spark.shuffle.IndexShuffleBlockResolver
import org.apache.spark.storage._

class BypassMergeSortShuffleWriterSuite extends SparkFunSuite with BeforeAndAfterEach {

  @Mock(answer = RETURNS_SMART_NULLS) private var sparkEnv: SparkEnv = _
  @Mock(answer = RETURNS_SMART_NULLS) private var blockManager: BlockManager = _
  @Mock(answer = RETURNS_SMART_NULLS) private var diskBlockManager: DiskBlockManager = _
  @Mock(answer = RETURNS_SMART_NULLS) private var taskContext: TaskContext = _
  @Mock(answer = RETURNS_SMART_NULLS) private var blockResolver: IndexShuffleBlockResolver = _
  @Mock(answer = RETURNS_SMART_NULLS) private var dependency: ShuffleDependency[Int, Int, Int] = _

  private var taskMetrics: TaskMetrics = _
  private var tempDir: URI = _
  private var outputFile: URI = _
  private val conf: SparkConf = new SparkConf(loadDefaults = false)
  private val temporaryFilesCreated: mutable.Buffer[File] = new ArrayBuffer[File]()
  private val blockIdToFileMap: mutable.Map[BlockId, File] = new mutable.HashMap[BlockId, File]
  private var shuffleHandle: BypassMergeSortShuffleHandle[Int, Int] = _
  private val shuffleFileSystem: ShuffleFileSystem = new LocalShuffleFileSystem

  override def beforeEach(): Unit = {
    super.beforeEach()
    MockitoAnnotations.initMocks(this)

    taskMetrics = new TaskMetrics
    shuffleHandle = new BypassMergeSortShuffleHandle[Int, Int](
      shuffleId = 0,
      numMaps = 2,
      dependency = dependency
    )

    when(sparkEnv.blockManager).thenReturn(blockManager)
    when(sparkEnv.conf).thenReturn(conf)
    when(blockManager.diskBlockManager).thenReturn(diskBlockManager)
    SparkEnv.set(sparkEnv)

    when(dependency.partitioner).thenReturn(new HashPartitioner(7))
    when(dependency.serializer).thenReturn(new JavaSerializer(conf))
    when(taskContext.taskMetrics()).thenReturn(taskMetrics)
    when(blockResolver.getDataFile(0, 0)).thenReturn(outputFile)
    when(blockManager.diskBlockManager).thenReturn(diskBlockManager)
    when(blockManager.getDiskWriter(
      any[BlockId],
      any[File],
      any[SerializerInstance],
      anyInt(),
      any[ShuffleWriteMetrics]
    )).thenAnswer(new Answer[DiskBlockObjectWriter] {
      override def answer(invocation: InvocationOnMock): DiskBlockObjectWriter = {
        val args = invocation.getArguments
        val manager = new SerializerManager(new JavaSerializer(conf), conf)
        new DiskBlockObjectWriter(
          args(1).asInstanceOf[File].toURI,
          manager,
          args(2).asInstanceOf[SerializerInstance],
          args(3).asInstanceOf[Int],
          syncWrites = false,
          args(4).asInstanceOf[ShuffleWriteMetrics],
          blockId = args(0).asInstanceOf[BlockId]
        )
      }
    })

    tempDir = shuffleFileSystem.createTempDir()
    outputFile = shuffleFileSystem.createTempFile("shuffle", null, tempDir)

    when(blockResolver.getDataFile(0, 0)).thenReturn(outputFile)
    when(diskBlockManager.createTempShuffleBlock()).thenAnswer(new Answer[(TempShuffleBlockId, File)] {
      override def answer(invocation: InvocationOnMock): (TempShuffleBlockId, File) = {
        val blockId = TempShuffleBlockId(UUID.randomUUID)
        val file = new File(new File(tempDir), blockId.name)
        blockIdToFileMap.put(blockId, file)
        temporaryFilesCreated += file
        (blockId, file)
      }
    })
    doAnswer( new Answer[Void] {
      override def answer(invocationOnMock: InvocationOnMock): Void = {
        val tmp = invocationOnMock.getArguments()(3).asInstanceOf[URI]
        if (tmp != null) {
          shuffleFileSystem.rename(tmp, outputFile)
        }
        null
      }
    }).when(blockResolver)
      .writeIndexFileAndCommit(anyInt, anyInt, any(classOf[Array[Long]]), any(classOf[URI]))

    when(diskBlockManager.getFile(any[String])).thenAnswer(new Answer[File] {
      override def answer(invocation: InvocationOnMock): File = {
        new File(URI.create(tempDir + invocation.getArguments.head.asInstanceOf[String]))
      }
    })
  }

  override def afterEach(): Unit = {
    try {
      shuffleFileSystem.deleteRecursively(tempDir)
      blockIdToFileMap.clear()
      temporaryFilesCreated.clear()
    } finally {
      super.afterEach()
    }
  }

  test("write empty iterator") {
    val writer = new BypassMergeSortShuffleWriter[Int, Int](
      blockManager,
      blockResolver,
      shuffleHandle,
      0, // MapId
      taskContext,
      conf,
      shuffleFileSystem
    )
    writer.write(Iterator.empty)
    writer.stop( /* success = */ true)
    assert(writer.getPartitionLengths.sum === 0)
    assert(shuffleFileSystem.exists(outputFile))
    assert(shuffleFileSystem.getFileSize(outputFile) === 0)
    assert(temporaryFilesCreated.isEmpty)
    val shuffleWriteMetrics = taskContext.taskMetrics().shuffleWriteMetrics
    assert(shuffleWriteMetrics.bytesWritten === 0)
    assert(shuffleWriteMetrics.recordsWritten === 0)
    assert(taskMetrics.diskBytesSpilled === 0)
    assert(taskMetrics.memoryBytesSpilled === 0)
  }

  test("write with some empty partitions") {
    def records: Iterator[(Int, Int)] =
      Iterator((1, 1), (5, 5)) ++ (0 until 100000).iterator.map(x => (2, 2))
    val writer = new BypassMergeSortShuffleWriter[Int, Int](
      blockManager,
      blockResolver,
      shuffleHandle,
      0, // MapId
      taskContext,
      conf,
      shuffleFileSystem
    )
    writer.write(records)
    writer.stop( /* success = */ true)
    assert(temporaryFilesCreated.nonEmpty)
    assert(writer.getPartitionLengths.sum === shuffleFileSystem.getFileSize(outputFile))
    assert(writer.getPartitionLengths.count(_ == 0L) === 4) // should be 4 zero length files
    assert(temporaryFilesCreated.count(_.exists()) === 0) // check that temporary files were deleted
    val shuffleWriteMetrics = taskContext.taskMetrics().shuffleWriteMetrics
    assert(shuffleWriteMetrics.bytesWritten === shuffleFileSystem.getFileSize(outputFile))
    assert(shuffleWriteMetrics.recordsWritten === records.length)
    assert(taskMetrics.diskBytesSpilled === 0)
    assert(taskMetrics.memoryBytesSpilled === 0)
  }

  test("only generate temp shuffle file for non-empty partition") {
    // Using exception to test whether only non-empty partition creates temp shuffle file,
    // because temp shuffle file will only be cleaned after calling stop(false) in the failure
    // case, so we could use it to validate the temp shuffle files.
    def records: Iterator[(Int, Int)] =
      Iterator((1, 1), (5, 5)) ++
        (0 until 100000).iterator.map { i =>
          if (i == 99990) {
            throw new SparkException("intentional failure")
          } else {
            (2, 2)
          }
        }

    val writer = new BypassMergeSortShuffleWriter[Int, Int](
      blockManager,
      blockResolver,
      shuffleHandle,
      0, // MapId
      taskContext,
      conf,
      shuffleFileSystem
    )

    intercept[SparkException] {
      writer.write(records)
    }

    assert(temporaryFilesCreated.nonEmpty)
    // Only 3 temp shuffle files will be created
    assert(temporaryFilesCreated.count(_.exists()) === 3)

    writer.stop( /* success = */ false)
    assert(temporaryFilesCreated.count(_.exists()) === 0) // check that temporary files were deleted
  }

  test("cleanup of intermediate files after errors") {
    val writer = new BypassMergeSortShuffleWriter[Int, Int](
      blockManager,
      blockResolver,
      shuffleHandle,
      0, // MapId
      taskContext,
      conf,
      shuffleFileSystem
    )
    intercept[SparkException] {
      writer.write((0 until 100000).iterator.map(i => {
        if (i == 99990) {
          throw new SparkException("Intentional failure")
        }
        (i, i)
      }))
    }
    assert(temporaryFilesCreated.nonEmpty)
    writer.stop( /* success = */ false)
    assert(temporaryFilesCreated.count(_.exists()) === 0)
  }

}
