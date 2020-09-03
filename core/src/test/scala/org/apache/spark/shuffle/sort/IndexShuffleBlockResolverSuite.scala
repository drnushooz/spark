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

import org.mockito.{Mock, MockitoAnnotations}
import org.mockito.Answers.RETURNS_SMART_NULLS
import org.mockito.Matchers._
import org.mockito.Mockito._
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.scalatest.BeforeAndAfterEach
import org.apache.spark.{SparkConf, SparkEnv, SparkFunSuite}
import org.apache.spark.shuffle.IndexShuffleBlockResolver
import org.apache.spark.storage._
import org.apache.spark.util.Utils
import org.apache.spark.storage.{BlockManager, DiskBlockManager}
import org.mockito.Answers.RETURNS_SMART_NULLS
import org.mockito.{Mock, MockitoAnnotations}
import org.mockito.invocation.InvocationOnMock


class IndexShuffleBlockResolverSuite extends SparkFunSuite with BeforeAndAfterEach {

  @Mock(answer = RETURNS_SMART_NULLS) private var sparkEnv: SparkEnv = _
  @Mock(answer = RETURNS_SMART_NULLS) private var blockManager: BlockManager = _
  @Mock(answer = RETURNS_SMART_NULLS) private var diskBlockManager: DiskBlockManager = _

  private val conf: SparkConf = new SparkConf(loadDefaults = false)

  private var tempDir: URI = _
  private val shuffleFileSystem: ShuffleFileSystem = new LocalShuffleFileSystem

  override def beforeEach(): Unit = {
    super.beforeEach()
    MockitoAnnotations.initMocks(this)

    when(sparkEnv.blockManager).thenReturn(blockManager)
    when(sparkEnv.conf).thenReturn(conf)
    SparkEnv.set(sparkEnv)

    when(blockManager.diskBlockManager).thenReturn(diskBlockManager)

    tempDir = shuffleFileSystem.createTempDir()

    when(diskBlockManager.getFile(any[String])).thenAnswer(new Answer[File] {
      override def answer(invocation: InvocationOnMock): File = {
        new File(URI.create(tempDir + invocation.getArguments.head.asInstanceOf[String]))
      }
    })
  }

  override def afterEach(): Unit = {
    try {
      shuffleFileSystem.deleteRecursively(tempDir)
    } finally {
      super.afterEach()
    }
  }

  test("commit shuffle files multiple times") {
    val shuffleId = 1
    val mapId = 2
    val idxName = s"shuffle_${shuffleId}_${mapId}_0.index"
    val resolver = new IndexShuffleBlockResolver(conf, shuffleFileSystem)
    val lengths = Array[Long](10, 0, 20)
    val dataTmp = shuffleFileSystem.createTempFile("shuffle", null, tempDir)
    val out = shuffleFileSystem.create(dataTmp, append = false)
    Utils.tryWithSafeFinally {
      out.write(new Array[Byte](30))
    } {
      out.close()
    }
    resolver.writeIndexFileAndCommit(shuffleId, mapId, lengths, dataTmp)

    val indexFile = URI.create(shuffleFileSystem.getAbsolutePath(tempDir) + idxName)
    val dataFile = resolver.getDataFile(shuffleId, mapId)

    assert(shuffleFileSystem.exists(indexFile))
    assert(shuffleFileSystem.getFileSize(indexFile) === (lengths.length + 1) * 8)
    assert(shuffleFileSystem.exists(dataFile))
    assert(shuffleFileSystem.getFileSize(dataFile) === 30)
    assert(!shuffleFileSystem.exists(dataTmp))

    val lengths2 = new Array[Long](3)
    val dataTmp2 = shuffleFileSystem.createTempFile("shuffle", null, tempDir)
    val out2 = shuffleFileSystem.create(dataTmp2, append = false)
    Utils.tryWithSafeFinally {
      out2.write(Array[Byte](1))
      out2.write(new Array[Byte](29))
    } {
      out2.close()
    }
    resolver.writeIndexFileAndCommit(shuffleId, mapId, lengths2, dataTmp2)

    assert(shuffleFileSystem.getFileSize(indexFile) === (lengths.length + 1) * 8)
    assert(lengths2.toSeq === lengths.toSeq)
    assert(shuffleFileSystem.exists(dataFile))
    assert(shuffleFileSystem.getFileSize(dataFile) === 30)
    assert(!shuffleFileSystem.exists(dataTmp2))

    // The dataFile should be the previous one
    val firstByte = new Array[Byte](1)
    val dataIn = shuffleFileSystem.open(dataFile)
    Utils.tryWithSafeFinally {
      dataIn.read(firstByte)
    } {
      dataIn.close()
    }
    assert(firstByte(0) === 0)

    // The index file should not change
    val indexIn = shuffleFileSystem.open(indexFile)
    Utils.tryWithSafeFinally {
      indexIn.readLong() // the first offset is always 0
      assert(indexIn.readLong() === 10, "The index file should not change")
    } {
      indexIn.close()
    }

    // remove data file
    shuffleFileSystem.delete(dataFile)

    val lengths3 = Array[Long](7, 10, 15, 3)
    val dataTmp3 = shuffleFileSystem.createTempFile("shuffle", null, tempDir)
    val out3 = shuffleFileSystem.create(dataTmp3, append = false)
    Utils.tryWithSafeFinally {
      out3.write(Array[Byte](2))
      out3.write(new Array[Byte](34))
    } {
      out3.close()
    }
    resolver.writeIndexFileAndCommit(shuffleId, mapId, lengths3, dataTmp3)
    assert(shuffleFileSystem.getFileSize(indexFile) === (lengths3.length + 1) * 8)
    assert(lengths3.toSeq != lengths.toSeq)
    assert(shuffleFileSystem.exists(dataFile))
    assert(shuffleFileSystem.getFileSize(dataFile) === 35)
    assert(!shuffleFileSystem.exists(dataTmp3))

    // The dataFile should be the new one, since we deleted the dataFile from the first attempt
    val dataIn2 = shuffleFileSystem.open(dataFile)
    Utils.tryWithSafeFinally {
      dataIn2.read(firstByte)
    } {
      dataIn2.close()
    }
    assert(firstByte(0) === 2)

    // The index file should be updated, since we deleted the dataFile from the first attempt
    val indexIn2 = shuffleFileSystem.open(indexFile)
    Utils.tryWithSafeFinally {
      indexIn2.readLong() // the first offset is always 0
      assert(indexIn2.readLong() === 7, "The index file should be updated")
    } {
      indexIn2.close()
    }
  }
}
