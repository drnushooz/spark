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

package org.apache.spark.storage

import java.io.File
import java.net.URI

import org.apache.spark.{SparkConf, SparkEnv, SparkFunSuite}
import org.apache.spark.network.netty.SparkTransportConf
import org.apache.spark.network.shuffle.ShuffleClient
import org.mockito.Answers.RETURNS_SMART_NULLS
import org.mockito.{Mock, MockitoAnnotations}
import org.mockito.Matchers._
import org.mockito.Mockito._
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.scalatest.BeforeAndAfterEach

private trait ShuffleFileSystemTestBase extends SparkFunSuite with BeforeAndAfterEach {

  @Mock(answer = RETURNS_SMART_NULLS) protected var sparkEnv: SparkEnv = _
  @Mock(answer = RETURNS_SMART_NULLS) protected var blockManager: BlockManager = _
  @Mock(answer = RETURNS_SMART_NULLS) protected var diskBlockManager: DiskBlockManager = _
  @Mock(answer = RETURNS_SMART_NULLS) protected var shuffleFileSystem: ShuffleFileSystem = _

  private val sparkConf: SparkConf = new SparkConf(loadDefaults = false)
    .set("spark.app.id", "ShuffleFileSystemTestBase")

  private var tempDir: URI = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    MockitoAnnotations.initMocks(this)

    when(sparkEnv.blockManager).thenReturn(blockManager)
    when(sparkEnv.conf).thenReturn(sparkConf)
    SparkEnv.set(sparkEnv)

    when(blockManager.diskBlockManager).thenReturn(diskBlockManager)
    when(diskBlockManager.getFile(any[String])).thenAnswer(new Answer[File] {
      override def answer(invocation: InvocationOnMock): File = {
        new File(URI.create(tempDir + invocation.getArguments.head.asInstanceOf[String]))
      }
    })
  }

  override def beforeEach(): Unit = {
    super.beforeEach()
    tempDir = shuffleFileSystem.createTempDir()
  }

  override def afterEach(): Unit = {
    shuffleFileSystem.deleteRecursively(tempDir)
    super.afterEach
  }

  protected override def afterAll(): Unit = {
    if(shuffleFileSystem != null)
      shuffleFileSystem.close
    super.afterAll()
  }

  test("open and create") {
    val sampleOutput = URI.create(tempDir + "someoutput")
    val outputWriter = shuffleFileSystem.create(sampleOutput, append = false)
    outputWriter.writeInt(10)
    outputWriter.close()
    val outputReader = shuffleFileSystem.open(sampleOutput)
    val readInt = outputReader.readInt()
    outputReader.close()
    assert(readInt == 10)
  }

  test("exists and delete") {
    val sampleOutputPath = URI.create(tempDir + "someoutput")
    createSampleOutput(sampleOutputPath)
    assert(shuffleFileSystem.delete(sampleOutputPath) && !shuffleFileSystem.exists(sampleOutputPath))
    createSampleOutput(sampleOutputPath)
    shuffleFileSystem.deleteRecursively(sampleOutputPath)
    assert(!shuffleFileSystem.exists(sampleOutputPath))
  }

  test("rename") {
    val sampleOutputPath = URI.create(tempDir + "someoutput")
    val newFilePath = URI.create(tempDir + "newfile")
    createSampleOutput(sampleOutputPath)
    shuffleFileSystem.rename(sampleOutputPath, newFilePath)
    assert(!shuffleFileSystem.exists(sampleOutputPath) && shuffleFileSystem.exists(newFilePath))
  }

  test("mkdirs") {
    val testOutputDir = URI.create(tempDir + "subtestdir")
    assert(shuffleFileSystem.mkdirs(testOutputDir) && shuffleFileSystem.exists(testOutputDir))
  }

  test("getFileSize") {
    val sampleOutputPath = URI.create(tempDir + "someoutput")
    createSampleOutput(sampleOutputPath)
    assert(shuffleFileSystem.getFileSize(sampleOutputPath) == 4)
  }

  test("getFileCount") {
    createSampleOutput(URI.create(tempDir + "someoutput1"))
    createSampleOutput(URI.create(tempDir + "someoutput2"))
    createSampleOutput(URI.create(tempDir + "someoutput3"))
    createSampleOutput(URI.create(tempDir + "someoutput4"))
    assert(shuffleFileSystem.getFileCount(tempDir) == 4)
  }

  test("getBoundedStream") {
    val sampleOutputPath = URI.create(tempDir + "someoutput")
    createSampleOutput(sampleOutputPath, 20)
    val inputStream = shuffleFileSystem.getBoundedStream(shuffleFileSystem.open(sampleOutputPath), 20)
    inputStream.skip(20)
    val readValue = inputStream.read()
    inputStream.close()
    assert(readValue == -1)
  }

  test("truncate") {
    val sampleOutputPath = URI.create(tempDir + "someoutput")
    createSampleOutput(sampleOutputPath, 25)
    shuffleFileSystem.truncate(sampleOutputPath, 20)
    assert(shuffleFileSystem.getFileSize(sampleOutputPath) == 20)
  }

  test("getShuffleClient") {
    assert(shuffleFileSystem.getShuffleClient.isInstanceOf[ShuffleClient])
  }

  test("createManagedBuffer") {
    val transConf = SparkTransportConf.fromSparkConf(sparkConf, "shuffle")
    val sampleOutputPath = URI.create(tempDir + "someoutput")
    createSampleOutput(sampleOutputPath, 5)
    val buf = shuffleFileSystem.createManagedBuffer(transConf, sampleOutputPath, 0, 20)
    val bufSize = buf.size()
    buf.release()
    assert(bufSize == 20)
  }

  private def createSampleOutput(outputFilePath: URI, numsToWrite: Int = 1): Unit = {
    val outputWriter = shuffleFileSystem.create(outputFilePath, append = false)
    (10 to (10 * numsToWrite) by 10).foreach(outputWriter.writeInt)
    outputWriter.close()
  }
}
