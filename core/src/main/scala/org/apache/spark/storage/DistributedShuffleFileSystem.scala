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

import java.io.{File, FileNotFoundException, IOException}
import java.net.URI
import java.security.SecureRandom
import java.util
import java.util.UUID

import org.apache.commons.io.input.BoundedInputStream
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY
import org.apache.hadoop.fs.Options.Rename
import org.apache.hadoop.fs._
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.hadoop.io.IOUtils
import org.apache.spark.executor.ShuffleWriteMetrics
import org.apache.spark.internal.Logging
import org.apache.spark.io.SeekableFileInputStream
import org.apache.spark.network.buffer._
import org.apache.spark.network.shuffle._
import org.apache.spark.network.util.TransportConf
import org.apache.spark.serializer.SerializerInstance
import org.apache.spark.util.Utils
import org.apache.spark.{SparkConf, SparkEnv}

import scala.util.Try

/**
  * Distributed file system which can be used with any HDFS compliant file system
  */
private[spark] class DistributedShuffleFileSystem(
    sparkConf : SparkConf,
    hadoopConf: Configuration)
  extends ShuffleFileSystem with Logging {

  private[spark] val subDirsPerLocalDir = sparkConf.get("spark.diskStore.subDirectories", "64").toInt

  private lazy val blockManager: BlockManager = SparkEnv.get.blockManager
  private lazy val shuffleClient: ShuffleClient = getDFSShuffleClient
  private val hadoopFC: FileContext = FileContext.getFileContext(hadoopConf)
  private lazy val tmpDir = System.getProperty("java.io.tmpdir")

  //Application specific directory under a node
  private lazy val appDirOnDFS: URI = getNodeAppDirOnDFS

  override def open(inputPath: URI): FSDataInputStream = {
    hadoopFC.open(new Path(inputPath))
  }

  override def open(inputPath: URI, bufferSize: Int): FSDataInputStream = {
    hadoopFC.open(new Path(inputPath), bufferSize)
  }

  override def create(inputUri: URI, append: Boolean): FSDataOutputStream = {
    val createFlags: util.EnumSet[CreateFlag] =
      if (append) {
        util.EnumSet.of(CreateFlag.CREATE, CreateFlag.APPEND)
      } else {
        util.EnumSet.of(CreateFlag.CREATE, CreateFlag.OVERWRITE)
      }
    hadoopFC.create(new Path(inputUri), createFlags, Options.CreateOpts.createParent)
  }

  override def create(inputUri: URI, append: Boolean, bufferSize: Int): FSDataOutputStream = {
    val createFlags: util.EnumSet[CreateFlag] =
      if (append) {
        util.EnumSet.of(CreateFlag.CREATE, CreateFlag.APPEND)
      } else {
        util.EnumSet.of(CreateFlag.CREATE, CreateFlag.OVERWRITE)
      }
    hadoopFC.create(
      new Path(inputUri),
      createFlags,
      Options.CreateOpts.createParent,
      Options.CreateOpts.bufferSize(bufferSize))
  }

  override def delete(inputUri: URI): Boolean = {
    hadoopFC.delete(new Path(inputUri), false)
  }

  override def deleteRecursively(file: URI): Unit = {
    hadoopFC.delete(new Path(file), true)
  }

  override def exists(inputUri: URI): Boolean = {
    hadoopFC.util.exists(new Path(inputUri))
  }

  override def rename(source: URI, dest: URI): Boolean = {
    Try {
      hadoopFC.rename(new Path(source), new Path(dest), Rename.OVERWRITE)
    }.isSuccess
  }

  override def close: Unit = {
    if(shuffleClient != null)
      shuffleClient.close()
  }

  override def mkdirs(path: URI): Boolean = {
    Try {
      hadoopFC.mkdir(new Path(path), FsPermission.getDirDefault, true)
    }.isSuccess
  }

  override def getFileSize(inputUri: URI): Long = {
    try {
      hadoopFC.getFileStatus(new Path(inputUri)).getLen
    } catch {
      case _: FileNotFoundException => 0l
      case _: IOException => 0l
      case e: Throwable => throw e
    }
  }

  override def getFileCount(inputUri: URI): Long = {
    val locatedStatus = hadoopFC.listLocatedStatus(new Path(inputUri))
    var count = 0
    while(locatedStatus.hasNext) {
      count += 1
      locatedStatus.next()
    }
    count
  }

  override def getBoundedStream(inputStream: FSDataInputStream, size: Long): FSDataInputStream = {
    new FSDataInputStream(new SeekableFileInputStream(new BoundedInputStream(inputStream, size)))
  }

  override def truncate(file: URI, length: Long): Unit = {
    //TODO Once the dependency is migrated to Hadoop 2.7 use the truncation api call
    if(length == 0) {
      //Overwrite
      create(file, append = false)
    } else {
      val truncatedFile = URI.create(file.toString + ".truncated")
      copyFile(file, truncatedFile, length)
      val truncatedFilePath = new Path(truncatedFile)
      hadoopFC.rename(truncatedFilePath, new Path(file), Rename.OVERWRITE)
      hadoopFC.delete(truncatedFilePath, true)
    }
  }

  override def getTempFilePath(fileName: String): URI = {
    URI.create(appDirOnDFS + "/" + fileName)
  }

  override def getDiskWriter(
      blockId: BlockId,
      file: URI,
      serializer: SerializerInstance,
      bufferSize: Int,
      writeMetrics: ShuffleWriteMetrics): DiskBlockObjectWriter = {
    blockManager.getDiskWriter(blockId, new File(file), serializer, bufferSize, writeMetrics)
  }

  override def createManagedBuffer(
      transportConf: TransportConf,
      file: URI,
      offset: Long,
      length: Long): ManagedBuffer = {
    new DFSManagedBuffer(hadoopConf, new Path(file), length, offset)
  }

  override def getShuffleClient: ShuffleClient = {
    shuffleClient
  }

  override def tempFileWith(file: URI): URI = {
    URI.create(getAbsolutePath(file) + "." + UUID.randomUUID())
  }

  override def getAbsolutePath(file: URI): URI = {
    val dfsPath = new Path(file)
    if(dfsPath.isUriPathAbsolute) {
      file
    } else {
      dfsPath.toUri
    }
  }

  /** Produces a unique block id and File suitable for storing local intermediate results. */
  override def createTempLocalBlock(): (TempLocalBlockId, URI) = {
    var blockId: TempLocalBlockId = TempLocalBlockId(UUID.randomUUID())
    val uri: URI = getTempFilePath(blockId.name)
    while (exists(uri)) {
      blockId = TempLocalBlockId(UUID.randomUUID())
    }
    (blockId, uri)
  }

  /** Produces a unique block id and File suitable for storing shuffled intermediate results. */
  override def createTempShuffleBlock(): (TempShuffleBlockId, URI) = {
    var blockId: TempShuffleBlockId = TempShuffleBlockId(UUID.randomUUID())
    val uri: URI = getTempFilePath(blockId.name)
    while (exists(uri)) {
      blockId = TempShuffleBlockId(UUID.randomUUID())
    }
    (blockId, uri)
  }

  /** Create a temporary directory with a given name. java.io.File.toURI adds a slash to the end so this
    * method should do the same as well
    * @param root
    * @param namePrefix
    * @return
    */
  override def createTempDir(root: String, namePrefix: String): URI = {
    URI.create(Utils.createTempDFSDir(root, namePrefix, this) + File.separator)
  }

  override def createTempFile(prefix: String, suffix: String, directory: URI): URI = {
    if (prefix.length < 3) {
      throw new IllegalArgumentException("Prefix string too short")
    }
    val secureRandom = new SecureRandom()
    val randomNum = secureRandom.nextLong()
    val fileNum = if (randomNum == Long.MinValue) 0 else Math.abs(randomNum)
    val resolvedSuffix = Option(suffix).getOrElse(".tmp")
    val fileName = prefix + fileNum.toString + resolvedSuffix
    val resolvedTmpdir = Option(directory).getOrElse(tmpDir)
    val tmpFilePath = getAbsolutePath(URI.create(resolvedTmpdir + "/" + fileName))
    create(tmpFilePath, append = false)
    tmpFilePath
  }

  private def getLocalDirOnDFS: String = {
    hadoopConf.get(FS_DEFAULT_NAME_KEY) + sparkConf.get("spark.local.dir", "/tmp")
  }

  private def getNodeAppDirOnDFS: URI = {
    val appDir = getLocalDirOnDFS + s"/${Utils.localHostName()}/spark/shuffle/${sparkConf.getAppId}"
    URI.create(appDir)
  }

  private def getDFSShuffleClient: ShuffleClient = {
    val maxThreads = sparkConf.getInt("spark.shuffle.distributed.clientThreads", 1)
    val shuffleClient = new DFSShuffleClient(sparkConf.getAppId, getLocalDirOnDFS, hadoopConf, maxThreads)
    shuffleClient
  }

  /**
    * Copies the contents of source file to destination file up to the specified
    * length.
    */
  private def copyFile(srcFile: URI, dstFile: URI, length: Long): Unit = {
    val inStream = getBoundedStream(open(srcFile), length)
    val outStream = create(dstFile, append = false)
    IOUtils.copyBytes(inStream, outStream, hadoopConf)
  }
}
