package org.apache.spark.storage

import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.CommonConfigurationKeysPublic.{FS_DEFAULT_NAME_DEFAULT, FS_DEFAULT_NAME_KEY}
import org.apache.hadoop.fs.{FSDataInputStream, FSDataOutputStream}
import org.apache.spark.{SparkConf, SparkEnv}
import org.apache.spark.executor.ShuffleWriteMetrics
import org.apache.spark.network.buffer.ManagedBuffer
import org.apache.spark.network.shuffle.ShuffleClient
import org.apache.spark.network.util.TransportConf
import org.apache.spark.serializer.SerializerInstance
import org.apache.spark.internal.Logging

private[spark] trait ShuffleFileSystem {

  def open(inputPath: URI): FSDataInputStream

  def open(inputPath: URI, bufferSize: Int): FSDataInputStream

  def create(inputUri: URI, append: Boolean): FSDataOutputStream

  def create(inputUri: URI, append: Boolean, bufferSize: Int): FSDataOutputStream

  def delete(inputUri: URI): Boolean

  def deleteRecursively(file: URI): Unit

  def exists(inputUri: URI): Boolean

  def rename(source: URI, dest: URI): Boolean

  def close: Unit

  def mkdirs(path: URI): Boolean

  def getFileSize(inputUri: URI): Long

  def getFileCount(inputUri: URI): Long

  def getBoundedStream(inputStream: FSDataInputStream, size: Long): FSDataInputStream

  def truncate(file: URI, length: Long): Unit

  def getTempFilePath(fileName: String): URI

  def getDiskWriter(
      blockId: BlockId,
      file: URI,
      serializer: SerializerInstance,
      bufferSize: Int,
      writeMetrics: ShuffleWriteMetrics): DiskBlockObjectWriter

  def createManagedBuffer(transportConf: TransportConf, file: URI, offset: Long, length: Long): ManagedBuffer

  def getShuffleClient: ShuffleClient

  def tempFileWith(file: URI): URI

  def getAbsolutePath(file: URI): URI

  def createTempLocalBlock(): (TempLocalBlockId, URI)

  def createTempShuffleBlock(): (TempShuffleBlockId, URI)

  //Following methods are used in tests for BypassMergeSortShuffleWriterSuite
  def createTempDir(root: String = System.getProperty("java.io.tmpdir"), namePrefix: String = "spark"): URI

  def createTempFile(prefix: String, suffix: String, directory: URI): URI
}

private[spark] object ShuffleFileSystem extends Logging {

  private[this] val sparkConf: SparkConf = Option(SparkEnv.get).map(_.conf).getOrElse(new SparkConf(loadDefaults = false))

  //This version of apply is mainly used for testing.
  def apply(): ShuffleFileSystem = {
    apply(sparkConf)
  }

  def apply(conf: SparkConf): ShuffleFileSystem = {
    val shuffleFSSetting = conf.get("spark.shuffle.filesystem", "file:///")
    shuffleFSSetting match {
      case FS_DEFAULT_NAME_DEFAULT =>
        new LocalShuffleFileSystem
      case _ =>
        val hadoopConf = new Configuration()
        hadoopConf.set(FS_DEFAULT_NAME_KEY, shuffleFSSetting)
        val shuffleFS = new DistributedShuffleFileSystem(conf, hadoopConf)
        logInfo(s"Distributed shuffle enabled with file system $shuffleFSSetting")
        shuffleFS
    }
  }
}