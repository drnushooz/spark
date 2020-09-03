package org.apache.spark.storage

import java.io.{BufferedOutputStream, File, IOException}
import java.net.URI
import java.nio.channels.FileChannel
import java.nio.file
import java.nio.file.{Files, NoSuchFileException, Paths, StandardCopyOption, StandardOpenOption}

import org.apache.commons.io.input.BoundedInputStream
import org.apache.hadoop.fs.{FSDataInputStream, FSDataOutputStream}
import org.apache.spark.SparkEnv
import org.apache.spark.executor.ShuffleWriteMetrics
import org.apache.spark.io.SeekableFileInputStream
import org.apache.spark.network.buffer.{FileSegmentManagedBuffer, ManagedBuffer}
import org.apache.spark.network.shuffle.ShuffleClient
import org.apache.spark.network.util.TransportConf
import org.apache.spark.serializer.SerializerInstance
import org.apache.spark.util.Utils

import scala.util.Try

/**
  * Local file system implementation to be used in shuffle and spills
  */
private[spark] class LocalShuffleFileSystem extends ShuffleFileSystem {
  private lazy val blockManager = SparkEnv.get.blockManager
  private lazy val diskBlockManager = blockManager.diskBlockManager

  override def open(inputUri: URI): FSDataInputStream = {
    new FSDataInputStream(new SeekableFileInputStream(getLocalizedPath(inputUri)))
  }

  override def open(inputUri: URI, bufferSize: Int): FSDataInputStream = {
    new FSDataInputStream(new SeekableFileInputStream(getLocalizedPath(inputUri), bufferSize))
  }

  override def create(inputUri: URI, append: Boolean): FSDataOutputStream = {
    val outputStream =
      if (append)
        Files.newOutputStream(
          getLocalizedPath(inputUri),
          StandardOpenOption.WRITE,
          StandardOpenOption.CREATE,
          StandardOpenOption.APPEND
        )
      else
        Files.newOutputStream(getLocalizedPath(inputUri))
    new FSDataOutputStream(new BufferedOutputStream(outputStream), null)
  }

  override def create(inputUri: URI, append: Boolean, bufferSize: Int): FSDataOutputStream = {
    val outputStream =
      if (append)
        Files.newOutputStream(
          getLocalizedPath(inputUri),
          StandardOpenOption.WRITE,
          StandardOpenOption.CREATE,
          StandardOpenOption.APPEND
        )
      else
        Files.newOutputStream(getLocalizedPath(inputUri))
    new FSDataOutputStream(new BufferedOutputStream(outputStream, bufferSize), null)
  }

  override def delete(inputUri: URI): Boolean = {
    Files.deleteIfExists(getLocalizedPath(inputUri))
  }

  override def deleteRecursively(file: URI): Unit = {
    Utils.deleteRecursively(new File(file.getPath))
  }

  override def exists(inputUri: URI): Boolean = {
    Files.exists(getLocalizedPath(inputUri))
  }

  override def rename(source: URI, dest: URI): Boolean = {
    Try {
      Files.move(getLocalizedPath(source), getLocalizedPath(dest), StandardCopyOption.REPLACE_EXISTING)
    }.isSuccess
  }

  override def close: Unit = {
  }

  override def mkdirs(path: URI): Boolean = {
    Files.exists(Files.createDirectories(Paths.get(path)))
  }

  override def getFileSize(inputUri: URI): Long = {
    try {
      Files.size(getLocalizedPath(inputUri))
    } catch {
      case _: NoSuchFileException => 0l
      case _: IOException => 0l
      case e: Throwable => throw e
    }
  }

  override def getFileCount(inputUri: URI): Long = {
    Paths.get(inputUri).toAbsolutePath.toFile.listFiles.length
  }

  override def getBoundedStream(inputStream: FSDataInputStream, size: Long): FSDataInputStream = {
    new FSDataInputStream(new SeekableFileInputStream(new BoundedInputStream(inputStream, size)))
  }

  override def truncate(file: URI, size: Long): Unit = {
    val fileChannelTry = Try { FileChannel.open(getLocalizedPath(file), StandardOpenOption.WRITE) }
    fileChannelTry.map { fileChannel =>
      fileChannel.truncate(size)
      fileChannel.close()
    }
  }

  override def getTempFilePath(fileName: String): URI = {
    diskBlockManager.getFile(fileName).toURI
  }

  override def getDiskWriter(
      blockId: BlockId,
      file: URI,
      serializer: SerializerInstance,
      bufferSize: Int,
      writeMetrics: ShuffleWriteMetrics): DiskBlockObjectWriter = {
    blockManager.getDiskWriter(
      blockId,
      new File(getLocalizedPath(file).toString),
      serializer,
      bufferSize,
      writeMetrics)
  }

  override def createManagedBuffer(
      transportConf: TransportConf,
      file: URI,
      offset: Long,
      length: Long): ManagedBuffer = {
    new FileSegmentManagedBuffer(
      transportConf,
      new File(getLocalizedPath(file).toString),
      offset,
      length)
  }

  override def getShuffleClient: ShuffleClient = {
    blockManager.shuffleClient
  }

  override def tempFileWith(file: URI): URI = {
    Utils.tempFileWith(new File(file)).toURI
  }

  override def getAbsolutePath(file: URI): URI = {
    getLocalizedPath(file).toUri
  }

  override def createTempLocalBlock(): (TempLocalBlockId, URI) = {
    val (blockId, tmpFile) = diskBlockManager.createTempLocalBlock()
    (blockId, tmpFile.toURI)
  }

  override def createTempShuffleBlock(): (TempShuffleBlockId, URI) = {
    val (blockId, tmpFile) = diskBlockManager.createTempShuffleBlock()
    (blockId, tmpFile.toURI)
  }

  /**
    * Create a temporary directory inside the given parent directory. The directory will be
    * automatically deleted when the VM shuts down.
    * This method should be kept in sync with Utils#createTempDir
    */
  override def createTempDir(root: String, namePrefix: String): URI = {
    Utils.createTempDir(root, namePrefix).toURI
  }

  override def createTempFile(prefix: String, suffix: String, directory: URI): URI = {
    File.createTempFile(prefix, suffix, new File(directory)).toURI
  }

  private def getLocalizedPath(inputUri: URI): file.Path = {
    Paths.get(inputUri.getPath).toAbsolutePath
  }
}
