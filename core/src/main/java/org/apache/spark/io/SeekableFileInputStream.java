/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.io;

import org.apache.commons.io.input.CountingInputStream;
import org.apache.hadoop.fs.PositionedReadable;
import org.apache.hadoop.fs.Seekable;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * A wrapper class which implements {@link org.apache.hadoop.fs.PositionedReadable} and {@link org.apache.hadoop.fs.Seekable}
 * and keeps track of available bytes and allows raw data reads
 */
public final class SeekableFileInputStream extends CountingInputStream implements Seekable, PositionedReadable {

  private final DataInputStream wrappedDataInputStream;

  public SeekableFileInputStream(Path inputPath) throws IOException {
    super(Files.newInputStream(inputPath));
    wrappedDataInputStream = new DataInputStream(this);
  }

  public SeekableFileInputStream(Path inputPath, int bufferSize) throws IOException {
    super(Files.newInputStream(inputPath));
    wrappedDataInputStream = new DataInputStream(new BufferedInputStream(this, bufferSize));
  }

  public SeekableFileInputStream(InputStream inputStream) {
    super(inputStream);
    wrappedDataInputStream = new DataInputStream(this);
  }

  @Override
  public int read(long position, byte[] buffer, int offset, int length) throws IOException {
    int availableLength = wrappedDataInputStream.available();
    if (position >= availableLength)
      throw new IllegalArgumentException(
        String.format(
          "Read from position %d for stream of size %d",
          position,
          availableLength)
      );
    if (position + availableLength > buffer.length)
      throw new IllegalArgumentException(
        String.format(
          "Provided buffer size %d is smaller than bytes left in the stream %d",
          buffer.length,
          availableLength)
      );
    if (length > buffer.length)
      throw new IllegalArgumentException(
        String.format(
          "Tried to read %d when buffer size is %d ",
          length,
          buffer.length
        )
      );
    wrappedDataInputStream.skip(position);
    return wrappedDataInputStream.read(buffer, offset, length);
  }

  @Override
  public void readFully(long position, byte[] buffer, int offset, int length) throws IOException {
    int availableLength = wrappedDataInputStream.available();
    if (position >= availableLength)
      throw new IllegalArgumentException(
        String.format(
          "Read from position %d for stream of size %d",
          position,
          availableLength)
      );
    wrappedDataInputStream.skip(position);
    wrappedDataInputStream.readFully(buffer, offset, length);
  }

  @Override
  public void readFully(long position, byte[] buffer) throws IOException {
    readFully(position, buffer, 0, buffer.length);
  }

  @Override
  public void seek(long pos) throws IOException {
    wrappedDataInputStream.skip(pos);
  }

  @Override
  public long getPos() throws IOException {
    return getByteCount();
  }

  @Override
  public boolean seekToNewSource(long targetPos) throws IOException {
    return false;
  }
}
