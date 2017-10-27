package org.brett.noworlater

import java.io.{IOException, OutputStream}
import java.nio.ByteBuffer

/**
  * Output stream backed by a byte buffer
  */
class ByteBufferOutputStream(val buffer: ByteBuffer) extends OutputStream {
  override def write(b: Array[Byte], off: Int, len: Int): Unit = {
    if (buffer.remaining() < len) throw new ByteBufferOverflowException
    buffer.put(b,off,len)
  }

  override def write(b: Int): Unit = {
    if (!buffer.hasRemaining) throw new ByteBufferOverflowException
    buffer.put(b.toByte)
  }
}

class ByteBufferOverflowException extends IOException
