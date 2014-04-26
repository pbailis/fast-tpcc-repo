package edu.berkeley.velox.util

import java.io.{ByteArrayOutputStream, InputStream}

object JVMClassClosureUtil {
  def classToBytes(cls: Class[_]): Array[Byte] = {
    val className = cls.getName
    val shortName = className.replaceFirst("^.*\\.", "") + ".class"
    val clsStream = cls.getResourceAsStream(shortName)
    streamToBytes(clsStream)
  }

  private def streamToBytes(in: InputStream): Array[Byte] = {
    val out = new ByteArrayOutputStream(1024)
    val buffer = new Array[Byte](8192)
    var n = 0
    while (n != -1) {
      n = in.read(buffer)
      if (n != -1) {
        out.write(buffer, 0, n)
      }
    }
    in.close()
    out.close()
    out.toByteArray
  }
}
