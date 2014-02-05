package edu.berkeley.velox.util

import com.twitter.chill.{AllScalaRegistrar, Kryo, EmptyScalaKryoInstantiator}
import edu.berkeley.velox.rpc.Request
import java.util.concurrent.LinkedBlockingQueue
import com.esotericsoftware.kryo.io.{ByteBufferOutputStream, ByteBufferInputStream, Input, Output}
import java.nio.ByteBuffer



object VeloxKryoRegistrar {

  val pool = new LinkedBlockingQueue[KryoSerializer]()

  def getKryo(): KryoSerializer = {
    var ret = pool.poll
    if(ret != null) {
      return ret
    }

    makeKryo()
  }

  def returnKryo(kryo: KryoSerializer) = {
    pool.put(kryo)
  }

  var classes = Seq.empty[Class[_]]
  def makeKryo(): KryoSerializer = {
    val instantiator = new EmptyScalaKryoInstantiator
    val kryo = instantiator.newKryo()
    val classLoader = Thread.currentThread.getContextClassLoader
    // Disable reference tracking
    // @todo make this a conf option
    kryo.setReferences( false )
    // Register important base types
    kryo.register(classOf[Request[_]])
    // Register all of chills classes
    new AllScalaRegistrar().apply(kryo)
    kryo.setClassLoader(classLoader)
    new KryoSerializer(kryo)
  }
}

class KryoSerializer(val kryo: Kryo) {
  val byteBuffer = ByteBuffer.allocate(4096)
  val bout = new ByteBufferOutputStream(byteBuffer)
  val out = new Output(bout)
  val bin = new ByteBufferInputStream(byteBuffer)
  val in = new Input(bin)

  def serialize(x: Any): Array[Byte] = {
    byteBuffer.clear()
    kryo.writeClassAndObject(out, x)
    out.flush()
    bout.flush()
    byteBuffer.flip()
    val ret = new Array[Byte](byteBuffer.remaining())
    byteBuffer.get(ret)
    ret
  }

  def deserialize(bytes: Array[Byte]): Any = {
    byteBuffer.clear()
    byteBuffer.put(bytes)
    byteBuffer.flip()
    bin.setByteBuffer(byteBuffer)
    in.setInputStream(bin)
    kryo.readClassAndObject(in)
  }

}
