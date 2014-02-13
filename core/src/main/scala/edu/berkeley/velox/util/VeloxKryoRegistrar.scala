package edu.berkeley.velox.util

import com.esotericsoftware.kryo.Kryo
import com.twitter.chill.{AllScalaRegistrar, EmptyScalaKryoInstantiator}
import edu.berkeley.velox.rpc.Request
import java.util.concurrent.LinkedBlockingQueue
import com.esotericsoftware.kryo.io.{ByteBufferOutputStream, ByteBufferInputStream, Input, Output,ByteBufferInput}
import java.nio.ByteBuffer

/** A class that, when constructed with a ByteBuffer,
  * doesn't do COMPLETELY the wrong thing with it
  */
class VeloxByteBufferInput(buffer:ByteBuffer) extends ByteBufferInput {
  setBuffer(buffer,buffer.position,buffer.remaining)
}


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

  def serialize(x: Any, buffer: ByteBuffer): ByteBuffer = {
    val bout = new ByteBufferOutputStream(buffer)
    val out = new Output(bout)
    kryo.writeClassAndObject(out, x)
    out.flush()
    bout.flush()
    buffer
  }

  def deserialize(buffer: ByteBuffer): Any = {
    val in = new VeloxByteBufferInput(buffer)
    kryo.readClassAndObject(in)
  }

}
