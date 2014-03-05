package edu.berkeley.velox.util

import com.esotericsoftware.kryo.{Serializer, Kryo}
import com.twitter.chill.{AllScalaRegistrar, EmptyScalaKryoInstantiator}
import edu.berkeley.velox.rpc.Request
import java.util.concurrent.LinkedBlockingQueue
import com.esotericsoftware.kryo.io.{ByteBufferOutputStream, ByteBufferInputStream, Input, Output,ByteBufferInput}
import java.nio.ByteBuffer
import java.util
import edu.berkeley.velox.benchmark.operation._
import edu.berkeley.velox.benchmark.operation.PreparePutAllRequest
import edu.berkeley.velox.benchmark.operation.CommitPutAllRequest
import edu.berkeley.velox.datamodel.{PrimaryKey, Row}
import edu.berkeley.velox.benchmark.TPCCItemKey
import edu.berkeley.velox.benchmark.datamodel.serializable.SerializableRow

import com.esotericsoftware.minlog.Log;
import com.esotericsoftware.minlog.Log._

/** A class that, when constructed with a ByteBuffer,
  * doesn't do COMPLETELY the wrong thing with it
  */
class VeloxByteBufferInput(buffer:ByteBuffer) extends ByteBufferInput {
  setBuffer(buffer,buffer.position,buffer.remaining)
}

object KryoThreadLocal {
  val kryoTL = new ThreadLocal[KryoSerializer]() {
    override protected
    def initialValue(): KryoSerializer = VeloxKryoRegistrar.makeKryo()
  }
}

object VeloxKryoRegistrar {

  val pool = new LinkedBlockingQueue[KryoSerializer]()

  // def getKryo(): KryoSerializer = {
  //   var ret = pool.poll
  //   if(ret != null) {
  //     return ret
  //   }

  //   makeKryo()
  // }

  // def returnKryo(kryo: KryoSerializer) = {
  //   pool.put(kryo)
  // }

  var classes = Seq.empty[Class[_]]
  def makeKryo(): KryoSerializer = {
    val instantiator = new EmptyScalaKryoInstantiator
    val kryo = instantiator.newKryo()
    val classLoader = Thread.currentThread.getContextClassLoader
    // Disable reference tracking
    // @todo make this a conf option
    kryo.setReferences( false )
    kryo.setRegistrationRequired(true)
    kryo.register(classOf[GetAllRequest])
    kryo.register(classOf[GetAllResponse])
    kryo.register(classOf[Array[Int]])
    kryo.register(classOf[Array[PrimaryKey]], new Serializer[Array[PrimaryKey]] {
      override def write(kryo: Kryo, output: Output, arr: Array[PrimaryKey]) {
        output.writeShort(arr.length)
        var i = 0
        while(i < arr.length) {
          kryo.writeObject(output, arr(i))
          i += 1
        }
      }

      override def read(kryo: Kryo, input: Input, t: java.lang.Class[Array[PrimaryKey]]) = {
        val len = input.readShort()
        val ret = new Array[PrimaryKey](len)
        var i = 0
        while(i < len) {
          ret(i) = kryo.readObject(input, classOf[PrimaryKey])
          i += 1
        }
        ret
      }

    })
    kryo.register(classOf[Array[Row]], new Serializer[Array[Row]] {
      override def write(kryo: Kryo, output: Output, arr: Array[Row]) {
            output.writeShort(arr.length)
            var i = 0
            while(i < arr.length) {
              kryo.writeObject(output, arr(i))
              i += 1
            }
          }

          override def read(kryo: Kryo, input: Input, t: java.lang.Class[Array[Row]]) = {
            val len = input.readShort()
            val ret = new Array[Row](len)
            var i = 0
            while(i < len) {
              ret(i) = kryo.readObject(input, classOf[Row])
              i += 1
            }
            ret
          }

        })

    //Log.set(LEVEL_TRACE)

    kryo.register(classOf[PreparePutAllRequest])
    kryo.register(classOf[PreparePutAllResponse])
    kryo.register(classOf[CommitPutAllRequest])
    kryo.register(classOf[CommitPutAllResponse])
    kryo.register(classOf[Row])
    kryo.register(classOf[PrimaryKey])
    kryo.register(classOf[util.HashMap[PrimaryKey, Row]])
    kryo.register(classOf[util.HashSet[PrimaryKey]])
    kryo.register(classOf[util.Map[PrimaryKey, Row]])
    kryo.register(classOf[TPCCItemKey])
    kryo.register(classOf[util.ArrayList[Int]])
    kryo.register(classOf[DeferredIncrement])
    kryo.register(classOf[TPCCNewOrderRequest])
    kryo.register(classOf[TPCCNewOrderResponse])
    kryo.register(classOf[TPCCNewOrderLineResult])
    kryo.register(classOf[TPCCLoadRequest])
    kryo.register(classOf[TPCCLoadResponse])

    kryo.register(classOf[SerializableGetAllRequest])
    kryo.register(classOf[SerializableGetAllResponse])
    kryo.register(classOf[SerializablePutAllRequest])
    kryo.register(classOf[SerializablePutAllResponse])
    kryo.register(classOf[SerializableUnlockRequest])
    kryo.register(classOf[SerializableRow])


    // Register important base types
    kryo.register(classOf[Request[_]])
    // Register all of chills classes
    new AllScalaRegistrar().apply(kryo)
    kryo.setClassLoader(classLoader)
    new KryoSerializer(kryo)
  }
}

class KryoSerializer(val kryo: Kryo) {
  val bout = new ByteBufferOutputStream()
  val out = new Output(bout)

  def serialize(x: Any, buffer: ByteBuffer): ByteBuffer = {
    bout.setByteBuffer(buffer)
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
