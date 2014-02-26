package edu.berkeley.velox.util

import java.util.concurrent.atomic.AtomicBoolean
import edu.berkeley.velox.datamodel.{PrimaryKey, Row}
import java.util

class ConcurrentVeloxHashMap[K, V <: AnyVal](val initialSize: Int, val concurrency: Int, name: String) {
  val bins = new Array[VeloxBin](initialSize)

  for(i <- 0 until concurrency) {
    bins(i) = new VeloxBin(initialSize/concurrency, name)
  }

  def size: Int = {
    var ret = 0
    for(i <- 0 until concurrency) {
      ret += bins(i).map.size
    }
    ret
  }

  def get(key: K, orElse: V): V = {
    val bin = bins(key.hashCode % concurrency)
    bin.lock
    val ret = bin.map.getOrElse(key, orElse)
    bin.unlock
    ret
  }

  def put(key: K, value: V) = {
    val hc = key.hashCode
    val bin = bins(hc % concurrency)
    bin.lock
    val ret = bin.map.update(key, value)
    bin.unlock
    ret
  }

  def getAll(keys: util.Map[K, V], none: V) {
    val key_it = keys.keySet.iterator()
    while(key_it.hasNext) {
      val key = key_it.next()
      keys.put(key, get(key, none))
    }
  }

  def putIfAbsent(key: K, value: V, none: V): V = {
    val hc = key.hashCode
    val bin = bins(hc % concurrency)
    bin.lock
    val existing = bin.map.getOrElse(key, none)
    var ret: V = none
    if(existing == none) {
      bin.map.update(key, value)
      ret = value
    }
    bin.unlock
    ret
  }

  def replace(key: K, oldValue: V, value: V, none: V): Boolean = {
    val hc = key.hashCode
    val bin = bins(hc % concurrency)
    bin.lock
    val existing = bin.map.getOrElse(key, none)
    var ret = false
    if(existing == oldValue) {
      bin.map.update(key, value)
      ret = true
    }
    bin.unlock
    ret
  }

  class VeloxBin(initialCapacity: Int, name: String) {
    private val _lock = new AtomicBoolean

    def lock = {
      while(_lock.compareAndSet(false, true)){}
    }

    def unlock = {
      _lock.set(false)
    }

    val map = new PrimitiveKeyOpenHashMap[K, V](initialCapacity, name)
  }
}