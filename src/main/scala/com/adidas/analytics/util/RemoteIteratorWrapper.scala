package com.adidas.analytics.util

import org.apache.hadoop.fs.RemoteIterator

import scala.collection.Iterator

/**
  * Convert RemoteIterator from Hadoop to Scala Iterator that provides all the functions such as map, filter, foreach, etc.
  */

object RemoteIteratorWrapper {
  implicit class RemoteIteratorToIterator[T](underlying: RemoteIterator[T]){
    def remoteIteratorToIterator : Iterator[T] = RemoteIteratorWrapper[T](underlying)
  }
}

case class RemoteIteratorWrapper[T](underlying: RemoteIterator[T]) extends Iterator[T] {
  override def hasNext: Boolean = underlying.hasNext
  override def next(): T = underlying.next()
}

