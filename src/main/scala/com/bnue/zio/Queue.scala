package com.bnue.zio

import zio.{Ref, Semaphore, UIO, ZIO, ZQueue}
import cats.implicits._
import zio.interop.catz._

object Queue {
  def wrap[A](q: zio.Queue[A], numberOfPermits: Int = Int.MaxValue): UIO[Queue[A]] =
    (Semaphore.make(numberOfPermits), Ref.make(false)).mapN((new Queue[A](q, _, numberOfPermits, _)))
}

// ordinary ZIO queue with additional 'shutdownAndTakeAll' method.
class Queue[A](q: zio.Queue[A], s: Semaphore, numberOfPermits: Int, shutdownFlag: Ref[Boolean]) extends ZQueue[Any, Nothing, Any, Nothing, A, A] {
  private def withPermit[T](f: UIO[T]): UIO[T] =
    cond(shutdownFlag.get)(UIO.interrupt, s.withPermit(f))
  override def take: ZIO[Any, Nothing, A] = withPermit(q.take)

  override def awaitShutdown: UIO[Unit] = withPermit(q.awaitShutdown)

  override def capacity: Int = q.capacity

  override def isShutdown: UIO[Boolean] = withPermit(q.isShutdown)

  override def offer(a: A): UIO[Boolean] = withPermit(q.offer(a))

  override def offerAll(as: Iterable[A]): UIO[Boolean] = withPermit(q.offerAll(as))

  override def size: UIO[Int] = withPermit(q.size)

  override def takeAll: UIO[List[A]] = withPermit(q.takeAll)

  override def takeUpTo(max: Int): UIO[List[A]] = withPermit(q.takeUpTo(max))

  override def shutdown: UIO[Unit] = withPermit(q.shutdown)

  def shutdownAndTakeAll: UIO[List[A]] = shutdownFlag.set(true) *> s.withPermits(numberOfPermits)(q.takeAll)
}
