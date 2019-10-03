package com.bnue.zio

import com.bnue.zio.CancellableTaskQueue.{Cancelled, Done}
import utest._
import zio.clock.Clock
import zio.{RIO, _}
import zio.duration._
import cats.implicits._
import zio.interop.catz._

import scala.reflect.ClassTag


object QTest extends TestSuite {
   val rt = new DefaultRuntime{}

  implicit class TT[R, T](val t: RIO[rt.Environment, T]) extends AnyVal{
    def runT =   rt.unsafeRunSync (
      t.run.map(_.fold(v => v.failures.headOption orElse(v.defects.headOption).orElse(if(v.interrupted)Some(new Exception("fiber interrupted")) else None), _ => None))
    ).fold( v => throw new java.lang.AssertionError(s"unexpected failure of test run $v"), _.foreach(v => throw  v))
  }

  def assertInstanceOf[T](receivedValue: Any)(f: T => Unit = (_: T) => ())(implicit expectedClassTag: ClassTag[T]): Unit = {
    assert(receivedValue.getClass == expectedClassTag.runtimeClass)
    f(receivedValue.asInstanceOf[T])
  }

  def runUntilShutdown(shutdown: Ref[Boolean], res: String = "done"): URIO[Clock, String] = {
    false.tailRecM(shouldShutdown  =>
      if(shouldShutdown) UIO.succeed(Right(res))
      else shutdown.get.map(Left(_)).delay(50.millis)
    )
  }
  val tests = Tests{

    test("put, cancel, doneCB, cancelCB")(
      CancellableTaskQueue[String, Unit, String].use { ops =>
        for {
          z <- ops.add("k", ZIO.never.map(_ => "never"))
          x <- z.await.race(UIO.succeed("win").delay(1.seconds))
          _ = assert(x == "win")
          zc  <- ops.cancel("k", Some("Joe"))
          zx <- zc.await
          _ = assert(zx == Cancelled(Some("Joe")))
        } yield ()
      }.runT
    )

    test("shutdown: inProgress tasks are interrupted. Escaped Ops is interrupting all calls")(
      for{
        interruptedRef <- Ref.make(false)
        escapedOps <-  CancellableTaskQueue[String, Unit, String].use { ops =>
          ops.add(
            "k",
            ZIO.never.run.flatMap(r => interruptedRef.set(r.interrupted).map(_ => ""))
          ).map(_ => ops)
        }
        cancelRes <- escapedOps.cancel("k", Some("Joe")).run
        putRes <- escapedOps.add("k1", ZIO.never.map(_ =>"")).run
        taskInterrupted <- interruptedRef.get
         _ = assert(cancelRes.interrupted)
         _ = assert(putRes.interrupted)
         _ = assert(taskInterrupted)
      } yield ()
    )

    test("uninterruptible tasks are not cancellable, and are blocking the finalizers")(

      CancellableTaskQueue[String, Unit, String].use { ops =>
        for {
          t <- ops.add("k", ZIO.never.map(_ => "never").uninterruptible)
          c  <- ops.cancel("k", Some("Joe"))
          taskDone <- t.isDone.delay(1.seconds)
          cancelDone <- c.isDone.delay(1.seconds)
          _ = assert (!taskDone && !cancelDone)
        } yield ()
      }
        //TODO: ensure that above assertion runs (eventually on var)
        .fork
        .flatMap(
          _.poll
            .map(_.isDefined)
            .delay(3.seconds)
            .map(completed => assert(!completed))
        ).runT
    )

    test("cancel hooks return Done on uninterruptible tasks and all cancel hooks are satisfied")(

      CancellableTaskQueue[String, Unit, String].use { ops =>
        for {
          e <- ZIO.environment[Clock]
          _ <- ops.add("k", ZIO.succeed("x").delay(1.second).provide(e).uninterruptible)
          c  <- ops.cancel("k", Some("Joe"))
          c1  <- ops.cancel("k", Some("xxx"))
          cancelDone <- c.await
          cancelDone1 <- c1.await
          _ = assertInstanceOf[Done[String]](cancelDone)(v => assert(v.result == "x"))
          _ = assert (cancelDone == cancelDone1)
        } yield ()
      }.runT
    )

    test("all cancel hooks are satisfied with the lable of the first cancel call")(
      CancellableTaskQueue[String, Unit, String].use { ops =>
        for {
          e <- ZIO.environment[Clock]
          _ <- ops.add("k", ZIO.succeed("x").delay(1.second).tap(_ => IO.never.interruptible).provide(e).uninterruptible)
          c  <- ops.cancel("k", Some("Joe"))
          c1  <- ops.cancel("k", Some("xxx"))
          c2  <- ops.cancel("k", Some("yyy"))
          cancelDone <- c.await
          cancelDone1 <- c1.await
          cancelDone2 <- c2.await
          _ = assertInstanceOf[Cancelled](cancelDone)(v => assert(v.toString.contains("Joe")))
          _ = assert (cancelDone == cancelDone1)
          _ = assert (cancelDone == cancelDone2)
        } yield ()
      }.runT
    )

    test("can add and remove tasks while active task is running (also when it is in process of cancellation)")(
      CancellableTaskQueue[String, Unit, String].use { ops =>
        for {
          e <- ZIO.environment[Clock]

          shutdown <- Ref.make[Boolean](false)

          _ <- ops.add("k", runUntilShutdown(shutdown).provide(e).uninterruptible)

          _ <- ops.add("z", IO.never)
          zc  <- ops.cancel("z", Some("Joe"))
          zCancelDone <- zc.await
          _ = assertInstanceOf[Cancelled](zCancelDone)()

          // initiate cancellation
          _  <- ops.cancel("k", Some("Foo"))
          _  <- ops.cancel("k", Some("xxx"))

          _ <- ops.add("z", IO.never)
          zc1  <- ops.cancel("z", Some("Joe"))
          zCancelDone1 <- zc1.await
          _ = assertInstanceOf[Cancelled](zCancelDone1)()

          _ <-  shutdown.set(true)

        } yield ()
      }.runT
    )

    test("join by key simple case")(
      CancellableTaskQueue[String, Unit, String].use { ops =>
        for {
          e <- ZIO.environment[Clock]
          _ <- ops.add("k", ZIO.succeed("x").delay(1.second).provide(e))
          k  <- ops.join("k")
          res <- k.await
          _ = assert(res == Done("x"))
        } yield ()
      }.runT
    )

    test("join tasks which are not yet running")(
      CancellableTaskQueue[String, Unit, String].use { ops =>
        for {
          e <- ZIO.environment[Clock]
          shutdown <- Ref.make[Boolean](false)

          _ <- ops.add("k", runUntilShutdown(shutdown, "res").provide(e).uninterruptible)
          _ <- ops.add("t1", UIO("r1"))
          _ <- ops.add_("t2", UIO("r2"))

          t1  <- ops.join("t1")
          _ <- ops.cancel("t1")
          t2  <- ops.join("t2")
          k  <- ops.join("k")

          _ <- shutdown.set(true)

          t1Res <- t1.await
          t2Res <- t2.await
          kres <- k.await

          _ = assert(t1Res == Cancelled(None))
          _ = assert(t2Res == Done("r2"))
          _ = assert(kres == Done("res"))
        } yield ()
      }.runT
    )

    test("get all registered task names")(
      CancellableTaskQueue[String, Unit, String].use { ops =>
        for {
          _ <- ops.add_("t1", IO.never)
          _ <- ops.add_("t2", IO.never)
          _ <- ops.add_("t3", IO.never)
          tasks <- ops.getRegisteredTaskKeys
          _ = assert(tasks == Set("t1", "t2", "t3"))
        } yield ()
      }.runT
    )

  }
}
