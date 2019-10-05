package com.bnue.zio

import com.bnue.zio.CancellableTaskQueue.{Cancelled, Done, Ops}
import utest._
import zio.clock.Clock
import zio.{RIO, _}
import zio.duration._
import cats.implicits._
import zio.Exit.Success
import zio.interop.catz._
import scala.language.higherKinds
import scala.reflect.ClassTag
import scala.util.Random

object QTest extends TestSuite {
  val rt = new DefaultRuntime {}

  implicit class TT[R, T](val t: RIO[rt.Environment, T]) extends AnyVal {
    def runT() =
      rt.unsafeRunSync(
            t.run
              .map(
                  _.fold(
                      v => v.failures.headOption orElse (v.defects.headOption).orElse(if (v.interrupted) Some(new Exception("fiber interrupted")) else None),
                      _ => None
                  )
              )
        )
        .fold(v => throw new java.lang.AssertionError(s"unexpected failure of test run $v"), _.foreach(v => throw v))
  }

  implicit class IdOps[T](val t: T) extends AnyVal {
    def |>[R](f: T => R): R = f(t)
  }

  def assertSuccess[E, V](z: Exit[E, V]): V = assertInstanceOf[Success[V]](z).value
  def assertInstanceOf[T](receivedValue: Any)(implicit expectedClassTag: ClassTag[T]): T = {
    assert(receivedValue.getClass == expectedClassTag.runtimeClass)
    receivedValue.asInstanceOf[T]
  }

  def runUntilShutdown(shutdown: Ref[Boolean], res: String = "done"): URIO[Clock, String] =
    false.tailRecM(
        shouldShutdown =>
          if (shouldShutdown) UIO.succeed(Right(res))
          else shutdown.get.map(Left(_)).delay(50.millis)
    )
  val tests = Tests {

    test("put, cancel, doneCB, cancelCB")(
        CancellableTaskQueue[String, Unit, String]
          .use { ops =>
            for {
              x  <- ops.add("k", ZIO.never.map(_ => "never")).map(_.toString).race(UIO.succeed("win").delay(1.seconds))
              _  = assert(x == "win")
              zx <- ops.cancel("k", Some("Joe"))
              _  = assert(zx == Cancelled(Some("Joe")))
            } yield ()
          }
          .runT()
    )

    test("shutdown: inProgress tasks are interrupted. Escaped Ops is interrupting all calls")(
        for {
          interruptedRef <- Ref.make(false)
          escapedOps <- CancellableTaskQueue[String, Unit, String].use { ops =>
                         ops
                           .add(
                               "k",
                               ZIO.never.run.flatMap(r => interruptedRef.set(r.interrupted).map(_ => ""))
                           )
                           .map(_ => ops)
                       }
          cancelRes       <- escapedOps.cancel("k", Some("Joe")).run
          putRes          <- escapedOps.add("k1", ZIO.never.map(_ => "")).run
          taskInterrupted <- interruptedRef.get
          _               = assert(cancelRes.interrupted)
          _               = assert(putRes.interrupted)
          _               = assert(taskInterrupted)
        } yield ()
    )

    test("uninterruptible tasks are not cancellable, and are blocking the finalizers")(
        CancellableTaskQueue[String, Unit, String]
          .use { ops =>
            for {
              t          <- ops.add("k", ZIO.never.map(_ => "never").uninterruptible).fork
              c          <- ops.cancel("k", Some("Joe")).fork
              taskDone   <- t.poll.map(_.isDefined).delay(1.seconds)
              cancelDone <- c.poll.map(_.isDefined).delay(1.seconds)
              _          = assert(!taskDone && !cancelDone)
            } yield ()
          }
          //TODO: ensure that above assertion runs (eventually on var)
          .fork
          .flatMap(
              _.poll
                .map(_.isDefined)
                .delay(3.seconds)
                .map(completed => assert(!completed))
          )
          .runT()
    )

    test("cancel hooks return Done on uninterruptible tasks and all cancel hooks are satisfied")(
        CancellableTaskQueue[String, Unit, String]
          .use { ops =>
            for {
              e           <- ZIO.environment[Clock]
              _           <- ops.add("k", ZIO.succeed("x").delay(1.second).provide(e).uninterruptible).fork
              c           <- ops.cancel("k", Some("Joe")).fork
              c1          <- ops.cancel("k", Some("xxx")).fork
              cancelDone  <- c.await
              cancelDone1 <- c1.await
              _ = assertSuccess(cancelDone) |>
                assertInstanceOf[Done[_]] |>
                (v => assert(v.result == "x"))

              _ = assert(cancelDone == cancelDone1)
            } yield ()
          }
          .runT()
    )

    test("all cancel hooks are satisfied with the label of the first cancel call")(
        CancellableTaskQueue[String, Unit, String]
          .use { ops =>
            for {
              e           <- ZIO.environment[Clock]
              _           <- ops.add("k", ZIO.succeed("x").delay(1.second).tap(_ => IO.never.interruptible).provide(e).uninterruptible).fork
              c           <- ops.cancel("k", Some("Joe")).fork
              c1          <- ops.cancel("k", Some("xxx")).fork
              c2          <- ops.cancel("k", Some("yyy")).fork
              cancelDone  <- c.await
              cancelDone1 <- c1.await
              cancelDone2 <- c2.await
              _ = assertSuccess(cancelDone) |>
                assertInstanceOf[Cancelled] |>
                (c => assert(c.label.contains("Joe")))

              _ = assert(cancelDone == cancelDone1)
              _ = assert(cancelDone == cancelDone2)
            } yield ()
          }
          .runT()
    )

    test("can add and remove tasks while active task is running (also when it is in process of cancellation)")(
        CancellableTaskQueue[String, Unit, String]
          .use { ops =>
            for {
              e <- ZIO.environment[Clock]

              shutdown <- Ref.make[Boolean](false)

              _ <- ops.add("k", runUntilShutdown(shutdown).provide(e).uninterruptible).fork

              _           <- ops.add("z", IO.never).fork
              zc          <- ops.cancel("z", Some("Joe")).fork
              zCancelDone <- zc.await

              _ = assertSuccess(zCancelDone) |>
                assertInstanceOf[Cancelled]

              // initiate cancellation
              _ <- ops.cancel("k", Some("Foo")).fork
              _ <- ops.cancel("k", Some("xxx")).fork

              _            <- ops.add("z", IO.never).fork
              zCancelDone1 <- ops.cancel("z", Some("Joe"))

              _ = assertInstanceOf[Cancelled](zCancelDone1)

              _ <- shutdown.set(true)

            } yield ()
          }
          .runT()
    )

    test("join by key simple case")(
        CancellableTaskQueue[String, Unit, String].use { ops =>
          for {
            e   <- ZIO.environment[Clock]
            _   <- ops.add("k", ZIO.succeed("x").delay(1.second).provide(e)).fork
            res <- ops.join("k")
            _   = assert(res == Done("x"))
          } yield ()
        }.runT
    )

    test("join tasks which are not yet running")(
        CancellableTaskQueue[String, Unit, String]
          .use { ops =>
            for {
              e        <- ZIO.environment[Clock]
              shutdown <- Ref.make[Boolean](false)

              _ <- ops.add("k", runUntilShutdown(shutdown, "res").provide(e).uninterruptible).fork
              _ <- ops.add("t1", UIO("r1")).fork
              _ <- ops.add_("t2", UIO("r2")).fork

              t1 <- ops.join("t1").fork
              _  <- ops.cancel("t1").fork
              t2 <- ops.join("t2").fork
              k  <- ops.join("k").fork

              _ <- shutdown.set(true)

              t1Res <- t1.await
              t2Res <- t2.await
              kres  <- k.await

              _ = assertSuccess(t1Res) |> (v => assert(v == Cancelled(None)))
              _ = assertSuccess(t2Res) |> (v => assert(v == Done("r2")))
              _ = assertSuccess(kres) |> (v => assert(v == Done("res")))
            } yield ()
          }
          .runT()
    )

    test("get all registered task names")(
        CancellableTaskQueue[String, Unit, String]
          .use { ops =>
            for {
              _     <- ops.add_("t1", IO.never)
              _     <- ops.add_("t2", IO.never)
              _     <- ops.add_("t3", IO.never)
              tasks <- ops.getRegisteredTaskKeys
              _     = assert(tasks == Set("t1", "t2", "t3"))
            } yield ()
          }
          .runT()
    )

    test("naive monkey test (no assertion errors, queue terminates properly)") {
      def opsFuncs(c: Clock): List[Ops[String, Unit, String] => ZIO[Any, Nothing, Any]] = {
        val p    = "abcde".toSeq.permutations.map(_.unwrap).toList
        val keys = Random.shuffle((0 to 150).map(_ => p).reduce(_ ++ _))
        keys.map(
            k =>
              (o: Ops[String, Unit, String]) =>
                Random.nextInt(5) match {
                  case 0 => o.add(k, IO.succeed("done").delay(Random.nextInt(100).millis).provide(c))
                  case 1 => o.cancel(k)
                  case 2 => o.getRegisteredTaskKeys
                  case 3 => o.join(k)
                  case 4 => o.add(k, IO.dieMessage("boom").delay(Random.nextInt(100).millis).provide(c))
                }
        )
      }

      CancellableTaskQueue[String, Unit, String]
        .use { ops =>
          for {
            e <- ZIO.environment[Clock]
            _ <- IO.traversePar(opsFuncs(e))(_(ops))
//           _ <- UIO(println(res.mkString("\n")))
          } yield ()
        }
        .runT()
    }

  }
}
