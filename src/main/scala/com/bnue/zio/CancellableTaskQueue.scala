package com.bnue.zio

import java.util.concurrent.atomic.AtomicInteger

import zio.{Queue => _, _}
import zio.interop.catz._
import cats.implicits._
import zio.Exit.{Failure, Success}

import scala.collection.concurrent.TrieMap

//. TODO: key to id map
object CancellableTaskQueue {
  type CompleteHook[E, V]= Promise[Nothing, CompleteHookStatus[E, V]]
  type CancelHook[E, V]= Promise[Nothing, CancelHookStatus[E, V]]
  private sealed trait Msg[K, E, V]
  private case class Add[K, E, V](key: K, task: IO[E, V], completeHook: Option[CompleteHook[E, V]]) extends Msg[K, E, V]
  private case class Cancel[K, E, V](key: K, hook: Option[CancelHook[E, V]], label: Option[String]) extends Msg[K, E, V]
  private case class Join[K, E, V](key: K, hook: CancelHook[E, V]) extends Msg[K, E, V]
  private case class Completed[K, E, V](id: Int, key: K) extends Msg[K, E, V]

  sealed trait CancelHookStatus[+E, +V]
  sealed trait CompleteHookStatus[+E, +V]
  sealed trait HookStatus[+E, +V] extends CancelHookStatus[E, V] with CompleteHookStatus[E, V]

  case class Cancelled(label: Option[String]) extends HookStatus[Nothing, Nothing]
  case class Done[V] (result: V) extends HookStatus[Nothing, V]
  case class Failed[E] (v: List[E]) extends HookStatus[E, Nothing]
  case class Died (v: List[Throwable]) extends HookStatus[Nothing, Nothing]
  case object Duplicate extends CompleteHookStatus[Nothing, Nothing]
  case object NotFound extends CancelHookStatus[Nothing, Nothing]

  private def nextId(id: Int): Int = (id + 1) % Int.MaxValue

  // TODO: support parallelism
  // TODO: create looser version of this that captures V on put and completeHook, loose V in cancel
  class Ops[K, E, V](q: Queue[Msg[K, E, V]], val getRegisteredTaskNames: UIO[collection.Set[K]] ) {
    // q.offer cannot return false in case of unbounded queue
    def put_(key: K, task: IO[E, V]): UIO[Unit] = q.offer(Add(key, task, None)).unit

    def put(key: K, task: IO[E, V]): UIO[CompleteHook[E, V]] =
      Promise.make[Nothing, CompleteHookStatus[E, V]]
        .tap(p => q.offer(Add(key, task, Some(p))))

    def cancel_(key: K, label: Option[String] = None): UIO[Unit] = q.offer(Cancel(key, None, label)).unit

    def cancel(key: K, label: Option[String] = None): UIO[CancelHook[E, V]] =
      Promise.make[Nothing, CancelHookStatus[E, V]]
        .tap(p => q.offer(Cancel(key, Some(p), label)))

    def join(key: K): UIO[CancelHook[E, V]] = Promise.make[Nothing, CancelHookStatus[E, V]].tap(p => q.offer(Join(key, p)))
  }

  def runProcessing[K, E, V](
                            q: Queue[Msg[K, E, V]],
                            taskStore: TrieMap[Int, Add[K, E, V]],
                            keyStore: TrieMap[K, Int],
                            genNextId: UIO[Int],
                            inProgress: Ref[Option[(Int, Fiber[Nothing, HookStatus[E, V]])]],
                            cancelInProgress:  Ref[Option[(Int, Fiber[Nothing, HookStatus[E, V]])]],
                            cancelRequestedBy: Ref[Option[String]],
                            shutdown: Ref[Boolean]
                    ): UIO[Unit] = {

    def toStatus(cancelledBy: Option[String], e:Exit[E, V] ):  HookStatus[E, V] = e match {
      case Success(v) => Done(v)
      case Failure(cause) =>
        if(cause.interrupted) Cancelled(cancelledBy)
        else if(cause.failed) Failed(cause.failures)
        else Died(cause.defects)
    }
    def runTask(id: Int, add: Add[K, E, V]): UIO[Unit] =
      inProgress.get.flatMap(_.traverse_(_ => UIO.dieMessage("trying to start new task when something is in progress"))) *>
        add
          .task
          // whe whole execution block (message processing) where runTask is called is uninterruptible
          .interruptible
          .run
          .flatMap(exit => cancelRequestedBy.get.map(toStatus(_, exit)))
          // Completed is guaranteed to be added to queue before hook succeeds and user that waits for hook will only be able to observe the state as it is AFTER Completed is processed
          .tap(status => q.offer(Completed(id, add.key)) *> add.completeHook.traverse_(_.succeed(status)))
          .fork
          .flatMap(fiber => inProgress.set(Some(id -> fiber)))

    def switchToNextTask(id: Int): UIO[Unit] = IO.when (taskStore.nonEmpty) {
      UIO.effectSuspendTotal {
        val (nid, nextTask) = (0 until Int.MaxValue)
          .view
          .flatMap { i =>
            val nid = nextId(id + i)
            taskStore.get(nid).map(nid -> _)
          }
          .headOption
          .ensuring(_.isDefined, "cannot find next task in nonempty taskstore")
          .get

        runTask(nid, nextTask)
      }
    }

    q.take.flatMap{

      case add: Add[K, E, V] =>
        cond(UIO(keyStore.contains(add.key)))(
          _true = add.completeHook.traverse_(_.succeed(Duplicate)),
          _false = genNextId.flatMap(id =>
            UIO(keyStore.put(add.key, id)) *>
            UIO(taskStore.put(id, add)) *>
            IO.whenM(
              and(inProgress.get.map(_.isEmpty) , cancelInProgress.get.map(_.isEmpty))
            )(runTask(id, add))
          )
        )
      case Cancel(key, cancelHookOpt, label) =>
        UIO(keyStore.get(key)).flatMap{
          case None => cancelHookOpt.traverse_(_.succeed(NotFound))
          case Some(id) => cancelInProgress.get.flatMap{
            // when cancelInprogress fiber is completed Completed msg is guaranteed to be in the queue user will always see the proper state
            case Some(`id` -> fiber) => cancelHookOpt.traverse_(_.complete(fiber.join))
            case _ => inProgress.get.flatMap {
              case Some(`id` -> fiber) =>
                cancelRequestedBy.set(label) *>
                  fiber.interrupt
                    .map{
                      case Success(v) => v
                      case e => throw new AssertionError(s"failure during the interruption of sandboxed IO $e")
                    }
                    .fork
                    .flatMap(interruptFiber =>
                      cancelHookOpt.traverse_(_.complete(interruptFiber.join)) *>
                        cancelInProgress.set(Some(id -> interruptFiber))
                    )

              case _ =>
                for{
                  _ <-  UIO(keyStore.remove(key))
                  add <- UIO(taskStore.remove(id).ensuring(_.isDefined, "key-id not in sync").get)
                  _ <- add.completeHook.traverse_(_.succeed(Cancelled(label)))
                  _ <- cancelHookOpt.traverse_(_.succeed(Cancelled(label)))
                } yield ()
            }
          }
        }
      case Join(key, joinHook) =>
        // Dirty hack to save a bit of runtime overhead: hooks are under control of this library and we '_know_' that Duplicate message cannot appear at this stage
        // To do this in type safe manner we'd have to put 2 Promises into 'Add' msg: 1) HookStatus, 2)Duplicate.type and race between them. This way we could ensure that duplicte msg never shows up in this method.
        val awaitNoDuplicateUnsfe: CompleteHook[E, V] => UIO[HookStatus[E, V]] =
          _.await.flatMap{
            case Duplicate => IO.dieMessage("improggress task is completed with Duplicate")
            case h: HookStatus[E, V] => IO.succeed(h)
          }
        UIO(keyStore.get(key)).flatMap{
          case None => joinHook.succeed(NotFound)
          case Some(id) =>
            inProgress.get.flatMap{
              case Some((`id`, fiber)) => joinHook.complete(fiber.join)
              case _ =>
                def registerHook(add: Add[K, E, V]): UIO[Promise[Nothing, CompleteHookStatus[E, V]]] =
                  Promise.make[Nothing, CompleteHookStatus[E, V]].tap(complHook =>
                    UIO(taskStore.put(id, add.copy(completeHook = Some(complHook))))
                  )
                //taskstore must contain all ids from keystore
                UIO(taskStore(id))
                  .flatMap(add => add.completeHook.fold(registerHook(add))(IO.succeed))
                  .flatMap(awaitNoDuplicateUnsfe.andThen(joinHook.complete))
                }
            }

      case Completed(id, key) =>
        UIO(keyStore.remove(key)) *>
          UIO(taskStore.remove(id)) *>
          cancelInProgress.get.flatMap(_.traverse_{
            case(cid, task) =>
              assert(cid == id, s"Completed key '$key' does not match key of the task being cancelled '$cid'")
              //guarantees that cancelHookOpt will be fulfilled before fiber gets garbage collected
              task.join
          }) *>
          cancelInProgress.set(None) *>
          cancelRequestedBy.set(None) *>
          inProgress.set(None) *>
          switchToNextTask(id)

    }.uninterruptible *>
      // controlled shutdown: this code can be interrupted only before or after message extraction+processing
      IO.whenM(shutdown.get.map(!_))(
        runProcessing(q, taskStore, keyStore, genNextId, inProgress, cancelInProgress, cancelRequestedBy, shutdown)
      )
  }
  def apply[K, E, V]: ZManaged[Any, Nothing, Ops[K, E, V]] = {
      for{
       shutdown <-  ZManaged.fromEffect(Ref.make(false))
       cancelRequestedBy <- ZManaged.fromEffect(Ref.make[Option[String]](None))
       genNextId <-  ZManaged.fromEffect(
         UIO(new AtomicInteger(0))
           .map(idHolder =>  UIO(idHolder.getAndUpdate(nextId)))
       )
       keyStore <- ZManaged.fromEffect(UIO(TrieMap.empty[K, Int]))

       taskStore <- ZManaged.make(UIO(TrieMap.empty[Int, Add[K, E, V]]))(UIO.traverse_(_)(_._2.completeHook.traverse_(_.interrupt)))
       // stopping the queue after inProgress task is completed because it calls queue.add(Completed) on complete
       q <- ZManaged.make(Queue.unbounded[Msg[K, E, V]])(v =>
         v.shutdownAndTakeAll.flatMap(_.traverse_{
           case Add(_, _, hook) => hook.traverse_(_.interrupt)
           case Cancel(_, hook, _) => hook.traverse_(_.interrupt)
           case Join(_, hook) => hook.interrupt
           case Completed(_, _)  => UIO.unit
         }))
       cancelInProgress <-  ZManaged.make(Ref.make[Option[(Int, Fiber[Nothing, HookStatus[E, V]])]](None))(v =>
         //guarantees that cancelHookOpt will be fulfilled
         v.get.flatMap(_.traverse_(_._2.join))
       )
       inProgress <- ZManaged.make(Ref.make[Option[(Int, Fiber[Nothing, HookStatus[E, V]])]](None))(v =>
         v.get.flatMap(_.traverse_(_._2.interrupt))
       )
       ops =  new Ops[K, E, V] (q, UIO(keyStore.keySet))
       _ <- ZManaged.make(runProcessing(q, taskStore, keyStore, genNextId, inProgress, cancelInProgress, cancelRequestedBy, shutdown).fork)(t =>
         shutdown.set(true) *>
           // waking up in case if q is empty and runProcessing is locked on '.take'
           ops.put_(null.asInstanceOf[K], IO.succeed(null.asInstanceOf[V]))
           *> t.join
       )
      } yield ops
  }
}