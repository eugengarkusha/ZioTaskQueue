package com.bnue.zio

import zio.{Queue => _, _}
import zio.interop.catz._
import cats.implicits._
import zio.Exit.{Failure, Success}

import scala.annotation.tailrec

object CancellableTaskQueue {
  type CompleteHook[E, V] = Promise[Nothing, TaskCompletionStatus[E, V]]
  type CancelHook[E, V]   = Promise[Nothing, TaskCancelStatus[E, V]]
  sealed private trait Msg[K, E, V]
  private case class Add[K, E, V](key: K, task: IO[E, V], completeHook: Option[CompleteHook[E, V]]) extends Msg[K, E, V]
  private case class Cancel[K, E, V](key: K, hook: Option[CancelHook[E, V]], label: Option[String]) extends Msg[K, E, V]
  private case class Join[K, E, V](key: K, hook: CancelHook[E, V])                                  extends Msg[K, E, V]
  private case class Completed[K, E, V](id: Int, key: K)                                            extends Msg[K, E, V]
  private case class GetKeys[K, E, V](p: Promise[Nothing, Set[K]])                                  extends Msg[K, E, V]

  sealed trait TaskCancelStatus[+E, +V]
  sealed trait TaskCompletionStatus[+E, +V]
  sealed trait TaskStatus[+E, +V] extends TaskCancelStatus[E, V] with TaskCompletionStatus[E, V]

  case class Cancelled(label: Option[String]) extends TaskStatus[Nothing, Nothing]
  case class Done[V](result: V)               extends TaskStatus[Nothing, V]
  case class Failed[E](v: List[E])            extends TaskStatus[E, Nothing]
  case class Died(v: List[Throwable])         extends TaskStatus[Nothing, Nothing]
  case object NotFound                        extends TaskCancelStatus[Nothing, Nothing]
  case object Duplicate                       extends TaskCompletionStatus[Nothing, Nothing]
  case object Rejected                        extends TaskCompletionStatus[Nothing, Nothing]

  private case class State[K, E, V](
      taskStore: Map[Int, Add[K, E, V]],
      keyStore: Map[K, Int],
      inProgress: Option[(Int, Fiber[Nothing, TaskStatus[E, V]])],
      cancelInProgress: Option[(Int, Fiber[Nothing, TaskStatus[E, V]])],
      lastAdddedId: Int
  )

  // TODO: support parallelism
  // TODO: create looser version of this that captures V on put and completeHook, loose V in cancel
  class Ops[K, E, V](q: Queue[Msg[K, E, V]]) {
    // q.offer cannot return false in case of unbounded queue
    def add_(key: K, task: IO[E, V]): UIO[Unit] = q.offer(Add(key, task, None)).unit

    def add(key: K, task: IO[E, V]): UIO[TaskCompletionStatus[E, V]] =
      Promise
        .make[Nothing, TaskCompletionStatus[E, V]]
        .flatMap(p => q.offer(Add(key, task, Some(p))) *> p.await)

    def cancel_(key: K, label: Option[String] = None): UIO[Unit] = q.offer(Cancel(key, None, label)).unit

    def cancel(key: K, label: Option[String] = None): UIO[TaskCancelStatus[E, V]] =
      Promise
        .make[Nothing, TaskCancelStatus[E, V]]
        .flatMap(p => q.offer(Cancel(key, Some(p), label)) *> p.await)

    def join(key: K): UIO[TaskCancelStatus[E, V]] =
      Promise.make[Nothing, TaskCancelStatus[E, V]].flatMap(p => q.offer(Join(key, p)) *> p.await)

    val getRegisteredTaskKeys: UIO[Set[K]] = Promise.make[Nothing, Set[K]].flatMap(p => q.offer(GetKeys(p)) *> p.await)
  }

  def runProcessing[K, E, V](
      q: Queue[Msg[K, E, V]],
      shutdown: Ref[Boolean],
      cancelRequestedBy: Ref[Option[String]],
      taskStoreSize: Int,
      state: State[K, E, V]
  ): UIO[Unit] = {
    import state._

    def toStatus(e: Exit[E, V], cancelReq: Option[String]): TaskStatus[E, V] = e match {
      case Success(v) => Done(v)
      case Failure(cause) =>
        if (cause.interrupted) Cancelled(cancelReq)
        else if (cause.failed) Failed(cause.failures)
        else Died(cause.defects)
    }

    def findId(startId: Int, f: Int => Boolean, msg: => String): Int = {
      @tailrec
      def find(currentId: Int): Int = {
        val rolledId = currentId % taskStoreSize
        if (f(rolledId)) rolledId
        //  startId == rolledId means that we rolled over the whole ids space
        else if (startId != rolledId) find(rolledId + 1)
        else throw new AssertionError(msg)
      }
      find(startId + 1)
    }

    def runTask(id: Int, add: Add[K, E, V]): UIO[Fiber[Nothing, TaskStatus[E, V]]] =
      IO.whenM(inProgress.fold(IO.succeed(false))(_._2.poll.map(_.isEmpty)))(
          IO.dieMessage("trying to start new task when something is in progress")
      ) *>
        add.task
        // whe whole execution block (message processing) where runTask is called is uninterruptible
          .interruptible
          .run
          .flatMap(exit => cancelRequestedBy.get.map(toStatus(exit, _)))
          // Completed is guaranteed to be added to queue before hook succeeds and user that waits for hook will only be able to observe the state as it is AFTER Completed is processed
          .tap(status => q.offer(Completed(id, add.key)) *> add.completeHook.traverse_(_.succeed(status)))
          .fork

    q.take
      .flatMap {
        case add: Add[K, E, V] =>
          if (keyStore.contains(add.key)) add.completeHook.traverse_(_.succeed(Duplicate)).as(state)
          else if (taskStore.size == taskStoreSize) add.completeHook.traverse_(_.succeed(Rejected)).as(state)
          else {
            val id = findId(
                lastAdddedId,
                v => !taskStore.contains(v),
                "cannot find vacant id in non-full taskStore"
            )

            inProgress
              .orElseIO(runTask(id, add).map(t => Some(id -> t)))
              .map(state.copy(taskStore.updated(id, add), keyStore.updated(add.key, id), _, lastAdddedId = id))
          }

        case Cancel(key, cancelHookOpt, label) =>
          keyStore.get(key) match {
            case None => cancelHookOpt.traverse_(_.succeed(NotFound)).as(state)
            case Some(id) =>
              cancelInProgress match {
                // when cancelInprogress fiber is completed Completed msg is guaranteed to be in the queue user will always see the proper state
                case Some(`id` -> fiber) => cancelHookOpt.traverse_(_.completeWith(fiber.join)).as(state)
                case _ =>
                  inProgress match {
                    case Some(`id` -> fiber) =>
                      cancelRequestedBy.set(label) *>
                        fiber
                          .interrupt
                          .map {
                            case Success(v) => v
                            case e          => throw new AssertionError(s"failure during the interruption of sandboxed IO $e")
                          }
                          .fork
                          .flatMap(
                              interruptFiber =>
                                cancelHookOpt
                                  .traverse_(_.completeWith(interruptFiber.join))
                                  .as(state.copy(cancelInProgress = Some(id -> interruptFiber)))
                          )
                    case _ =>
                      unsafe(taskStore(id))("key-id is not in sync")
                        .completeHook
                        .traverse_(_.succeed(Cancelled(label)))
                        .*>(cancelHookOpt.traverse_(_.succeed(Cancelled(label))))
                        .as(state.copy(taskStore - id, keyStore - key))
                  }
              }
          }
        case Join(key, joinHook) =>
          // Dirty hack to save a bit of runtime overhead: hooks are under control of this library and we '_know_' that Duplicate message cannot appear at this stage
          // To do this in type safe manner we'd have to put 2 Promises into 'Add' msg: 1) TaskStatus, 2)Duplicate.type and race between them. This way we could ensure that duplicate msg never shows up in this method.
          val awaitAndTrimForbiddenMsgs: CompleteHook[E, V] => UIO[TaskStatus[E, V]] =
            _.await.flatMap {
              case t @ (Duplicate | Rejected) => IO.dieMessage(s"in progress task is completed with '$t'")
              case h: TaskStatus[E, V]        => IO.succeed(h)
            }
          keyStore.get(key) match {
            case None => joinHook.succeed(NotFound).as(state)
            case Some(id) =>
              inProgress match {
                case Some((`id`, fiber)) => joinHook.completeWith(fiber.join).as(state)
                case _ =>
                  val add = unsafe(taskStore(id))("key-id is not in sync")
                  add.completeHook match {
                    case Some(completeHook) => joinHook.completeWith(awaitAndTrimForbiddenMsgs(completeHook)).as(state)
                    case None =>
                      for {
                        completeHook <- Promise.make[Nothing, TaskCompletionStatus[E, V]]
                        _            <- joinHook.completeWith(awaitAndTrimForbiddenMsgs(completeHook))
                      } yield state.copy(taskStore = taskStore.updated(id, add.copy(completeHook = Some(completeHook))))
                  }
              }
          }

        case GetKeys(p) => p.succeed(keyStore.keySet).as(state)

        case Completed(id, key) =>
          cancelInProgress.traverse_ {
            case (cid, task) =>
              assert(cid == id, s"Completed key '$key' does not match key of the task being cancelled '$cid'")
              //guarantees that cancelHookOpt will be fulfilled before fiber gets garbage collected
              task.join
          } *>
            ZIO
              .optional(taskStore.size > 1) {
                val nextId = findId(
                    id,
                    taskStore.contains,
                    "cannot find next task in nonempty taskstore"
                )
                // Completed message is sent from the inProgress fiber after the task is completed. Waiting for this fiber to complete before running next task.
                inProgress.traverse_(_._2.join) *>
                  runTask(nextId, taskStore(nextId)).map(nextId -> _)
              }
              .map(state.copy(taskStore - id, keyStore - key, _, None))
      }
      .uninterruptible
      .flatMap { state =>
        // controlled shutdown: this code can be interrupted only before or after message extraction+processing
        shutdown
          .get
          .flatMap(
              sd =>
                if (!sd) runProcessing(q, shutdown, cancelRequestedBy, taskStoreSize, state)
                else {
                  inProgress.traverse_(_._2.interrupt) *>
                    //guarantees that cancelHookOpt will be fulfilled
                    cancelInProgress.traverse_(_._2.join) *>
                    UIO.traverse_(taskStore)(_._2.completeHook.traverse_(_.interrupt))
                }
          )
      }
  }

  def apply[K, E, V](commandQueueCapacity: Int = 1024, taskStoreCapacity: Int = Int.MaxValue): ZManaged[Any, Nothing, Ops[K, E, V]] =
    for {
      shutdown          <- ZManaged.fromEffect(Ref.make(false))
      cancelRequestedBy <- ZManaged.fromEffect(Ref.make[Option[String]](None))

      commandQueue <- ZManaged.make(zio.Queue.bounded[Msg[K, E, V]](commandQueueCapacity).flatMap(Queue.wrap(_)))(
                         v =>
                           v.shutdownAndTakeAll
                             .flatMap(_.traverse_ {
                               case Add(_, _, hook)                             => hook.traverse_(_.interrupt)
                               case Cancel(_, hook, _)                          => hook.traverse_(_.interrupt)
                               case Join(_, hook)                               => hook.interrupt
                               case _: Completed[_, _, _] | _: GetKeys[_, _, _] => UIO.unit
                             })
                     )

      ops = new Ops[K, E, V](commandQueue)
      _ <- ZManaged.make(
              runProcessing[K, E, V](
                  commandQueue,
                  shutdown,
                  cancelRequestedBy,
                  taskStoreCapacity,
                  State(Map.empty, Map.empty, None, None, -1)
              ).fork
          )(
              t =>
                shutdown.set(true) *>
                  // waking up in case if q is empty and runProcessing is locked on '.take'
                  ops.add_(null.asInstanceOf[K], IO.succeed(null.asInstanceOf[V]))
                  *> t.join
          )
    } yield ops
}
