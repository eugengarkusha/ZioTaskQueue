package com.bnue.zio

import zio.{Queue => _, _}
import zio.interop.catz._
import cats.implicits._
import zio.Exit.{Failure, Success}
import zio.stream.{Sink, ZStream}

import scala.annotation.tailrec

object CancellableTaskQueue {
  type CompleteHook[E, V] = Promise[Nothing, TaskCompletionStatus[E, V]]
  type CancelHook[E, V]   = Promise[Nothing, TaskCancelStatus[E, V]]
  sealed private trait Msg[K, E, V]
  private case class Add[K, E, V](key: K, task: IO[E, V], completeHook: Option[CompleteHook[E, V]]) extends Msg[K, E, V]
  private case class Cancel[K, E, V](key: K, hook: Option[CancelHook[E, V]], label: Option[String]) extends Msg[K, E, V]
  private case class Join[K, E, V](key: K, hook: CancelHook[E, V])                                  extends Msg[K, E, V]
  private case class Completed[K, E, V](id: Int, key: K, exit: Exit[E, V])                          extends Msg[K, E, V]
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
      inProgress: Option[(Int, Fiber[Nothing, Exit[E, V]])],
      cancelInProgress: Option[(Int, Fiber[Nothing, TaskStatus[E, V]])],
      inProgressCancelRequestedBy: Option[String],
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

  def apply[K, E, V](commandQueueCapacity: Int = 1024, taskStoreCapacity: Int = Int.MaxValue): ZManaged[Any, Nothing, Ops[K, E, V]] =
    for {
      shutdown <- ZManaged.fromEffect(Ref.make(false))

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
                  taskStoreCapacity,
                  State(Map.empty, Map.empty, None, None, None, -1)
              ).fork
          )(
              t =>
                shutdown.set(true) *>
                  // waking up in case if q is empty and runProcessing is locked on '.take'
                  ops.add_(null.asInstanceOf[K], IO.succeed(null.asInstanceOf[V]))
                  *> t.join
          )
    } yield ops

  private def runProcessing[K, E, V](
      q: Queue[Msg[K, E, V]],
      shutdown: Ref[Boolean],
      taskStoreSize: Int,
      currentState: State[K, E, V]
  ): UIO[Unit] =
    ZIO.ifM(shutdown.get)(
        // controlled shutdown: this code can be interrupted only before or after message extraction+processing
        currentState.inProgress.traverse_(_._2.interrupt) *>
          //guarantees that cancelHookOpt will be fulfilled
          currentState.cancelInProgress.traverse_(_._2.join) *>
          UIO.foreach_(currentState.taskStore)(_._2.completeHook.traverse_(_.interrupt)),
        for {
          nextMsg   <- q.take
          nextState <- runState(taskStoreSize, q, currentState, nextMsg)
          _         <- runProcessing(q, shutdown, taskStoreSize, nextState)
        } yield ()
    )

  private def runState[K, E, V](taskStoreSize: Int, q: Queue[Msg[K, E, V]], st: State[K, E, V], msg: Msg[K, E, V]): UIO[State[K, E, V]] = msg match {
    case add: Add[K, E, V] =>
      if (st.keyStore.contains(add.key)) add.completeHook.traverse_(_.succeed(Duplicate)).as(st)
      else if (st.taskStore.size == taskStoreSize) add.completeHook.traverse_(_.succeed(Rejected)).as(st)
      else {
        val id = findId(
            st.lastAdddedId,
            v => !st.taskStore.contains(v),
            taskStoreSize,
            "cannot find vacant id in non-full taskStore"
        )

        st.inProgress
          .orElseIO(runTask(id, add, st.inProgress.map(_._2), q).map(t => Some(id -> t)))
          .map(inProg => st.copy(st.taskStore.updated(id, add), st.keyStore.updated(add.key, id), inProg, lastAdddedId = id))
      }

    case Cancel(key, cancelHookOpt, label) =>
      st.keyStore.get(key) match {
        case None => cancelHookOpt.traverse_(_.succeed(NotFound)).as(st)
        case Some(id) =>
          st.cancelInProgress match {
            // when cancelInprogress fiber is completed Completed msg is guaranteed to be in the queue user will always see the proper state
            case Some(`id` -> fiber) => cancelHookOpt.traverse_(_.completeWith(fiber.join)).as(st)
            case _ =>
              st.inProgress match {
                case Some(`id` -> fiber) =>
                  fiber
                    .interrupt
                    .map {
                      case Success(v) => v
                      case e          => throw new AssertionError(s"failure during the interruption of sandboxed IO $e")
                    }
                    .fork
                    .flatMap { interruptFiber =>
                      val interruptFiberWithStatus = interruptFiber.map(toStatus(_, cancelRequestedBy = label))
                      cancelHookOpt
                        .traverse_(_.completeWith(interruptFiberWithStatus.join))
                        .as(st.copy(cancelInProgress = Some(id -> interruptFiberWithStatus), inProgressCancelRequestedBy = label))
                    }
                case _ =>
                  unsafe(st.taskStore(id))("key-id is not in sync")
                    .completeHook
                    .traverse_(_.succeed(Cancelled(label)))
                    .*>(cancelHookOpt.traverse_(_.succeed(Cancelled(label))))
                    .as(st.copy(st.taskStore - id, st.keyStore - key))
              }
          }
      }
    case Join(key, joinHook) =>
      // Dirty hack to save a bit of runtime overhead: hooks are under control of this library and we '_know_' that Duplicate and Reject messages cannot appear at this stage
      val awaitAndTrimForbiddenMsgs: CompleteHook[E, V] => UIO[TaskStatus[E, V]] =
        _.await.flatMap {
          case t @ (Duplicate | Rejected) => IO.dieMessage(s"in progress task is completed with '$t'")
          case h: TaskStatus[E, V]        => IO.succeed(h)
        }

      st.keyStore.get(key) match {
        case None => joinHook.succeed(NotFound).as(st)
        case Some(id) =>
          val add = unsafe(st.taskStore(id))("key-id is not in sync")
          add.completeHook match {
            case Some(completeHook) => joinHook.completeWith(awaitAndTrimForbiddenMsgs(completeHook)).as(st)
            case None =>
              for {
                completeHook <- Promise.make[Nothing, TaskCompletionStatus[E, V]]
                _            <- joinHook.completeWith(awaitAndTrimForbiddenMsgs(completeHook))
              } yield st.copy(taskStore = st.taskStore.updated(id, add.copy(completeHook = Some(completeHook))))
          }
      }

    case GetKeys(p) => p.succeed(st.keyStore.keySet).as(st)

    case Completed(id, key, exit) =>
      st.cancelInProgress.traverse_ {
        case (cid, taskFiber) =>
          assert(cid == id, s"Completed key '$key' does not match key of the task being cancelled '$cid'")
          //guarantees that cancelHookOpt will be fulfilled before fiber gets garbage collected
          taskFiber.join
      } *>
        // Completed message is sent from the inProgress fiber after the task is completed. This fiber should be already completed
        st.inProgress.traverse_(_._2.join) *>
        unsafe(st.taskStore(id))("key-id is not in sync").completeHook.traverse_(_.succeed(toStatus(exit, st.inProgressCancelRequestedBy))) *>
        ZIO
          .optional(st.taskStore.size > 1) {
            val nextId = findId(
                id,
                st.taskStore.contains,
                taskStoreSize,
                "cannot find next task in nonempty taskstore"
            )
            runTask(nextId, st.taskStore(nextId), st.inProgress.map(_._2), q).map(nextId -> _)
          }
          .map(inProg => st.copy(st.taskStore - id, st.keyStore - key, inProg, None, None))
  }

  private def toStatus[E, V](e: Exit[E, V], cancelRequestedBy: Option[String]): TaskStatus[E, V] = e match {
    case Success(v) => Done(v)
    case Failure(cause) =>
      if (cause.interrupted) Cancelled(cancelRequestedBy)
      else if (cause.failed) Failed(cause.failures)
      else Died(cause.defects)
  }

  private def findId(startId: Int, f: Int => Boolean, taskStoreSize: Int, msg: => String): Int = {
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

  private def runTask[K, E, V](
      id: Int,
      add: Add[K, E, V],
      inProgress: Option[Fiber[Nothing, Exit[E, V]]],
      q: Queue[Msg[K, E, V]]
  ): UIO[Fiber[Nothing, Exit[E, V]]] =
    IO.whenM(inProgress.fold(IO.succeed(false))(_.poll.map(_.isEmpty)))(
        IO.dieMessage("trying to start new task when something is in progress")
    ) *>
      add.task
      // making task itself interruptible while all surrounding is uninterruptible
        .interruptible
        .run
        .tap(exit => q.offer(Completed(id, add.key, exit)))
        .fork

}
