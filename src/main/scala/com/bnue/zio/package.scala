package com.bnue

import _root_.zio._

package object zio {
  def cond[R, E, V](b1: ZIO[R, E, Boolean])(_true: ZIO[R, E, V], _false:ZIO[R, E, V]): ZIO[R, E, V] = b1.flatMap(b1 => if(b1)_true else _false)
  def and[R, E](b1: ZIO[R, E, Boolean], b2: ZIO[R,E, Boolean]):ZIO[R, E, Boolean] = b1.flatMap(b1 => if(b1) b2 else UIO.succeed(false))
}
