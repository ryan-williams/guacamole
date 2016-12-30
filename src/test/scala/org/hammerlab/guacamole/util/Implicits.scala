package org.hammerlab.guacamole.util

trait Implicits {
  implicit def liftImplicitTuple2[A, B, A1, B1](t: (A, B))
                                               (implicit f1: A => A1, f2: B => B1): (A1, B1) =
    (f1(t._1), f2(t._2))

  implicit def liftImplicitTuple3[A, B, C, A1, B1, C1](t: (A, B, C))
                                                      (implicit f1: A => A1, f2: B => B1, f3: C => C1): (A1, B1, C1) =
    (f1(t._1), f2(t._2), f3(t._3))

  implicit def liftImplicitTuple4[A, B, C, D, A1, B1, C1, D1](t: (A, B, C, D))
                                                             (implicit f1: A => A1, f2: B => B1, f3: C => C1, f4: D ⇒ D1): (A1, B1, C1, D1) =
    (f1(t._1), f2(t._2), f3(t._3), f4(t._4))

  implicit def convertSeq[T, U](s: Seq[T])(implicit f: T ⇒ U): Vector[U] = s.map(f).toVector

  implicit def liftMap[K, V, K1, V1](m: Map[K, V])(implicit fk: K ⇒ K1, fv: V ⇒ V1): Map[K1, V1] =
    m.map(t ⇒ (fk(t._1), fv(t._2)))

  implicit def convertOpt[T, U](o: Option[T])(implicit f: T ⇒ U): Option[U] = o.map(f)
}
