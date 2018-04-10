package com.futurice.iodf.util

import java.io.Closeable

import com.futurice.iodf.{IoScope, Utils}

trait ResourceLSeq[+T] extends Closeable {
  def open(i:Long) : T
  def apply(i:Long)(implicit bind:IoScope) = bind(open(i))
  def lsize : Long
}

trait RefLSeq[+T] extends ResourceLSeq[Ref[T]] {}

/* long sequence interface */
trait LSeq[+T] extends Iterable[T] with PartialFunction[Long, T] with Closeable {
  def close = {}
  def apply(l:Long) : T
  def lsize : Long
  override def size = lsize.toInt
  def isDefinedAt(l:Long) = l >= 0 && l < size

  // FIXME: should this be openView?
  def view(from:Long, until:Long) : LSeq[T] = {
    val self = this
    new LSeq[T] {
      def lsize = until - from
      def apply(l:Long) = self.apply(l+from)
    }
  }

  /**
   * TODO: rename this, lazy implies cacheing, while we don't cache anything
   */
  def lazyMap[B](f: T => B) : LSeq[B] = map[B](f)
  def map[B](f: T => B) : LSeq[B] = {
    val self = this
    new LSeq[B] {
      def apply(l:Long) = f(self.apply(l))
      def lsize : Long = self.lsize
      override def iterator : Iterator[B] = self.iterator.map(f)
      override def close = self.close
    }
  }
  def zipWithIndex : LSeq[(T, Long)] = {
    val self = this
    new LSeq[(T, Long)] {
      override def apply(l: Long) = (self(l), l)
      override def lsize = self.lsize

      override def iterator : Iterator[(T, Long)] = new Iterator[(T, Long)] {
        val i = self.iterator
        var at = 0L
        override def hasNext = i.hasNext
        override def next() = {
          val rv = (i.next(), at)
          at += 1
          rv
        }
      }
    }
  }

  def zip[E](b:LSeq[E]) = {
    val a = this
    new LSeq[(T, E)] {
      override def apply(l: Long) = (a.apply(l), b.apply(l))
      override def lsize = a.lsize
      override def iterator = new Iterator[(T, E)] {
        val ai = a.iterator
        val bi = b.iterator
        override def hasNext = ai.hasNext && bi.hasNext
        override def next() = (ai.next, bi.next)
      }
    }
  }

  // TODO, FIXME: remplement all Seq APIs

  override def drop(i:Int) = drop(i.toLong)
  def drop(i:Long) = view(i, size)

  def iterator : Iterator[T] = new Iterator[T] {
    var i = 0L
    override def hasNext: Boolean = {
      i < lsize
    }
    override def next(): T = {
      val rv = apply(i)
      i += 1
      rv
    }
  }

  def select(indexes:LSeq[Long]) = {
    val self = this
    indexes.map { i : Long => self(i) }
  }


  def openSelectSome(indexes:LSeq[Option[Long]]) = {
    val self = this
    indexes.map { e : Option[Long] => e.map { index =>
        self(index)
      }
    }
  }

  def ++[B >: T](b:LSeq[B]) = {
    val a = this
    new LSeq[B] {
      override def apply(l: Long) =
        if (l < a.size) a(l) else b(l - a.size)
      override def lsize = a.size + b.size
    }
  }
  override def toString =
    f"LSeq(" + (if (lsize < 8) this.mkString(", ") else this.take(8).mkString(", ") + "..") + ")"

  def bind(closer:Closeable) = {
    val self = this
    new LSeq[T] {
      override def apply(l: Long) = self.apply(l)

      override def lsize = self.lsize

      override def iterator = self.iterator

      override def close = {
        self.close
        closer.close

      }
    }
  }

}

object LSeq {
  def fill[T](n:Long, t:T) = new LSeq[T] {
      override def apply(l: Long): T = t
      override def lsize: Long = n
  }
  def empty[T] = new LSeq[T] {
    override def apply(l:Long) = throw new IndexOutOfBoundsException(f"this sequence is empty")
    override def lsize = 0
  }
  def apply[T](v:T*) = from[T](v)
  def from[T](v:Seq[T]) = new LSeq[T] {
    override def apply(l: Long): T = v(l.toInt)
    override def lsize: Long = v.size
    override def iterator = v.iterator
  }
  def from[T](v:Array[T]) = new LSeq[T] {
    override def apply(l: Long): T = v(l.toInt)
    override def lsize: Long = v.size
    override def iterator = v.iterator
  }
}


trait OptionLSeq[T] extends LSeq[Option[T]] {
  def defined : LBits
  def definedStates : LSeq[T]
  def definedStatesWithIndex : LSeq[(T, Long)]

  def lazyMapDefinedStates[B](f : T => B) = {
    OptionLSeq.from(defined, definedStates.lazyMap(f))
  }

  override def bind(closer:Closeable) = {
    val self = this
    new OptionLSeq[T] {
      override def apply(l: Long) = self.apply(l)
      override def lsize = self.lsize
      override def iterator = self.iterator
      override def close = {
        self.close
        closer.close

      }
      override def defined = self.defined
      override def definedStates = self.definedStates
      override def definedStatesWithIndex =self.definedStatesWithIndex
    }
  }

}

object OptionLSeq {
  def mask[T](mask:LBits, values:LSeq[T]) =
    new OptionLSeq[T] {
      override lazy val defined = mask
      lazy val definedIndexes = mask.trues
      override lazy val definedStates = definedIndexes.lazyMap { values(_) }
      override def definedStatesWithIndex = definedIndexes.lazyMap { i =>
        (values(i), i)
      }
      override def apply(l: Long) = {
        mask(l) match {
          case true => Some(values(l))
          case false => None
        }
      }
      override def lsize = mask.lsize
    }
  def from[T](opt:Seq[Option[T]]) = {
    new OptionLSeq[T] {
      override lazy val defined = LBits.from(opt.map(_.isDefined).toArray)
      lazy val definedIndexes = defined.trues
      override lazy val definedStates = definedIndexes.lazyMap { i => opt(i.toInt).get }
      override def definedStatesWithIndex = definedIndexes.lazyMap { i =>
        (opt(i.toInt).get, i)
      }
      override def apply(l: Long) = opt(l.toInt)
      override def lsize = opt.size
    }
  }
  def from[T](_defined:LBits, _definedValues:LSeq[T]) =
    new OptionLSeq[T] {
      override lazy val defined = _defined
      lazy val definedIndexes = _defined.trues
      override lazy val definedStates = _definedValues
      override def definedStatesWithIndex = _definedValues zip _defined.trues
      override def apply(l: Long) = {
        _definedValues(l) match {
          case true => Some(_definedValues(_defined.trues(l)))
          case false => None
        }
      }
      override def lsize = _defined.lsize
    }
}

trait ScannableLSeq[T, Key] extends Scannable[T, Key] with LSeq[T] {
  override def iterator : Scanner[T, Key] = throw new NotImplementedError()
}