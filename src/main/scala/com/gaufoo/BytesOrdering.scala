package com.gaufoo

object BytesOrdering {
  implicit val o: Ordering[Array[Byte]] =
    new Ordering[Array[Byte]] {
      def compare(a: Array[Byte], b: Array[Byte]): Int =
        if (a eq null)
          if (b eq null) 0
          else -1
        else if (b eq null) 1
        else {
          val L = math.min(a.length, b.length)
          var i = 0
          while (i < L) {
            if (a(i) < b(i)) return -1
            else if (b(i) < a(i)) return 1
            i += 1
          }
          if (L < b.length) -1
          else if (L < a.length) 1
          else 0
        }
    }
}
