package org.hammerlab.guacamole.readsets

import org.hammerlab.guacamole.loci.set.LociSet
import org.hammerlab.guacamole.reference.ReferencePosition
import org.hammerlab.guacamole.util.GuacFunSuite

class WindowIteratorSuite extends GuacFunSuite with Util {

  def checkReads(
    halfWindowSize: Int,
    lociStr: String,
    reads: Iterator[TestRegion],
    fromOpt: Option[ReferencePosition] = None,
    untilOpt: Option[ReferencePosition] = None
  )(
    expected: ((String, Int), String)*
  ): Unit = {

    val it =
      new WindowIterator(
        halfWindowSize,
        fromOpt,
        untilOpt,
        LociSet(lociStr),
        reads.buffered
      )

    checkReads(it, expected.toMap)
  }

  test("bounds") {
    checkReads(
      2,
      "chr1:0-200",
      makeReads(
        ("chr1", 100, 105, 1),
        ("chr1", 101, 106, 1)
      ),
      untilOpt = Some(ReferencePosition("chr1", 108))
    )(
      ("chr1",  98) -> "[100,105)",
      ("chr1",  99) -> "[100,105), [101,106)",
      ("chr1", 100) -> "[100,105), [101,106)",
      ("chr1", 101) -> "[100,105), [101,106)",
      ("chr1", 102) -> "[100,105), [101,106)",
      ("chr1", 103) -> "[100,105), [101,106)",
      ("chr1", 104) -> "[100,105), [101,106)",
      ("chr1", 105) -> "[100,105), [101,106)",
      ("chr1", 106) -> "[100,105), [101,106)",
      ("chr1", 107) -> "[101,106)"
    )
  }

  test("hello world") {
    checkReads(
      1,
      List(
        "chr1:50-52",
        "chr1:98-102",
        "chr1:199-203",
        "chr2:10-12",
        "chr2:100-102",
        "chr4:10-12",
        "chr5:100-102"
      ).mkString(","),
      makeReads(
        ("chr1", 100, 200,  1),
        ("chr1", 101, 201,  1),
        ("chr2",   8,   9,  1),
        ("chr2",  13,  15,  1),
        ("chr2",  90, 100,  1),
        ("chr2", 102, 105,  1),
        ("chr2", 103, 106,  1),
        ("chr2", 110, 120,  1),
        ("chr3", 100, 200,  1),
        ("chr5",  90, 110, 10)
      )
    )(
      ("chr1",  50) -> "",
      ("chr1",  51) -> "",
      ("chr1",  98) -> "",
      ("chr1",  99) -> "[100,200)",
      ("chr1", 100) -> "[100,200), [101,201)",
      ("chr1", 101) -> "[100,200), [101,201)",
      ("chr1", 199) -> "[100,200), [101,201)",
      ("chr1", 200) -> "[100,200), [101,201)",
      ("chr1", 201) -> "[101,201)",
      ("chr1", 202) -> "",
      ("chr2",  10) -> "",
      ("chr2",  11) -> "",
      ("chr2", 100) -> "[90,100)",
      ("chr2", 101) -> "[102,105)",
      ("chr5", 100) -> "[90,110)*10",
      ("chr5", 101) -> "[90,110)*10"
    )
  }

}
