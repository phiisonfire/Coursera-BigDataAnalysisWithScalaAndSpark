val vs = List(
  (1, 100),
  (2, 80),
  (3, 70),
  (2, 90),
  (1, 110),
  (1, 200)
)
vs
  .groupBy((langIndex, _) => langIndex)
  .map((langIndex, pairs) => (langIndex, pairs.size))
  .foldLeft((0, 0)){ case ((lang1, c1), (lang2, c2)) =>
    if (c1 < c2) (lang2, c2) else (lang1, c1)
  }
vs(0)