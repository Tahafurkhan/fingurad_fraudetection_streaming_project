"""Pure, testable transformation logic shared by the streaming models.

WHY THIS PACKAGE EXISTS
-----------------------
The Lakeflow models are not testable in place. Each model is a decorated
function that calls `spark.readStream.table(...)` on its first line, so the
transformation and its source are welded together: there is no way to hand it a
DataFrame of fixtures and assert on the result.

This package holds the same logic as plain functions of
`(DataFrame) -> DataFrame`. The models call these; the tests call these. There
is exactly one implementation.

THE CONSTRAINT THAT SHAPES EVERYTHING HERE
------------------------------------------
Lakeflow loads each pipeline file with `exec()` into a uniquely-named synthetic
module. Those files therefore cannot import one another -- there is no stable
module name to import, and `__file__` does not exist. That is why the payload
schema was previously duplicated across three files with a comment explaining
the duplication.

Files under `src/pipelines/transforms/` are NOT pipeline files. They are never
exec'd by Lakeflow; they are ordinary Python modules that ship alongside the
pipeline and are imported normally at runtime. The distinction is what makes a
single source of truth possible without fighting the execution model.

WHAT BELONGS HERE
-----------------
Logic that is a pure function of its input DataFrame. What does NOT belong:
table reads, table writes, `@dp.table` decorators, expectations, or anything
touching a catalog. Those stay in the model files, where Lakeflow can see them.
"""

from __future__ import annotations

__all__ = ["transactions"]
