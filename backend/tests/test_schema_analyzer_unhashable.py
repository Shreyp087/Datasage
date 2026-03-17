import pandas as pd

from app.pipeline.steps.schema_analyzer import SchemaAnalyzer


def test_schema_analyzer_handles_unhashable_dict_cells() -> None:
    df = pd.DataFrame(
        {
            "payload": [{"a": 1}, {"a": 2}, None, {"b": [1, 2]}],
            "row_id": [101, 102, 103, 104],
        }
    )

    class DummyContext:
        job_id = "job-1"
        domain = "general"
        schema = {}
        warnings = []

    ctx = DummyContext()
    step = SchemaAnalyzer()
    result = step.run(df.copy(), ctx)

    assert result.df is not None
    assert "payload" in ctx.schema
    assert ctx.schema["row_id"] == "id_col"

