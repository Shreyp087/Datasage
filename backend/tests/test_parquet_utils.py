from datetime import datetime, timezone

import pandas as pd

from app.utils.parquet import prepare_dataframe_for_parquet


def test_prepare_dataframe_for_parquet_handles_mixed_object_datetime(tmp_path):
    df = pd.DataFrame(
        {
            "most_recent_activity": [
                datetime(2026, 2, 23, 10, 21, 3, tzinfo=timezone.utc),
                "2026-02-24T12:00:00Z",
                None,
            ],
            "payload": [{"k": "v"}, {"k": 2}, None],
            "blob": [b"abc", b"xyz", None],
        }
    )

    prepared = prepare_dataframe_for_parquet(df)

    assert isinstance(prepared.loc[0, "most_recent_activity"], str)
    assert isinstance(prepared.loc[0, "payload"], str)
    assert isinstance(prepared.loc[0, "blob"], str)

    out = tmp_path / "test.parquet"
    prepared.to_parquet(out, index=False)
    assert out.exists()
