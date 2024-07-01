from typing import Any

import narwhals as nw

data = {
    "a": [1.0, 2.0, None, 4.0],
    "b": [None, 3.0, None, 5.0],
}


def test_drop_nulls(constructor: Any) -> None:
    result = len(nw.from_native(constructor(data)))
    assert result == 4
