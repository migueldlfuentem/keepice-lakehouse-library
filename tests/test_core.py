from keepice_lakehouse_library import compute


def test_compute():
    assert compute(["a", "bc", "abc"]) == "abc"
