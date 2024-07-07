import subprocess


def test_main():
    assert subprocess.check_output(["keepice-lakehouse-library", "foo", "foobar"], text=True) == "foobar\n"
