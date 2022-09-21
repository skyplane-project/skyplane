from skyplane.utils import imports


def test_inject():
    @imports.inject("os", "sys")
    def example_fn(os, sys):
        assert os is not None
        assert sys is not None
        assert os.path.exists(sys.executable)

    example_fn()
