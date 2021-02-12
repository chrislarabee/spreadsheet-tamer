from pathlib import Path

import pytest

from tamer import schema as sc


class TestColumn:
    def test_that_it_handles_mixing_valid_and_invalid_as_expected(self):
        c = sc.Column(int, valid_values=[1, 2, 3], invalid_values=[4, 5, 6])
        assert c.valid_values == [1, 2, 3]
        assert c.invalid_values == []
        c = sc.Column(str, valid_patterns=[r"\D"], invalid_patterns=[r"\d"])
        assert c.valid_patterns == [r"\D"]
        assert c.invalid_patterns == []
        c = sc.Column(
            str, valid_values=["a"], invalid_patterns=[r"\d"], invalid_values=["a"]
        )
        assert c.valid_values == ["a"]
        assert c.invalid_patterns == []
        assert c.invalid_values == []
        c = sc.Column(
            str, valid_patterns=[r"\D"], invalid_patterns=[r"\d"], invalid_values=["a"]
        )
        assert c.valid_patterns == [r"\D"]
        assert c.invalid_patterns == []
        assert c.invalid_values == []

    class TestEvaluate:
        def test_that_it_works_with_only_data_type_constraint(self):
            c = sc.Column(int)
            assert c.evaluate(1)
            assert not c.evaluate("1")

        def test_that_it_works_with_valid_values(self):
            c = sc.Column(int, valid_values=[1, 2, 3])
            assert c.evaluate(1)
            assert not c.evaluate(4)
            c = sc.Column(str, valid_values=["a", "b", "c"])
            assert c.evaluate("a")
            assert not c.evaluate("bar")

        def test_that_it_works_with_invalid_values(self):
            c = sc.Column(int, invalid_values=[1, 2, 3])
            assert c.evaluate(4)
            assert not c.evaluate(1)
            c = sc.Column(str, invalid_values=["a", "b", "c"])
            assert c.evaluate("d")
            assert not c.evaluate("a")

        def test_that_it_works_with_valid_patterns(self):
            c = sc.Column(str, valid_patterns=[r"^S$", r"^M$", r"^L$", r"^X+L$"])
            assert c.evaluate("S")
            assert not c.evaluate("Small")
            assert c.evaluate("XL")
            assert c.evaluate("XXXL")
            assert not c.evaluate("-XL")

        def test_that_it_works_with_invalid_patterns(self):
            c = sc.Column(str, invalid_patterns=[r"\d", r"\s"])
            assert c.evaluate("latte")
            assert not c.evaluate("no3lle")
            assert not c.evaluate("what now")

        def test_that_it_works_with_valid_values_and_patterns(self):
            pass

        def test_that_it_works_with_invalid_values_and_patterns(self):
            pass


class TestSchema:
    def test_that_type_hints_cause_no_errors(self):
        s = sc.Schema(
            a=sc.Column(str),
            b=sc.Column(int),
        )

    def test_that_from_yaml_loads_successfully(self):
        s = sc.Schema.from_yaml(Path("tests/samples/schema.yml"))
        assert list(s.columns.keys()) == ["a", "b"]
        assert s.columns["a"].data_type == "int"
        assert s.columns["a"].valid_values == [1, 2, 3]
        assert s.columns["a"].invalid_values == []
        assert not s.columns["a"].required
        assert s.columns["a"].unique
        assert s.columns["b"].data_type == "str"
        assert s.columns["b"].valid_values == []
        assert s.columns["b"].invalid_values == []
        assert s.columns["b"].required
        assert not s.columns["b"].unique
