from pathlib import Path

from tamer import schema as sc


class TestColumn:
    class TestEvaluate:
        def test_that_it_works_with_only_data_type_constraint(self):
            c = sc.Column(int)
            assert c.evaluate(1)
            assert not c.evaluate("1")

        def test_that_it_works_with_valid_values(self):
            c = sc.Column(int, valid_values=[1, 2, 3])
            assert c.evaluate(1)
            assert not c.evaluate(4)
            c = sc.Column(str, valid_values=[r"^S$", r"^M$", r"^L$", r"^X+L$"])
            assert c.evaluate("S")
            assert not c.evaluate("Small")
            assert c.evaluate("XL")
            assert c.evaluate("XXXL")
            assert not c.evaluate("-XL")

    class TestValidate:
        def test_that_it_works_with_strings(self):
            assert sc.Column._validate("a", "a")
            assert not sc.Column._validate("a", "b")
            assert sc.Column._validate("a", "natter")
            assert not sc.Column._validate("a", "lurk")
        
        def test_that_it_works_with_regex(self):
            assert sc.Column._validate(r"size\d", "part_size1")
            assert not sc.Column._validate(r"size\d", "part_size")
            assert sc.Column._validate(r"^size\d", "size1")
            assert not sc.Column._validate(r"^size\d", "part_size1")


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
