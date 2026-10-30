"""Evaluator unit tests."""

import numpy as np
import pytest

from canopy.contracts.sample import AlarmSeverity
from canopy.fox.services.parser import Parser
from canopy.fox.services.evaluator import (
    evaluate_array,
    worst_severity,
    evaluate_scalar,
    worst_severity_arrays,
)


@pytest.fixture
def parser():
    return Parser()


class TestEvaluateScalar:
    async def test_simple_addition(self, parser):
        parsed = parser.parse("{a} + {b}", {"a": "PV:A", "b": "PV:B"})
        result = await evaluate_scalar(parsed, {"__v0": 5.0, "__v1": 3.0}, timeout_s=1.0)
        assert result == 8.0

    async def test_sqrt(self, parser):
        parsed = parser.parse("sqrt({x})", {"x": "PV:X"})
        result = await evaluate_scalar(parsed, {"__v0": 9.0}, timeout_s=1.0)
        assert result == 3.0

    async def test_boolean_result(self, parser):
        parsed = parser.parse("{a} > {b}", {"a": "PV:A", "b": "PV:B"})
        result = await evaluate_scalar(parsed, {"__v0": 10.0, "__v1": 5.0}, timeout_s=1.0)
        assert result is True

    @pytest.mark.parametrize(
        ("expression", "value", "expected"),
        [
            ("{a} & 3", 6.9, 2.0),
            ("{a} | 3", 4.9, 7.0),
            ("{a} ^ 3", 6.9, 5.0),
            ("~{a}", 1.9, -2.0),
            ("{a} << 2", 3.9, 12.0),
            ("{a} >> 2", 8.9, 2.0),
        ],
    )
    async def test_bitwise_operators_use_integer_operands(self, parser, expression, value, expected):
        """Evaluate each bitwise operator after truncating operands to integers."""
        parsed = parser.parse(expression, {"a": "PV:A"})
        result = await evaluate_scalar(parsed, {"__v0": value}, timeout_s=1.0)
        assert result == expected

    async def test_epics_comparison_precedence(self, parser):
        """Ensure comparisons bind before bitwise operations as in EPICS Calc."""
        parsed = parser.parse("{a} & 4 == 4", {"a": "PV:A"})
        result = await evaluate_scalar(parsed, {"__v0": 4.0}, timeout_s=1.0)
        assert result == 0.0

    async def test_explicit_parentheses_override_epics_precedence(self, parser):
        """Ensure explicit grouping can put bitwise evaluation before comparison."""
        parsed = parser.parse("({a} & 4) == 4", {"a": "PV:A"})
        result = await evaluate_scalar(parsed, {"__v0": 4.0}, timeout_s=1.0)
        assert result is True

    @pytest.mark.parametrize("expression", ["{a} & 1", "~{a}"])
    async def test_bitwise_rejects_nonfinite_operands(self, parser, expression):
        """Reject NaN operands in binary and unary bitwise expressions."""
        from canopy.fox.errors import FormulaTypeError

        parsed = parser.parse(expression, {"a": "PV:A"})
        with pytest.raises(FormulaTypeError, match="finite"):
            await evaluate_scalar(parsed, {"__v0": float("nan")}, timeout_s=1.0)

    @pytest.mark.parametrize("shift", [-1, 64])
    async def test_bitwise_rejects_out_of_range_shift_counts(self, parser, shift):
        """Reject negative and oversized scalar bit-shift counts."""
        from canopy.fox.errors import FormulaTypeError

        parsed = parser.parse(f"{{a}} << {shift}", {"a": "PV:A"})
        with pytest.raises(FormulaTypeError, match="shift counts"):
            await evaluate_scalar(parsed, {"__v0": 1.0}, timeout_s=1.0)

    @pytest.mark.parametrize(
        ("expression", "value", "expected"),
        [
            ("float({a})", 3, 3.0),
            ("int({a})", 3.9, 3),
            ("str({a})", 3.5, "3.5"),
            ("isnan({a})", float("nan"), True),
            ("np.isnan({a})", float("nan"), True),
        ],
    )
    async def test_conversion_and_nan_functions(self, parser, expression, value, expected):
        """Evaluate scalar conversion functions and both supported isnan spellings."""
        parsed = parser.parse(expression, {"a": "PV:A"})
        result = await evaluate_scalar(parsed, {"__v0": value}, timeout_s=1.0)
        assert result == expected


class TestEvaluateArray:
    async def test_numexpr_path(self, parser):
        parsed = parser.parse("{a} + {b}", {"a": "PV:A", "b": "PV:B"})
        assert parsed.is_numexpr_compatible
        a = np.array([1.0, 2.0, 3.0])
        b = np.array([4.0, 5.0, 6.0])
        result = await evaluate_array(parsed, {"__v0": a, "__v1": b}, timeout_s=1.0)
        np.testing.assert_array_equal(result, [5.0, 7.0, 9.0])

    async def test_asteval_fallback(self, parser):
        parsed = parser.parse("mean({a})", {"a": "PV:A"})
        assert not parsed.is_numexpr_compatible
        a = np.array([1.0, 2.0, 3.0])
        result = await evaluate_array(parsed, {"__v0": a}, timeout_s=1.0)
        assert float(result) == 2.0

    async def test_shape_mismatch_maps_to_type_error(self, parser):
        # numexpr raises a bare ValueError on a broadcast mismatch; it must surface
        # as a typed FormulaTypeError (422), not a generic 500.
        from canopy.fox.errors import FormulaTypeError

        parsed = parser.parse("{a} + {b}", {"a": "PV:A", "b": "PV:B"})
        with pytest.raises(FormulaTypeError):
            await evaluate_array(
                parsed, {"__v0": np.array([1.0, 2.0, 3.0]), "__v1": np.array([1.0, 2.0])}, timeout_s=1.0
            )

    async def test_multiply(self, parser):
        parsed = parser.parse("{a} * {b}", {"a": "PV:A", "b": "PV:B"})
        a = np.array([1.0, 2.0, 3.0])
        b = np.array([4.0, 5.0, 6.0])
        result = await evaluate_array(parsed, {"__v0": a, "__v1": b}, timeout_s=1.0)
        np.testing.assert_array_equal(result, [4.0, 10.0, 18.0])

    @pytest.mark.parametrize(
        ("expression", "expected"),
        [
            ("{a} & 3", [0.0, 1.0, 2.0]),
            ("{a} | 3", [3.0, 3.0, 3.0]),
            ("{a} ^ 3", [3.0, 2.0, 1.0]),
            ("~{a}", [-1.0, -2.0, -3.0]),
            ("{a} << 1", [0.0, 2.0, 4.0]),
            ("{a} >> 1", [0.0, 0.0, 1.0]),
            ("int({a})", [0.0, 1.0, 2.0]),
            ("float({a})", [0.5, 1.5, 2.5]),
            ("isnan({a})", [False, False, False]),
        ],
    )
    async def test_bitwise_and_conversion_functions_on_arrays(self, parser, expression, expected):
        """Apply bitwise operators and numeric conversions element-wise to arrays."""
        parsed = parser.parse(expression, {"a": "PV:A"})
        result = await evaluate_array(parsed, {"__v0": np.array([0.5, 1.5, 2.5])}, timeout_s=1.0)
        np.testing.assert_array_equal(result, expected)

    async def test_int_array_rejects_nonfinite_values(self, parser):
        """Reject non-finite elements when converting an array to integers."""
        from canopy.fox.errors import FormulaTypeError

        parsed = parser.parse("int({a})", {"a": "PV:A"})
        with pytest.raises(FormulaTypeError, match="NaN or infinity"):
            await evaluate_array(parsed, {"__v0": np.array([1.0, np.nan])}, timeout_s=1.0)

    @pytest.mark.parametrize(
        ("values", "message"),
        [
            ([1.0, np.inf], "finite"),
            ([1.0, float(2**63)], "signed 64-bit"),
        ],
    )
    async def test_bitwise_array_rejects_invalid_integer_operands(self, parser, values, message):
        """Reject non-finite and out-of-range values in bitwise array operands."""
        from canopy.fox.errors import FormulaTypeError

        parsed = parser.parse("{a} & 1", {"a": "PV:A"})
        with pytest.raises(FormulaTypeError, match=message):
            await evaluate_array(parsed, {"__v0": np.array(values)}, timeout_s=1.0)

    async def test_int_array_rejects_values_outside_int64(self, parser):
        """Reject array integer conversions that exceed the signed 64-bit range."""
        from canopy.fox.errors import FormulaTypeError

        parsed = parser.parse("int({a})", {"a": "PV:A"})
        with pytest.raises(FormulaTypeError, match="signed 64-bit"):
            await evaluate_array(parsed, {"__v0": np.array([float(2**63)])}, timeout_s=1.0)

    async def test_str_array_is_rejected(self, parser):
        """Reject string conversion on arrays because string results are scalar-only."""
        from canopy.fox.errors import FormulaTypeError

        parsed = parser.parse("str({a})", {"a": "PV:A"})
        with pytest.raises(FormulaTypeError, match="scalar"):
            await evaluate_array(parsed, {"__v0": np.array([1.0, 2.0])}, timeout_s=1.0)


class TestSeverityPropagation:
    def test_worst_severity_basic(self):
        assert worst_severity([AlarmSeverity.NO_ALARM, AlarmSeverity.MINOR]) == AlarmSeverity.MINOR
        assert worst_severity([AlarmSeverity.MINOR, AlarmSeverity.MAJOR]) == AlarmSeverity.MAJOR
        assert worst_severity([AlarmSeverity.NO_ALARM]) == AlarmSeverity.NO_ALARM
        assert worst_severity([AlarmSeverity.INVALID, AlarmSeverity.NO_ALARM]) == AlarmSeverity.INVALID

    def test_worst_severity_arrays(self):
        a = np.array([0, 1, 0])  # NO_ALARM, MINOR, NO_ALARM
        b = np.array([0, 0, 2])  # NO_ALARM, NO_ALARM, MAJOR
        result = worst_severity_arrays([a, b])
        assert result == [AlarmSeverity.NO_ALARM, AlarmSeverity.MINOR, AlarmSeverity.MAJOR]


class TestScalarStringOutput:
    async def test_ternary_returns_string(self):
        from canopy.fox.services.parser import Parser

        parser = Parser()
        parsed = parser.parse("'high' if {a} > 100 else 'low'", {"a": "PV:A"})
        result = await evaluate_scalar(parsed, {"__v0": 150}, timeout_s=1.0)
        assert result == "high"

    async def test_nested_ternary_returns_string(self):
        from canopy.fox.services.parser import Parser

        parser = Parser()
        parsed = parser.parse(
            "'red' if {sev} > 1 else ('orange' if {x} > 10 else 'green')",
            {"sev": "PV:SEV", "x": "PV:X"},
        )
        result = await evaluate_scalar(parsed, {"__v0": 0, "__v1": 20}, timeout_s=1.0)
        assert result == "orange"

    async def test_boolean_operators_in_scalar(self):
        from canopy.fox.services.parser import Parser

        parser = Parser()
        parsed = parser.parse("{a} > 0 && {b} > 0", {"a": "PV:A", "b": "PV:B"})
        result = await evaluate_scalar(parsed, {"__v0": 5, "__v1": 3}, timeout_s=1.0)
        assert result is True
