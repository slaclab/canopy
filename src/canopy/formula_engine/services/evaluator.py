"""Core evaluation dispatch: numexpr for arrays, asteval for scalars.

All compute is CPU-bound and runs via ``asyncio.to_thread`` to avoid blocking
the event loop. Timeout is enforced via ``asyncio.wait_for``.
"""

import re
import ast
import asyncio
from typing import TYPE_CHECKING
from functools import wraps
from collections.abc import Mapping, Iterable

import numpy as np
import numexpr as ne
from asteval import Interpreter

from canopy.contracts.sample import AlarmSeverity
from canopy.formula_engine.errors import (
    FormulaTypeError,
    FormulaEngineError,
    FormulaEvalTimeout,
    FormulaInputOutage,
)
from canopy.formula_engine.services.parser import ParsedFormula
from canopy.formula_engine.language.namespace import (
    NUMEXPR_NAME_MAP,
    build_eval_namespace,
)

if TYPE_CHECKING:
    from canopy.contracts.formula import FormulaSpec
    from canopy.formula_engine.ports import DataSource

_SEVERITY_ORDER = [AlarmSeverity.NO_ALARM, AlarmSeverity.MINOR, AlarmSeverity.MAJOR, AlarmSeverity.INVALID]
_BITWISE_HELPERS = {
    ast.BitAnd: "_bitwise_bitand",
    ast.BitOr: "_bitwise_bitor",
    ast.BitXor: "_bitwise_bitxor",
    ast.LShift: "_bitwise_lshift",
    ast.RShift: "_bitwise_rshift",
}
_ARRAY_BITWISE_OPERATORS = {
    int.__and__: np.bitwise_and,
    int.__or__: np.bitwise_or,
    int.__xor__: np.bitwise_xor,
    int.__lshift__: np.left_shift,
    int.__rshift__: np.right_shift,
    int.__invert__: np.invert,
}
_NAN_REDUCTIONS = {"mean", "sum", "min", "max", "std", "var", "ptp", "count_nonzero"}


class _BitwiseTransformer(ast.NodeTransformer):
    """Route bitwise and supported NumPy aliases through integer coercion."""

    def visit_Call(self, node: ast.Call) -> ast.AST:
        self.generic_visit(node)
        if (
            isinstance(node.func, ast.Attribute)
            and isinstance(node.func.value, ast.Name)
            and node.func.value.id == "np"
            and node.func.attr in {"float", "int", "str", "isnan", "where", *_NAN_REDUCTIONS}
        ):
            return ast.copy_location(
                ast.Call(func=ast.Name(id=node.func.attr, ctx=ast.Load()), args=node.args, keywords=node.keywords),
                node,
            )
        return node

    def visit_Compare(self, node: ast.Compare) -> ast.AST:
        self.generic_visit(node)
        if len(node.ops) != 1:
            return node
        return ast.copy_location(
            ast.Call(
                func=ast.Name(id="_bitwise_comparison", ctx=ast.Load()),
                args=[node, ast.Constant(value=isinstance(node.ops[0], ast.NotEq))],
                keywords=[],
            ),
            node,
        )

    def visit_BinOp(self, node: ast.BinOp) -> ast.AST:
        self.generic_visit(node)
        helper = _BITWISE_HELPERS.get(type(node.op))
        if helper is None:
            return node
        call = ast.Call(
            func=ast.Name(id=helper, ctx=ast.Load()),
            args=[node.left, node.right],
            keywords=[],
        )
        return ast.copy_location(call, node)

    def visit_UnaryOp(self, node: ast.UnaryOp) -> ast.AST:
        self.generic_visit(node)
        if not isinstance(node.op, ast.Invert):
            return node
        call = ast.Call(
            func=ast.Name(id="_bitwise_invert", ctx=ast.Load()),
            args=[node.operand],
            keywords=[],
        )
        return ast.copy_location(call, node)


def _bitwise_integer(value: object) -> int | np.ndarray:
    """Coerce a scalar or array operand to signed 64-bit integers.

    Array NaN values remain NaN at their original positions. Scalar NaN values
    are rejected because Python integer conversion does not define a missing
    scalar integer.
    """
    if isinstance(value, np.ndarray):
        data = np.ma.getdata(value)
        present = ~np.ma.getmaskarray(value)
        if np.issubdtype(data.dtype, np.integer) or np.issubdtype(data.dtype, np.bool_):
            integers = data[present].astype(object)
            if np.any(integers < -(2**63)) or np.any(integers >= 2**63):
                raise OverflowError("Bitwise array operands must fit in signed 64-bit integers")
            return value.astype(np.int64)

        values = np.asarray(data, dtype=np.float64)
        finite = np.isfinite(values) & present
        truncated = np.trunc(values[finite])
        if np.any(truncated < -(2**63)) or np.any(truncated >= 2**63):
            raise OverflowError("Bitwise array operands must fit in signed 64-bit integers")
        if np.ma.isMaskedArray(value):
            return np.ma.array(values, mask=~present)
        return values

    if isinstance(value, np.generic):
        value = value.item()
    if isinstance(value, float) and not np.isfinite(value):
        raise ValueError("Bitwise operands must be finite")
    integer = int(value)
    if integer < -(2**63) or integer >= 2**63:
        raise OverflowError("Bitwise operands must fit in signed 64-bit integers")
    return integer


def _wrap_int64(value: int) -> int:
    """Wrap scalar integer results to the signed 64-bit range."""
    value %= 2**64
    if value >= 2**63:
        value -= 2**64
    return value


def _nan_reduction(function):
    """Keep missing values as NaN rather than ignoring them in reductions."""

    @wraps(function)
    def reduce(*args, **kwargs):
        args = tuple(arg.astype(np.float64).filled(np.nan) if np.ma.isMaskedArray(arg) else arg for arg in args)
        kwargs = {
            key: value.astype(np.float64).filled(np.nan) if np.ma.isMaskedArray(value) else value
            for key, value in kwargs.items()
        }
        return function(*args, **kwargs)

    return reduce


def _where(condition, *choices):
    """Preserve missing elements when selecting from masked bitwise results."""
    if len(choices) == 2 and any(np.ma.isMaskedArray(value) for value in (condition, *choices)):
        condition = np.ma.filled(condition, True)
        return np.ma.where(condition, *choices)
    if np.ma.isMaskedArray(condition):
        condition = condition.astype(np.float64).filled(np.nan)
    return np.where(condition, *choices)


def _bitwise_apply(operator, left: object, right: object | None = None) -> int | np.ndarray:
    """Apply a bitwise operator to scalar or array operands."""
    left_int = _bitwise_integer(left)
    right_int = None if right is None else _bitwise_integer(right)

    if isinstance(left_int, np.ndarray) or isinstance(right_int, np.ndarray):
        left_values = np.ma.getdata(left_int)
        left_finite = (
            np.isfinite(left_values)
            if np.issubdtype(left_values.dtype, np.floating)
            else np.ones(left_values.shape, dtype=bool)
        )
        left_finite &= ~np.ma.getmaskarray(left_int)
        left_integer = np.zeros(left_values.shape, dtype=np.int64)
        left_integer[left_finite] = left_values[left_finite].astype(np.int64)
        left_values = left_integer

        if right_int is None:
            finite_result = _ARRAY_BITWISE_OPERATORS[operator](left_values[left_finite])
            result = np.zeros(left_values.shape, dtype=np.int64)
            result[left_finite] = finite_result
            finite = left_finite
        else:
            right_values = np.ma.getdata(right_int)
            right_finite = (
                np.isfinite(right_values)
                if np.issubdtype(right_values.dtype, np.floating)
                else np.ones(right_values.shape, dtype=bool)
            )
            right_finite &= ~np.ma.getmaskarray(right_int)
            right_integer = np.zeros(right_values.shape, dtype=np.int64)
            right_integer[right_finite] = right_values[right_finite].astype(np.int64)
            right_values = right_integer
            left_values, left_finite, right_values, right_finite = np.broadcast_arrays(
                left_values, left_finite, right_values, right_finite
            )
            finite = left_finite & right_finite
            result = np.zeros(left_values.shape, dtype=np.int64)
            if operator in (int.__lshift__, int.__rshift__):
                if np.any(right_values[finite] < 0) or np.any(right_values[finite] >= 64):
                    raise ValueError("Bitwise shift counts must be between 0 and 63")
            finite_result = _ARRAY_BITWISE_OPERATORS[operator](left_values[finite], right_values[finite])
            result[finite] = finite_result
        if np.all(finite):
            return result
        return np.ma.array(result, mask=~finite)

    if right_int is None:
        result = _wrap_int64(operator(left_int))
    else:
        if operator in (int.__lshift__, int.__rshift__) and (right_int < 0 or right_int >= 64):
            raise ValueError("Bitwise shift counts must be between 0 and 63")
        result = _wrap_int64(operator(left_int, right_int))
    return result


def _bitwise_eval_namespace() -> dict[str, object]:
    """Build the asteval namespace with bitwise helper functions."""
    namespace = build_eval_namespace()
    for name in _NAN_REDUCTIONS:
        namespace[name] = _nan_reduction(namespace[name])
    namespace.update(
        {
            "where": _where,
            "_bitwise_bitand": lambda left, right: _bitwise_apply(int.__and__, left, right),
            "_bitwise_bitor": lambda left, right: _bitwise_apply(int.__or__, left, right),
            "_bitwise_bitxor": lambda left, right: _bitwise_apply(int.__xor__, left, right),
            "_bitwise_lshift": lambda left, right: _bitwise_apply(int.__lshift__, left, right),
            "_bitwise_rshift": lambda left, right: _bitwise_apply(int.__rshift__, left, right),
            "_bitwise_invert": lambda value: _bitwise_apply(int.__invert__, value),
            "_bitwise_comparison": lambda value, missing: np.ma.filled(value, missing)
            if np.ma.isMaskedArray(value)
            else value,
        }
    )
    return namespace


def worst_severity(severities: Iterable[AlarmSeverity]) -> AlarmSeverity:
    """Return the most severe alarm across the inputs."""
    worst = AlarmSeverity.NO_ALARM
    for s in severities:
        if _SEVERITY_ORDER.index(s) > _SEVERITY_ORDER.index(worst):
            worst = s
            if worst == AlarmSeverity.INVALID:
                break
    return worst


def worst_severity_arrays(severity_arrays: list[np.ndarray]) -> list[AlarmSeverity]:
    """Per-step worst severity across multiple ordinal-encoded severity arrays.

    Each array element is an int index into ``_SEVERITY_ORDER``.
    """
    if not severity_arrays:
        return []
    stacked = np.stack(severity_arrays, axis=0)
    max_ordinals = np.max(stacked, axis=0)
    return [_SEVERITY_ORDER[int(o)] for o in max_ordinals]


_BARE_BINDING_RE = re.compile(r"\s*\{([^{}]+)\}\s*")


def bare_binding(expression: str, bindings: Mapping[str, str]) -> str | None:
    """The binding a column passes through unchanged (``"{x}"``), else None.

    A bare-binding table column needs no parse and no numexpr call: its cells
    are the input's values as they are, so the table path skips
    ``evaluate_array`` for it. Scalar/timeseries/waveform formulas are unchanged.
    """
    match = _BARE_BINDING_RE.fullmatch(expression)
    if match is None or match.group(1) not in bindings:
        return None
    return match.group(1)


def raise_on_open_outage(spec: "FormulaSpec", parsed: ParsedFormula, data_source: "DataSource") -> None:
    """Raise ``FormulaInputOutage`` naming every bound input whose connection is down now.

    The scalar and waveform paths call this before any ``read_latest``: the
    ring still holds the last sample from before the drop, and a read of a
    missing ring may refill it from the archive -- neither is the PV's current
    value. The timeseries path does not: its window is history, and the samples
    it already holds stay true; an outage only stops new ones from arriving.
    """
    pvs = dict.fromkeys(spec.bindings[t] for t in parsed.tokens)
    in_outage = [pv for pv in pvs if data_source.in_open_outage(pv)]
    if in_outage:
        raise FormulaInputOutage.for_pvs(in_outage)


def _numexpr_expr(parsed: ParsedFormula) -> str:
    """Translate our placeholder expression to numexpr-compatible form."""
    expr = parsed.placeholder_expr
    for our_name, ne_name in NUMEXPR_NAME_MAP.items():
        expr = expr.replace(our_name + "(", ne_name + "(")
    return expr


def _evaluate_array_numexpr(parsed: ParsedFormula, variables: dict[str, np.ndarray]) -> np.ndarray:
    """Evaluate using numexpr (fast path for numeric array expressions)."""
    expr = _numexpr_expr(parsed)
    local_dict = dict(variables)
    return ne.evaluate(expr, local_dict=local_dict)


def _evaluate_array_asteval(parsed: ParsedFormula, variables: dict[str, np.ndarray]) -> np.ndarray:
    """Evaluate using asteval (fallback for expressions with unsupported functions)."""
    interp = Interpreter(usersyms=_bitwise_eval_namespace())
    for name, arr in variables.items():
        interp.symtable[name] = arr
    tree = ast.parse(parsed.placeholder_expr, mode="eval")
    tree = ast.fix_missing_locations(_BitwiseTransformer().visit(tree))
    result = interp.eval(ast.unparse(tree.body))
    if interp.error:
        msgs = [err.get_error()[1] for err in interp.error]
        raise FormulaTypeError(f"Evaluation error: {'; '.join(msgs)}")
    if result is None:
        raise FormulaTypeError("Expression returned None")
    if np.ma.isMaskedArray(result):
        result = result.astype(np.float64).filled(np.nan)
    return np.asarray(result, dtype=np.float64)


def _evaluate_scalar_asteval(
    parsed: ParsedFormula, variables: dict[str, float | int | bool | str]
) -> float | int | bool | str:
    """Evaluate a scalar expression using asteval."""
    interp = Interpreter(usersyms=_bitwise_eval_namespace())
    for name, val in variables.items():
        interp.symtable[name] = val
    tree = ast.parse(parsed.placeholder_expr, mode="eval")
    tree = ast.fix_missing_locations(_BitwiseTransformer().visit(tree))
    result = interp.eval(ast.unparse(tree.body))
    if interp.error:
        msgs = [err.get_error()[1] for err in interp.error]
        raise FormulaTypeError(f"Evaluation error: {'; '.join(msgs)}")
    if result is None:
        raise FormulaTypeError("Expression returned None")
    if isinstance(result, np.generic):
        result = result.item()
    if (
        isinstance(tree.body, ast.Call)
        and isinstance(tree.body.func, ast.Name)
        and tree.body.func.id in {*_BITWISE_HELPERS.values(), "_bitwise_invert"}
    ):
        return float(result)
    return result


async def evaluate_array(
    parsed: ParsedFormula,
    variables: dict[str, np.ndarray],
    timeout_s: float,
) -> np.ndarray:
    """Dispatch array evaluation: numexpr if compatible, else asteval.

    Timeout is best-effort: ``to_thread`` workers cannot be interrupted, so a
    timed-out computation continues in the background until it completes
    naturally. The real resource bound is expression length + input size.
    """

    return await _run_compute(lambda: evaluate_array_sync(parsed, variables), timeout_s)


def evaluate_array_sync(parsed: ParsedFormula, variables: dict[str, np.ndarray]) -> np.ndarray:
    """Evaluate on the calling thread: numexpr if compatible, else asteval; no timeout, no error mapping.

    For a caller that picks its own thread and handles failures itself: the
    table store computes column arrays and nulls the cells it cannot compute.
    """
    if parsed.is_numexpr_compatible:
        return _evaluate_array_numexpr(parsed, variables)
    return _evaluate_array_asteval(parsed, variables)


async def evaluate_scalar(
    parsed: ParsedFormula,
    variables: dict[str, float | int | bool | str],
    timeout_s: float,
) -> float | int | bool | str:
    """Evaluate a scalar expression using asteval with timeout."""

    def compute():
        return _evaluate_scalar_asteval(parsed, variables)

    return await _run_compute(compute, timeout_s)


async def _run_compute(compute, timeout_s: float):
    """Run a compute() in a worker thread, mapping failures to typed errors.

    numexpr/numpy raise plain ``ValueError`` (shape/dtype), ``OverflowError``,
    ``RecursionError`` etc. Without this they escape as generic 500s; map them to
    ``FormulaTypeError`` (422) so bad inputs read as client errors, not crashes.
    """
    try:
        return await asyncio.wait_for(asyncio.to_thread(compute), timeout=timeout_s)
    except TimeoutError:
        raise FormulaEvalTimeout(f"Expression evaluation timed out after {timeout_s:.1f}s") from None
    except FormulaEngineError:
        raise
    except (ValueError, ArithmeticError, TypeError, RecursionError) as exc:
        raise FormulaTypeError(f"Evaluation error: {exc}") from exc
