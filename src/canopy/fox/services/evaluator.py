"""Core evaluation dispatch: numexpr for arrays, asteval for scalars.

All compute is CPU-bound and runs via ``asyncio.to_thread`` to avoid blocking
the event loop. Timeout is enforced via ``asyncio.wait_for``.
"""

import asyncio
import ast
from collections.abc import Iterable

import numpy as np
import numexpr as ne
from asteval import Interpreter

from canopy.fox.errors import FoxError, FormulaTypeError, FormulaEvalTimeout
from canopy.contracts.sample import AlarmSeverity
from canopy.fox.services.parser import ParsedFormula
from canopy.fox.language.namespace import NUMEXPR_NAME_MAP, build_eval_namespace

_SEVERITY_ORDER = [AlarmSeverity.NO_ALARM, AlarmSeverity.MINOR, AlarmSeverity.MAJOR, AlarmSeverity.INVALID]
_BITWISE_HELPERS = {
    ast.BitAnd: "_fox_bitand",
    ast.BitOr: "_fox_bitor",
    ast.BitXor: "_fox_bitxor",
    ast.LShift: "_fox_lshift",
    ast.RShift: "_fox_rshift",
}
_ARRAY_BITWISE_OPERATORS = {
    int.__and__: np.bitwise_and,
    int.__or__: np.bitwise_or,
    int.__xor__: np.bitwise_xor,
    int.__lshift__: np.left_shift,
    int.__rshift__: np.right_shift,
    int.__invert__: np.invert,
}


class _BitwiseTransformer(ast.NodeTransformer):
    """Route bitwise AST nodes through EPICS-compatible integer coercion."""

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
            func=ast.Name(id="_fox_invert", ctx=ast.Load()),
            args=[node.operand],
            keywords=[],
        )
        return ast.copy_location(call, node)


def _bitwise_integer(value: object) -> int | np.ndarray:
    if isinstance(value, np.ndarray):
        values = np.asarray(value, dtype=np.float64)
        if not np.all(np.isfinite(values)):
            raise ValueError("Bitwise operands must be finite")
        values = np.trunc(values)
        if np.any(values < -(2**63)) or np.any(values >= 2**63):
            raise OverflowError("Bitwise array operands must fit in signed 64-bit integers")
        return values.astype(np.int64)

    if isinstance(value, np.generic):
        value = value.item()
    if isinstance(value, float) and not np.isfinite(value):
        raise ValueError("Bitwise operands must be finite")
    return int(value)


def _bitwise_apply(operator, left: object, right: object | None = None) -> float | np.ndarray:
    left_int = _bitwise_integer(left)
    right_int = None if right is None else _bitwise_integer(right)

    if isinstance(left_int, np.ndarray) or isinstance(right_int, np.ndarray):
        left_values = np.asarray(left_int, dtype=np.int64)
        if right_int is None:
            result = _ARRAY_BITWISE_OPERATORS[operator](left_values)
        else:
            right_values = np.asarray(right_int, dtype=np.int64)
            if operator in (int.__lshift__, int.__rshift__):
                if np.any(right_values < 0) or np.any(right_values >= 64):
                    raise ValueError("Bitwise shift counts must be between 0 and 63")
            result = _ARRAY_BITWISE_OPERATORS[operator](left_values, right_values)
        return np.asarray(result, dtype=np.float64)

    if right_int is None:
        result = operator(left_int)
    else:
        if operator in (int.__lshift__, int.__rshift__) and (right_int < 0 or right_int >= 64):
            raise ValueError("Bitwise shift counts must be between 0 and 63")
        result = operator(left_int, right_int)
    return float(result)


def _bitwise_eval_namespace() -> dict[str, object]:
    namespace = build_eval_namespace()
    namespace.update({
        "_fox_bitand": lambda left, right: _bitwise_apply(int.__and__, left, right),
        "_fox_bitor": lambda left, right: _bitwise_apply(int.__or__, left, right),
        "_fox_bitxor": lambda left, right: _bitwise_apply(int.__xor__, left, right),
        "_fox_lshift": lambda left, right: _bitwise_apply(int.__lshift__, left, right),
        "_fox_rshift": lambda left, right: _bitwise_apply(int.__rshift__, left, right),
        "_fox_invert": lambda value: _bitwise_apply(int.__invert__, value),
    })
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

    def compute():
        if parsed.is_numexpr_compatible:
            return _evaluate_array_numexpr(parsed, variables)
        return _evaluate_array_asteval(parsed, variables)

    return await _run_compute(compute, timeout_s)


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
    except FoxError:
        raise
    except (ValueError, ArithmeticError, TypeError, RecursionError) as exc:
        raise FormulaTypeError(f"Evaluation error: {exc}") from exc
