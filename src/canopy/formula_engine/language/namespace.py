"""Function and constant namespace exposed to Formula Engine expressions.

``SAFE_FUNCS`` is the single allowlist: any ``Call`` in the AST whose function
name is not a key here is rejected. The values are the actual callables used
at evaluation time.
"""

import math

import numpy as np


def _float(value: object) -> float | np.ndarray:
    """Convert a scalar or array value to floating point."""
    if isinstance(value, np.ndarray):
        return np.asarray(value, dtype=np.float64)
    return float(value)


def _int(value: object) -> int | np.ndarray:
    """Convert a scalar or array to a signed 64-bit integer.

    Array NaN and infinity values remain NaN; finite values are truncated
    toward zero and returned as float64 so missing samples stay missing.
    """
    if not isinstance(value, np.ndarray):
        return int(value)

    if np.issubdtype(value.dtype, np.integer) or np.issubdtype(value.dtype, np.bool_):
        if np.any(value.astype(object) < -(2**63)) or np.any(value.astype(object) >= 2**63):
            raise OverflowError("int() array values must fit in signed 64-bit integers")
        return value.astype(np.int64)

    values = np.asarray(value, dtype=np.float64)
    finite = np.isfinite(values)
    finite_values = np.trunc(values[finite])
    if np.any(finite_values < -(2**63)) or np.any(finite_values >= 2**63):
        raise OverflowError("int() array values must fit in signed 64-bit integers")
    result = np.full(values.shape, np.nan, dtype=np.float64)
    result[finite] = finite_values
    return result


def _str(value: object) -> str:
    """Convert a scalar value to text; array conversion is unsupported."""
    if isinstance(value, np.ndarray):
        raise TypeError("str() is only supported for scalar values")
    return str(value)


def _isnan(value: object) -> bool | np.ndarray:
    """Test scalar or array values for NaN."""
    if isinstance(value, np.ndarray):
        return np.isnan(value)
    return bool(np.isnan(value))


SAFE_FUNCS: dict[str, object] = {
    # Conversion and classification
    "float": _float,
    "int": _int,
    "str": _str,
    "isnan": _isnan,
    # Trigonometric
    "sin": np.sin,
    "cos": np.cos,
    "tan": np.tan,
    "asin": np.arcsin,
    "acos": np.arccos,
    "atan": np.arctan,
    "atan2": np.arctan2,
    "sinh": np.sinh,
    "cosh": np.cosh,
    "tanh": np.tanh,
    # Exponential / logarithmic
    "exp": np.exp,
    "log": np.log,
    "log10": np.log10,
    "ln": np.log,
    "sqrt": np.sqrt,
    "pow": np.power,
    "abs": np.abs,
    # Rounding
    "floor": np.floor,
    "ceil": np.ceil,
    # Angle conversion
    "radians": np.radians,
    "degrees": np.degrees,
    # Reductions
    "mean": np.mean,
    "sum": np.sum,
    "min": np.min,
    "max": np.max,
    "std": np.std,
    "var": np.var,
    "ptp": np.ptp,
    "count_nonzero": np.count_nonzero,
    # Elementwise
    "where": np.where,
    "clip": np.clip,
}

SAFE_CONSTANTS: dict[str, object] = {
    "pi": math.pi,
    "e": math.e,
    "inf": math.inf,
    "nan": math.nan,
    "True": True,
    "False": False,
    "None": None,
}

# Functions that numexpr supports natively (used to detect fast-path compatibility)
NUMEXPR_FUNCS: set[str] = {
    "sin",
    "cos",
    "tan",
    "arcsin",
    "arccos",
    "arctan",
    "arctan2",
    "sinh",
    "cosh",
    "tanh",
    "exp",
    "log",
    "log10",
    "sqrt",
    "abs",
    "where",
}

# Mapping from our namespace names to numexpr-compatible names
NUMEXPR_NAME_MAP: dict[str, str] = {
    "asin": "arcsin",
    "acos": "arccos",
    "atan": "arctan",
    "atan2": "arctan2",
    "ln": "log",
}


def build_eval_namespace() -> dict[str, object]:
    """Build the namespace dict for asteval / direct eval."""
    ns: dict[str, object] = {}
    ns.update(SAFE_FUNCS)
    ns.update(SAFE_CONSTANTS)
    ns["np"] = np
    ns["math"] = math
    return ns
