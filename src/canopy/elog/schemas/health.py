from typing import Literal

from canopy.contracts.base import CanopyModel


class ElogAppHealthDTO(CanopyModel):
    """One app's token, probed with the provider's own identity lookup."""

    app: str
    healthy: bool
    # Who the e-log backend says the token is (elog-plus: the app's email).
    identity: str | None = None
    latency_ms: float | None = None
    last_error: str | None = None


class ElogHealthSummaryDTO(CanopyModel):
    """Rollup across configured apps.

    ``disabled`` when no app can post (no provider, or no app has a token);
    otherwise ``ok`` when every app's probe succeeds, ``down`` when none does,
    and ``degraded`` in between.
    """

    status: Literal["ok", "degraded", "down", "disabled"]
    provider: str | None = None
    apps: list[ElogAppHealthDTO]
