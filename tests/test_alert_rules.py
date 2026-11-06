"""The provisioned Grafana alert rules agree with the gauges Canopy exports.

Every rule file under ``docker/observability/grafana/provisioning/alerting/`` is
checked: uids are unique and short enough for Grafana, every rule routes by ``severity`` and ``team``, a series
that doesn't exist is OK rather than an alert, every metric an expression names is
one a collector emits for a representative status, and every ``$labels.X`` an
annotation uses is a label those series carry (or the scrape's ``instance``/``job``).
A rule on a head's per-arm series has to name the arm.

A collector added later contributes its own representative samples to ``SAMPLES``.
"""

import re
from pathlib import Path
from datetime import UTC, datetime

import yaml
import pytest
from prometheus_client import CollectorRegistry, generate_latest
from prometheus_client.parser import text_string_to_metric_families

from canopy.core.replication.metrics import ReplicationCollector

ALERTING = Path(__file__).resolve().parents[1] / "docker" / "observability" / "grafana" / "provisioning" / "alerting"


def _samples(collector) -> dict[str, set[str]]:
    """``{sample name: label names}`` of one scrape of ``collector``."""
    registry = CollectorRegistry()
    registry.register(collector)
    series: dict[str, set[str]] = {}
    for family in text_string_to_metric_families(generate_latest(registry).decode()):
        for sample in family.samples:
            series.setdefault(sample.name, set()).update(sample.labels)
    return series


def _replication(status: dict) -> dict[str, set[str]]:
    collector = ReplicationCollector()
    collector.status = status
    return _samples(collector)


_SLOT = {
    "slot_name": "canopy_history_arm1",
    "kind": "subscription",
    "active": True,
    "wal_status": "reserved",
    "safe_wal_size": 1,
    "retained_bytes": 1,
    "lag_bytes": 1,
}
_SUBSCRIPTION = {
    "arm": "arm1",
    "name": "canopy_history_arm1",
    "exists": True,
    "enabled": True,
    "worker": {"pid": 1, "last_msg_receipt_time": datetime.now(UTC).isoformat()},
    "applied_lsn": "0/1",
    "apply_error_count": 0,
    "sync_error_count": 0,
    "tables_not_ready": {},
}

# Every series a rule may use, from a representative status of each collector.
SAMPLES: dict[str, set[str]] = {}
for _series in (
    _replication({"side": "publisher", "slots": [_SLOT], "max_slot_wal_keep_bytes": 1}),
    _replication({"side": "subscriber", "subscriptions": [_SUBSCRIPTION], "unexpected": []}),
):
    for _name, _labels in _series.items():
        SAMPLES.setdefault(_name, set()).update(_labels)

SCRAPE_LABELS = {"instance", "job"}
_METRIC = re.compile(r"\bcanopy_[a-z0-9_]+\b")
_LABEL_REF = re.compile(r"\$labels\.([a-zA-Z_][a-zA-Z0-9_]*)")


def _rules() -> list[tuple[str, dict]]:
    rules = []
    for path in sorted(ALERTING.glob("*.yaml")):
        for group in yaml.safe_load(path.read_text())["groups"]:
            rules.extend((path.name, rule) for rule in group["rules"])
    return rules


RULES = _rules()


def _metrics(rule: dict) -> set[str]:
    return {
        name
        for query in rule["data"]
        if query["datasourceUid"] != "__expr__"
        for name in _METRIC.findall(query["model"]["expr"])
    }


def test_there_are_rules():
    assert RULES


def test_uids_are_unique_and_grafana_accepts_them():
    """Grafana refuses a whole provisioning file, and stops, over a uid longer than 40 characters."""
    uids = [rule["uid"] for _, rule in RULES]
    assert len(uids) == len(set(uids))
    assert not [uid for uid in uids if len(uid) > 40]


@pytest.mark.parametrize(("path", "rule"), RULES, ids=[rule["uid"] for _, rule in RULES])
def test_a_rule_routes_and_stays_quiet_without_its_series(path, rule):
    assert rule["labels"]["severity"] in {"warning", "critical"}
    assert rule["labels"]["team"] == "canopy"
    assert rule["noDataState"] == "OK"
    assert rule["condition"] in {query["refId"] for query in rule["data"]}


@pytest.mark.parametrize(("path", "rule"), RULES, ids=[rule["uid"] for _, rule in RULES])
def test_a_rule_uses_only_exported_series_and_their_labels(path, rule):
    metrics = _metrics(rule)
    assert metrics, "the rule queries no Canopy series"
    assert not (unknown := metrics - set(SAMPLES)), f"no collector emits {sorted(unknown)}"
    labels = set().union(*(SAMPLES[name] for name in metrics)) | SCRAPE_LABELS
    used = set(_LABEL_REF.findall(" ".join(rule["annotations"].values())))
    assert not (missing := used - labels), f"annotations use labels the series lack: {sorted(missing)}"


@pytest.mark.parametrize(("path", "rule"), RULES, ids=[rule["uid"] for _, rule in RULES])
def test_a_rule_on_a_heads_arms_names_the_arm(path, rule):
    if any("arm" in SAMPLES[name] for name in _metrics(rule) if name in SAMPLES):
        assert "$labels.arm" in rule["annotations"]["summary"]
