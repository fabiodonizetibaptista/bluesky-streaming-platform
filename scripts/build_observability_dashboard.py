"""Gera o dashboard Grafana com métricas Flink verificadas.

Não acessa credenciais nem modifica containers.
O JSON produzido é declarativo e versionável.
"""

import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
OUT = (
    ROOT
    / "monitoring/grafana/provisioning/dashboards/json"
    / "flink-operational.json"
)

OUT.parent.mkdir(parents=True, exist_ok=True)

DATASOURCE = {"type": "prometheus", "uid": "prometheus"}

panels = []


def target(expression, legend):
    return {
        "expr": expression,
        "legendFormat": legend,
        "refId": chr(65 + len(panels) % 26),
    }


def panel(title, expression, legend, x, y, w=12, h=8,
          unit="short", visualization="timeseries"):
    index = len(panels) + 1

    panels.append({
        "id": index,
        "title": title,
        "type": visualization,
        "datasource": DATASOURCE,
        "gridPos": {
            "x": x, "y": y, "w": w, "h": h,
        },
        "targets": [{
            "expr": expression,
            "legendFormat": legend,
            "refId": "A",
        }],
        "fieldConfig": {
            "defaults": {
                "unit": unit,
                "color": {"mode": "palette-classic"},
            },
            "overrides": [],
        },
        "options": {
            "legend": {
                "displayMode": "list",
                "placement": "bottom",
            },
            "tooltip": {"mode": "multi"},
        } if visualization == "timeseries" else {
            "reduceOptions": {
                "calcs": ["lastNotNull"],
                "fields": "",
                "values": False,
            },
            "orientation": "auto",
            "textMode": "auto",
        },
    })


panel(
    "Flink exporters disponíveis",
    'sum(up{job="flink"})',
    "Endpoints disponíveis",
    0, 0, 6, 5,
    visualization="stat",
)

panel(
    "Slots Flink disponíveis",
    "flink_jobmanager_taskSlotsAvailable",
    "Slots disponíveis",
    6, 0, 6, 5,
    visualization="stat",
)

panel(
    "Disponibilidade por processo",
    'up{job="flink"}',
    "{{instance}}",
    12, 0, 12, 5,
)

panel(
    "Slots totais e disponíveis",
    "flink_jobmanager_taskSlotsTotal",
    "Slots totais",
    0, 5, 12, 8,
)

# Segundo alvo no painel de slots.
panels[-1]["targets"].append({
    "expr": "flink_jobmanager_taskSlotsAvailable",
    "legendFormat": "Slots disponíveis",
    "refId": "B",
})

panel(
    "CPU JVM — JobManager",
    "flink_jobmanager_Status_JVM_CPU_Load",
    "JobManager",
    12, 5, 12, 8,
    unit="percentunit",
)

panel(
    "CPU JVM — TaskManagers",
    "flink_taskmanager_Status_JVM_CPU_Load",
    "{{instance}}",
    0, 13, 12, 8,
    unit="percentunit",
)

panel(
    "Heap JVM — JobManager",
    "flink_jobmanager_Status_JVM_Memory_Heap_Used",
    "JobManager",
    12, 13, 12, 8,
    unit="bytes",
)

panel(
    "Heap JVM — TaskManagers",
    "flink_taskmanager_Status_JVM_Memory_Heap_Used",
    "{{instance}}",
    0, 21, 12, 8,
    unit="bytes",
)

panel(
    "Heap JVM utilizada (%)",
    "100 * "
    "flink_taskmanager_Status_JVM_Memory_Heap_Used"
    " / clamp_min("
    "flink_taskmanager_Status_JVM_Memory_Heap_Max, 1)",
    "{{instance}}",
    12, 21, 12, 8,
    unit="percent",
)

dashboard = {
    "uid": "financial-flink-ops",
    "title": "Financial Streaming | Flink Operations",
    "description": (
        "Monitoramento operacional do cluster Apache Flink "
        "com métricas coletadas pelo Prometheus."
    ),
    "tags": ["data-engineering", "flink", "streaming"],
    "timezone": "browser",
    "schemaVersion": 41,
    "version": 1,
    "refresh": "10s",
    "time": {"from": "now-1h", "to": "now"},
    "panels": panels,
}

OUT.write_text(
    json.dumps(dashboard, indent=2, ensure_ascii=False) + "\n",
    encoding="utf-8",
)

print("DASHBOARD_GENERATED=True")
print("PANELS =", len(panels))
print("OUTPUT =", OUT.relative_to(ROOT))
