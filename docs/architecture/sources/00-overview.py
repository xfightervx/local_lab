#!/usr/bin/env python3
"""
Overview Diagram Generator
--------------------------
Generates the architecture overview (PNG & SVG) with brand icons when available.

Outputs:
  docs/architecture/exports/00-overview.png
  docs/architecture/exports/00-overview.svg

Icon search path:
  docs/architecture/icons/
  Expected names : kafka.svg, spark.svg, postgres.svg, dbt.svg, airflow.svg,
                   pandas.svg, datasource.svg, analyst.svg
  Missing icons gracefully fall back to built-in diagram nodes.

Requirements (install locally):
  - Python 3.9+
  - Graphviz system package
"""

from __future__ import annotations

import argparse
import shutil
from pathlib import Path
from typing import Dict, Optional

from diagrams import Diagram, Edge, Cluster
from diagrams.custom import Custom

# Built-in fallbacks
from diagrams.onprem.queue import Kafka as FallbackKafka
from diagrams.onprem.analytics import Spark as FallbackSpark
from diagrams.onprem.workflow import Airflow as FallbackAirflow
from diagrams.onprem.database import PostgreSQL as FallbackPostgres
from diagrams.onprem.client import Client as FallbackClient
from diagrams.generic.storage import Storage as FallbackStorage

# ---------------------------------------------------------------------------
# Paths & constants
# ---------------------------------------------------------------------------

BASE_DIR = Path(__file__).resolve().parent
ICONS_DIR = BASE_DIR.parent / "icons"
EXPORTS_DIR = BASE_DIR.parent / "exports"
PROCESSED_DIR = EXPORTS_DIR / "_processed_icons"  # cache for normalized+bordered icons

TITLE = "Rides Analytics — Overview"
DEFAULT_OUT_BASENAME = "00-overview"

LBL_JDBC_UPSERT = "JDBC upsert"
LBL_SPARKSUBMIT = "SparkSubmit (hourly)"
LBL_DBT_RUN = "dbt run + test"

# Normalization settings (logos)
ICON_SIZE = 256           # final square canvas (px)
ICON_BORDER = 6           # border width (px)
ICON_BORDER_ALPHA = 180   # 0..255 (semi-opaque)
ICON_BORDER_COLOR = (60, 60, 60, ICON_BORDER_ALPHA)  # dark gray, semi-opaque
ICON_INNER_MARGIN = 12    # padding inside border before the image (px)

ICON_NAMES = ["kafka", "spark", "postgres", "dbt", "airflow", "pandas", "datasource", "analyst"]

# ---------------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------------

def graphviz_available() -> bool:
    return shutil.which("dot") is not None

def ensure_dirs() -> None:
    EXPORTS_DIR.mkdir(parents=True, exist_ok=True)
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

def _raw_icon_path(name: str) -> Optional[Path]:
    """Return the path to a raw source icon if it exists (PNG only, per current script)."""
    png = ICONS_DIR / f"{name}.png"
    return png if png.exists() else None

def _process_icon(raw: Path) -> Path:
    """
    Normalize icon to a square PNG canvas with transparent background,
    add a border rectangle, and cache to PROCESSED_DIR.
    If Pillow isn't available, fall back to the raw icon.
    """
    try:
        from PIL import Image, ImageOps, ImageDraw
    except Exception:
        print(f"[WARN] Pillow not available; using original icon without border: {raw.name}")
        return raw

    out_path = PROCESSED_DIR / raw.name

    try:
        im = Image.open(raw).convert("RGBA")
        max_inner = ICON_SIZE - 2 * ICON_BORDER - 2 * ICON_INNER_MARGIN
        if max_inner <= 0:
            max_inner = ICON_SIZE

        im = ImageOps.contain(im, (max_inner, max_inner))

        canvas = Image.new("RGBA", (ICON_SIZE, ICON_SIZE), (0, 0, 0, 0))
        x = (ICON_SIZE - im.width) // 2
        y = (ICON_SIZE - im.height) // 2
        canvas.paste(im, (x, y), im)

        draw = ImageDraw.Draw(canvas)
        inset = ICON_BORDER // 2
        draw.rectangle(
            [inset, inset, ICON_SIZE - 1 - inset, ICON_SIZE - 1 - inset],
            outline=ICON_BORDER_COLOR,
            width=ICON_BORDER
        )

        canvas.save(out_path, format="PNG")
        return out_path
    except Exception as e:
        print(f"[WARN] Failed to process icon {raw.name}: {e}. Using original.")
        return raw

def icon(name: str) -> Optional[Path]:
    """Return a Path to a normalized, bordered icon (PNG) if available; else raw PNG; else None."""
    raw = _raw_icon_path(name)
    if raw is None:
        return None
    return _process_icon(raw)

# Helper to make a Custom node or fallback quickly
def _node(label: str, icon_name: str, fallback_cls):
    ic = icon(icon_name)
    return Custom(label, str(ic)) if ic else fallback_cls(label)

# ---------------------------------------------------------------------------
# Edges
# ---------------------------------------------------------------------------

def wire_edges(nodes: Dict[str, object]) -> None:
    """
    Wire edges according to the agreed overview spec.
    Top row (stream path): Source → Kafka → Spark Streaming → Postgres
    Bottom row (batch/orchestration): Airflow → Spark Batch → Postgres → dbt → Postgres → Analyst
    """
    nodes["source"] >> nodes["kafka"]
    nodes["kafka"] >> nodes["spark_stream"]
    nodes["spark_stream"] >> Edge(label=LBL_JDBC_UPSERT) >> nodes["postgres"]

    nodes["airflow"] >> Edge(label=LBL_SPARKSUBMIT) >> nodes["spark_batch"]
    nodes["spark_batch"] >> nodes["postgres"]
    nodes["airflow"] >> Edge(label=LBL_DBT_RUN) >> nodes["dbt"]
    nodes["dbt"] >> nodes["postgres"]
    nodes["postgres"] >> nodes["analyst"]

# ---------------------------------------------------------------------------
# Render
# ---------------------------------------------------------------------------

def render(out_basename: str, formats: list[str]) -> None:
    """
    Render the diagram for all requested formats. Adds three rectangles (clusters):
      - App (outer)
      - Streaming Process (inner)
      - Batch & Orchestration (inner)
    """
    for fmt in formats:
        with Diagram(
            TITLE,
            filename=str(EXPORTS_DIR / out_basename),
            direction="LR",
            show=False,
            outformat=fmt,
        ):
            nodes: Dict[str, object] = {}

            # Outer rectangle for the whole app
            with Cluster("Rides Analytics (App)"):
                # Streaming cluster (top lane)
                with Cluster("Streaming Process"):
                    nodes["source"] = _node("Data Source\n(CSV/Parquet)", "datasource", FallbackStorage)
                    nodes["kafka"] = _node("Kafka\n(topic: rides_raw)", "kafka", FallbackKafka)
                    nodes["spark_stream"] = _node("Spark — Structured Streaming\n(clean/enrich)", "spark", FallbackSpark)

                # Central warehouse lives at app scope (shared by both lanes)
                nodes["postgres"] = _node("Postgres\n(stg / core / mart)", "postgres", FallbackPostgres)

                # Batch & orchestration cluster (bottom lane)
                with Cluster("Batch & Orchestration"):
                    nodes["airflow"] = _node("Airflow\n(orchestration)", "airflow", FallbackAirflow)
                    nodes["spark_batch"] = _node("Spark — Batch Aggregations\n(hourly)", "spark", FallbackSpark)
                    nodes["dbt"] = _node("dbt\n(models + tests)", "dbt", FallbackClient)
                    nodes["analyst"] = _node("Analyst / Notebook\n(Pandas)", "pandas", FallbackClient)

                # Wire edges across clusters
                wire_edges(nodes)

# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> None:
    if not graphviz_available():
        print(
            "[ERROR] Graphviz 'dot' is not available on PATH.\n"
            "        Install Graphviz (e.g., 'brew install graphviz' on macOS, "
            "'sudo apt-get install graphviz' on Debian/Ubuntu) and retry."
        )
        return

    ensure_dirs()

    parser = argparse.ArgumentParser(description="Generate architecture overview diagram.")
    g = parser.add_mutually_exclusive_group()
    g.add_argument("--png-only", action="store_true", help="Render PNG only")
    g.add_argument("--svg-only", action="store_true", help="Render SVG only")
    parser.add_argument("--out-basename", default=DEFAULT_OUT_BASENAME, help="Output file basename")
    args = parser.parse_args()

    formats = ["png", "svg"]
    if args.png_only:
        formats = ["png"]
    if args.svg_only:
        formats = ["svg"]

    missing = [name for name in ICON_NAMES if _raw_icon_path(name) is None]
    if missing:
        print(f"[INFO] Using fallbacks for missing icons: {', '.join(missing)}")

    try:
        render(args.out_basename, formats)
    except Exception as e:
        print("[ERROR] Failed to render diagram.")
        print(f"        {e}")
        return

    produced = ", ".join(str((EXPORTS_DIR / args.out_basename).with_suffix(f".{f}")) for f in formats)
    print(f"[OK] Generated: {produced}")

if __name__ == "__main__":
    main()
