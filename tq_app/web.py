from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from flask import Flask, jsonify, render_template, request

from tq_app.service import MarketDataService


def create_app(service: MarketDataService, project_root: Path) -> Flask:
    app = Flask(
        __name__,
        template_folder=str(project_root / "templates"),
        static_folder=str(project_root / "static"),
    )

    @app.get("/")
    def index() -> str:
        return render_template("index.html")

    @app.get("/api/config")
    def api_config() -> Any:
        provider = request.args.get("provider", "").strip() or None
        return jsonify(service.get_config(provider=provider))

    @app.get("/api/snapshot")
    def api_snapshot() -> Any:
        indicator_param = request.args.get("indicators", "")
        indicator_params_raw = request.args.get("indicator_params", "")
        symbol = request.args.get("symbol", "").strip() or None
        provider = request.args.get("provider", "").strip() or None
        duration_raw = request.args.get("duration_seconds", "").strip()
        bar_mode = request.args.get("bar_mode", "").strip() or None
        range_ticks_raw = request.args.get("range_ticks", "").strip()
        brick_length_raw = request.args.get("brick_length", "").strip()
        indicator_ids = [item.strip() for item in indicator_param.split(",") if item.strip()]
        indicator_params: dict[str, dict[str, Any]] | None = None
        duration_seconds: int | None = None
        range_ticks: int | None = None
        brick_length: int | None = None
        if indicator_params_raw:
            indicator_params = json.loads(indicator_params_raw)
        if duration_raw:
            duration_seconds = int(duration_raw)
        if range_ticks_raw:
            range_ticks = int(range_ticks_raw)
        if brick_length_raw:
            brick_length = int(brick_length_raw)
        try:
            return jsonify(
                service.get_snapshot(
                    indicator_ids or None,
                    indicator_params,
                    symbol=symbol,
                    provider=provider,
                    duration_seconds=duration_seconds,
                    bar_mode=bar_mode,
                    range_ticks=range_ticks,
                    brick_length=brick_length,
                )
            )
        except Exception as exc:
            return jsonify({"error": str(exc)}), 500

    return app
