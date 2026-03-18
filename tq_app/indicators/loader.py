from __future__ import annotations

import importlib.util
from pathlib import Path

from .base import IndicatorRegistry
from .builtin import register_builtin_indicators


def build_indicator_registry(project_root: Path) -> IndicatorRegistry:
    registry = IndicatorRegistry()
    register_builtin_indicators(registry)
    _load_custom_indicators(project_root, registry)
    return registry


def _load_custom_indicators(project_root: Path, registry: IndicatorRegistry) -> None:
    custom_path = project_root / "custom_indicators.py"
    if not custom_path.exists():
        return

    spec = importlib.util.spec_from_file_location("custom_indicators", custom_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"无法加载自定义指标文件: {custom_path}")

    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    register = getattr(module, "register_indicators", None)
    if register is None:
        raise RuntimeError("custom_indicators.py 需要提供 register_indicators(registry) 函数。")
    register(registry)
