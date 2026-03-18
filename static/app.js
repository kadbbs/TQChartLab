const state = {
  config: null,
  activeSymbol: "",
  activeDurationSeconds: null,
  selectedIndicators: [],
  indicatorParams: {},
  charts: [],
  seriesByKey: new Map(),
  seriesDataByKey: new Map(),
  primarySeriesKeyByPane: new Map(),
  currentPriceLine: null,
  hasFitted: false,
  isAdjustingRange: false,
  isSyncingCrosshair: false,
};

const DEFAULT_VISIBLE_BARS = 120;
const MAX_VISIBLE_BARS = 180;
const DISPLAY_WARMUP_BARS = 200;
const RANGE_RIGHT_PADDING_BARS = 8;
const PRICE_RANGE_TOP_PADDING = 0.1;
const PRICE_RANGE_BOTTOM_PADDING = 0.14;

const els = {
  title: document.getElementById("page-title"),
  provider: document.getElementById("provider-name"),
  symbol: document.getElementById("symbol-name"),
  duration: document.getElementById("duration-name"),
  symbolSelect: document.getElementById("symbol-select"),
  durationSelect: document.getElementById("duration-select"),
  lastPrice: document.getElementById("last-price"),
  lastUpdate: document.getElementById("last-update"),
  cursorTime: document.getElementById("cursor-time"),
  indicatorForm: document.getElementById("indicator-form"),
  chartStack: document.getElementById("chart-stack"),
  error: document.getElementById("error-message"),
};

const chartTheme = {
  layout: {
    background: { type: "solid", color: "#fff8ee" },
    textColor: "#5f4a35",
    fontFamily: "IBM Plex Sans, PingFang SC, Microsoft YaHei, sans-serif",
  },
  grid: {
    vertLines: { color: "rgba(92, 70, 47, 0.09)" },
    horzLines: { color: "rgba(92, 70, 47, 0.09)" },
  },
  crosshair: {
    mode: LightweightCharts.CrosshairMode.Normal,
    vertLine: { color: "#a24f2f", labelBackgroundColor: "#a24f2f" },
    horzLine: { color: "#a24f2f", labelBackgroundColor: "#a24f2f" },
  },
  rightPriceScale: {
    borderColor: "rgba(92, 70, 47, 0.18)",
    autoScale: true,
    scaleMargins: {
      top: 0.16,
      bottom: 0.2,
    },
  },
  timeScale: {
    borderColor: "rgba(92, 70, 47, 0.18)",
    timeVisible: true,
    secondsVisible: false,
    rightOffset: 10,
    barSpacing: 10,
    minBarSpacing: 4,
    tickMarkMaxCharacterLength: 18,
    ticksVisible: true,
  },
  handleScroll: {
    mouseWheel: true,
    pressedMouseMove: true,
    horzTouchDrag: true,
    vertTouchDrag: false,
  },
  handleScale: {
    mouseWheel: true,
    pinch: true,
    axisPressedMouseMove: {
      time: true,
      price: false,
    },
  },
  localization: {
    locale: "zh-CN",
    dateFormat: "yyyy-MM-dd",
  },
};

async function fetchJson(url) {
  const response = await fetch(url, { cache: "no-store" });
  const payload = await response.json();
  if (!response.ok) {
    throw new Error(payload.error || "请求失败");
  }
  return payload;
}

function getIndicatorIds() {
  return [...els.indicatorForm.querySelectorAll("input[type=checkbox]:checked")].map((item) => item.value);
}

function getDefaultIndicatorParams(indicators) {
  const defaults = {};
  indicators.forEach((indicator) => {
    defaults[indicator.id] = {};
    (indicator.params || []).forEach((param) => {
      defaults[indicator.id][param.key] = param.default;
    });
  });
  return defaults;
}

function updateIndicatorParamState(indicatorId, key, value) {
  if (!state.indicatorParams[indicatorId]) {
    state.indicatorParams[indicatorId] = {};
  }
  state.indicatorParams[indicatorId][key] = value;
}

function formatDurationLabel(seconds) {
  if (seconds < 60) {
    return `${seconds} 秒`;
  }
  if (seconds < 3600) {
    return `${Math.round(seconds / 60)} 分钟`;
  }
  if (seconds < 86400) {
    return `${Math.round(seconds / 3600)} 小时`;
  }
  return `${Math.round(seconds / 86400)} 天`;
}

function syncMarketHeader(symbolLabel, durationSeconds) {
  els.title.textContent = `${symbolLabel} 图表工作台`;
  els.symbol.textContent = symbolLabel;
  els.duration.textContent = formatDurationLabel(durationSeconds);
}

function buildDurationOptions(options, activeValue) {
  els.durationSelect.innerHTML = "";
  options.forEach((seconds) => {
    const option = document.createElement("option");
    option.value = String(seconds);
    option.textContent = formatDurationLabel(seconds);
    option.selected = seconds === activeValue;
    els.durationSelect.append(option);
  });
}

function buildContractOptions(contracts, activeSymbol) {
  els.symbolSelect.innerHTML = "";
  const normalizedContracts = contracts.length
    ? contracts
    : [{ symbol: activeSymbol, label: activeSymbol }];
  normalizedContracts.forEach((contract) => {
    const option = document.createElement("option");
    option.value = contract.symbol;
    option.textContent = contract.label;
    option.selected = contract.symbol === activeSymbol;
    els.symbolSelect.append(option);
  });
}

function getRequestedSymbol() {
  return els.symbolSelect.value || state.config.symbol;
}

function getRequestedDuration() {
  return Number(els.durationSelect.value || state.config.duration_seconds);
}

function buildIndicatorSelector(indicators, defaults) {
  els.indicatorForm.innerHTML = "";
  indicators.forEach((indicator) => {
    const label = document.createElement("label");
    label.className = "indicator-item";

    const checkbox = document.createElement("input");
    checkbox.type = "checkbox";
    checkbox.value = indicator.id;
    checkbox.checked = defaults.includes(indicator.id);
    checkbox.addEventListener("change", async () => {
      state.selectedIndicators = getIndicatorIds();
      toggleParamInputs(indicator.id, checkbox.checked);
      rebuildCharts();
      await refreshSnapshot();
    });

    const content = document.createElement("div");
    content.className = "indicator-body";
    content.innerHTML = `<div><strong>${indicator.name}</strong><span>${indicator.description}</span></div>`;

    const paramsWrap = document.createElement("div");
    paramsWrap.className = "indicator-params";
    paramsWrap.dataset.indicatorId = indicator.id;

    (indicator.params || []).forEach((param) => {
      const field = document.createElement("label");
      field.className = "indicator-param";

      const title = document.createElement("small");
      title.textContent = param.label;

      const input = document.createElement("input");
      input.type = "number";
      input.value = state.indicatorParams[indicator.id]?.[param.key] ?? param.default;
      input.dataset.indicatorId = indicator.id;
      input.dataset.paramKey = param.key;
      input.step = param.step ?? "any";
      if (param.min !== undefined) {
        input.min = param.min;
      }
      if (param.max !== undefined) {
        input.max = param.max;
      }
      input.disabled = !checkbox.checked;
      input.addEventListener("change", async (event) => {
        updateIndicatorParamState(indicator.id, param.key, event.target.value);
        if (checkbox.checked) {
          await refreshSnapshot();
        }
      });

      field.append(title, input);
      paramsWrap.append(field);
    });

    if ((indicator.params || []).length > 0) {
      content.append(paramsWrap);
    }

    label.append(checkbox, content);
    els.indicatorForm.append(label);
  });
}

function toggleParamInputs(indicatorId, enabled) {
  els.indicatorForm.querySelectorAll(`[data-indicator-id="${indicatorId}"][data-param-key]`).forEach((input) => {
    input.disabled = !enabled;
  });
}

function paneLayoutFor(indicators) {
  const panes = ["price"];
  indicators.forEach((item) => {
    if (item.pane === "indicator") {
      panes.push(item.id);
    }
  });
  return panes;
}

function syncRange(sourceChart, targetChart) {
  sourceChart.timeScale().subscribeVisibleLogicalRangeChange((range) => {
    if (range && !state.isAdjustingRange) {
      targetChart.timeScale().setVisibleLogicalRange(range);
    }
  });
}

function seriesValueAtPoint(point) {
  if (!point) {
    return null;
  }
  if (typeof point.value === "number") {
    return point.value;
  }
  if (typeof point.close === "number") {
    return point.close;
  }
  if (typeof point.high === "number") {
    return point.high;
  }
  if (typeof point.open === "number") {
    return point.open;
  }
  return null;
}

function findSeriesPointAtTime(seriesKey, time) {
  const points = state.seriesDataByKey.get(seriesKey) || [];
  return points.find((point) => point && point.time === time) || null;
}

function setSeriesData(seriesKey, series, data) {
  series.setData(data);
  state.seriesDataByKey.set(seriesKey, data);
}

function primarySeriesKeyForPane(paneId) {
  if (paneId === "price") {
    return "candles";
  }
  return state.primarySeriesKeyByPane.get(paneId) || null;
}

function syncCrosshair(sourcePaneId, param) {
  if (state.isSyncingCrosshair) {
    return;
  }

  const time = param?.time;
  if (time === undefined) {
    els.cursorTime.textContent = "--";
    state.isSyncingCrosshair = true;
    state.charts.forEach((entry) => {
      entry.chart.clearCrosshairPosition();
    });
    state.isSyncingCrosshair = false;
    return;
  }

  els.cursorTime.textContent = formatCrosshairTime(time);
  state.isSyncingCrosshair = true;
  state.charts.forEach((entry) => {
    if (entry.paneId === sourcePaneId) {
      return;
    }

    const seriesKey = primarySeriesKeyForPane(entry.paneId);
    if (!seriesKey) {
      entry.chart.clearCrosshairPosition();
      return;
    }

    const series = state.seriesByKey.get(seriesKey);
    const point = findSeriesPointAtTime(seriesKey, time);
    const value = seriesValueAtPoint(point);
    if (!series || value === null) {
      entry.chart.clearCrosshairPosition();
      return;
    }

    entry.chart.setCrosshairPosition(value, time, series);
  });
  state.isSyncingCrosshair = false;
}

function clampVisibleRange(chart) {
  chart.timeScale().subscribeVisibleLogicalRangeChange((range) => {
    if (!range || state.isAdjustingRange) {
      return;
    }

    const span = range.to - range.from;
    const nextFrom = Math.max(range.from, 0);
    const nextTo = Math.max(range.to, nextFrom + 1);
    const nextSpan = nextTo - nextFrom;

    if (nextSpan <= MAX_VISIBLE_BARS && nextFrom === range.from && nextTo === range.to) {
      return;
    }

    state.isAdjustingRange = true;
    chart.timeScale().setVisibleLogicalRange({
      from: nextSpan > MAX_VISIBLE_BARS ? nextTo - MAX_VISIBLE_BARS : nextFrom,
      to: nextTo,
    });
    state.isAdjustingRange = false;
  });
}

function focusRecentBars(chart, barCount) {
  const visibleBars = Math.min(DEFAULT_VISIBLE_BARS, barCount);
  chart.timeScale().setVisibleLogicalRange({
    from: Math.max(0, barCount - visibleBars),
    to: barCount - 1 + RANGE_RIGHT_PADDING_BARS,
  });
}

function firstDefinedPointIndex(points) {
  return points.findIndex((point) => point && point.value !== null && point.value !== undefined);
}

function indicatorReadyIndex(snapshot) {
  const seriesIndices = snapshot.indicators.flatMap((indicator) =>
    indicator.series
      .map((series) => firstDefinedPointIndex(series.data))
      .filter((index) => index >= 0)
  );

  if (seriesIndices.length === 0) {
    return 0;
  }
  return Math.max(...seriesIndices);
}

function trimSnapshotForDisplay(snapshot) {
  const trimCount = Math.min(DISPLAY_WARMUP_BARS, snapshot.candles.length);
  if (trimCount <= 0) {
    return snapshot;
  }

  return {
    ...snapshot,
    candles: snapshot.candles.slice(trimCount),
    volume: snapshot.volume.slice(trimCount),
    indicators: snapshot.indicators.map((indicator) => ({
      ...indicator,
      series: indicator.series.map((series) => ({
        ...series,
        data: series.data.slice(trimCount),
      })),
    })),
  };
}

function focusComputedBars(chart, snapshot) {
  const barCount = snapshot.candles.length;
  if (barCount === 0) {
    return;
  }

  const readyIndex = indicatorReadyIndex(snapshot);
  const visibleBars = Math.min(DEFAULT_VISIBLE_BARS, Math.max(1, barCount - readyIndex));
  const from = Math.max(readyIndex, barCount - visibleBars);
  chart.timeScale().setVisibleLogicalRange({
    from,
    to: barCount - 1 + RANGE_RIGHT_PADDING_BARS,
  });
}

function chartHeight(index, total) {
  if (index === 0) {
    return total === 1 ? 100 : 68;
  }
  return Math.max(18, Math.floor(32 / (total - 1)));
}

function rebuildCharts() {
  state.charts.forEach((entry) => entry.chart.remove());
  state.charts = [];
  state.seriesByKey = new Map();
  state.seriesDataByKey = new Map();
  state.primarySeriesKeyByPane = new Map();
  state.currentPriceLine = null;
  state.hasFitted = false;
  els.chartStack.innerHTML = "";

  const activeIndicators = state.config.indicators.filter((item) => state.selectedIndicators.includes(item.id));
  const panes = paneLayoutFor(activeIndicators);

  panes.forEach((paneId, index) => {
    const pane = document.createElement("div");
    pane.className = "chart-pane";
    pane.style.height = `${chartHeight(index, panes.length)}%`;
    els.chartStack.appendChild(pane);

    const chart = LightweightCharts.createChart(pane, {
      width: pane.clientWidth || 800,
      height: pane.clientHeight || 300,
      ...chartTheme,
    });

    state.charts.push({ paneId, container: pane, chart });
    chart.subscribeCrosshairMove((param) => {
      syncCrosshair(paneId, param);
    });
  });

  for (let i = 0; i < state.charts.length - 1; i += 1) {
    syncRange(state.charts[i].chart, state.charts[i + 1].chart);
    syncRange(state.charts[i + 1].chart, state.charts[i].chart);
  }

  const priceChart = state.charts[0].chart;
  const candleSeries = priceChart.addCandlestickSeries({
    upColor: "#197278",
    downColor: "#c44536",
    borderVisible: false,
    wickUpColor: "#197278",
    wickDownColor: "#c44536",
    priceLineVisible: false,
    autoscaleInfoProvider: (original) => {
      const autoscaleInfo = original();
      if (!autoscaleInfo || !autoscaleInfo.priceRange) {
        return autoscaleInfo;
      }

      const minValue = autoscaleInfo.priceRange.minValue;
      const maxValue = autoscaleInfo.priceRange.maxValue;
      const range = Math.max(maxValue - minValue, Math.abs(maxValue) * 0.01, 1e-6);
      return {
        ...autoscaleInfo,
        priceRange: {
          minValue: minValue - range * PRICE_RANGE_BOTTOM_PADDING,
          maxValue: maxValue + range * PRICE_RANGE_TOP_PADDING,
        },
      };
    },
  });
  const volumeSeries = priceChart.addHistogramSeries({
    priceScaleId: "",
    priceFormat: { type: "volume" },
  });
  priceChart.priceScale("").applyOptions({
    scaleMargins: { top: 0.78, bottom: 0 },
  });
  priceChart.timeScale().applyOptions({
    visible: state.charts.length === 1,
  });
  clampVisibleRange(priceChart);
  state.charts.slice(1).forEach((entry, index) => {
    entry.chart.timeScale().applyOptions({
      visible: index === state.charts.length - 2,
    });
    entry.chart.priceScale("right").applyOptions({
      autoScale: true,
      scaleMargins: { top: 0.18, bottom: 0.18 },
    });
  });

  state.seriesByKey.set("candles", candleSeries);
  state.seriesByKey.set("volume", volumeSeries);
}

function createSeries(chart, definition) {
  switch (definition.series_type) {
    case "line":
      return chart.addLineSeries(definition.options || {});
    case "histogram":
      return chart.addHistogramSeries(definition.options || {});
    case "area":
      return chart.addAreaSeries(definition.options || {});
    default:
      throw new Error(`暂不支持的序列类型: ${definition.series_type}`);
  }
}

function indicatorPaneId(indicator) {
  return indicator.pane === "price" ? "price" : indicator.id;
}

function formatCrosshairTime(time) {
  if (typeof time === "number") {
    return new Date(time * 1000).toLocaleString("zh-CN", {
      hour12: false,
    });
  }
  if (time && typeof time === "object" && "year" in time) {
    const month = String(time.month).padStart(2, "0");
    const day = String(time.day).padStart(2, "0");
    return `${time.year}-${month}-${day}`;
  }
  return "--";
}

function applySnapshot(snapshot) {
  els.error.textContent = "";
  state.activeSymbol = snapshot.symbol;
  state.activeDurationSeconds = snapshot.duration_seconds;
  els.symbolSelect.value = snapshot.symbol;
  els.durationSelect.value = String(snapshot.duration_seconds);
  syncMarketHeader(snapshot.symbol_label || snapshot.symbol, snapshot.duration_seconds);
  els.lastPrice.textContent = snapshot.last_close.toFixed(2);
  els.lastPrice.style.color = snapshot.last_color;
  els.lastUpdate.textContent = snapshot.last_time;

  const displaySnapshot = trimSnapshotForDisplay(snapshot);
  const candleSeries = state.seriesByKey.get("candles");
  const volumeSeries = state.seriesByKey.get("volume");
  setSeriesData("candles", candleSeries, displaySnapshot.candles);
  setSeriesData("volume", volumeSeries, displaySnapshot.volume);

  if (state.currentPriceLine) {
    candleSeries.removePriceLine(state.currentPriceLine);
  }
  state.currentPriceLine = candleSeries.createPriceLine({
    price: snapshot.last_close,
    color: snapshot.last_color,
    lineWidth: 1,
    lineStyle: LightweightCharts.LineStyle.Dashed,
    axisLabelVisible: true,
    title: "现价",
  });

  displaySnapshot.indicators.forEach((indicator) => {
    const paneId = indicatorPaneId(indicator);
    const paneEntry = state.charts.find((item) => item.paneId === paneId);
    if (!paneEntry) {
      return;
    }

    indicator.series.forEach((seriesDefinition) => {
      const key = `indicator:${indicator.id}:${seriesDefinition.id}`;
      let series = state.seriesByKey.get(key);
      if (!series) {
        series = createSeries(paneEntry.chart, seriesDefinition);
        state.seriesByKey.set(key, series);
        if (!state.primarySeriesKeyByPane.has(paneId)) {
          state.primarySeriesKeyByPane.set(paneId, key);
        }
      }
      setSeriesData(key, series, seriesDefinition.data);
    });
  });

  if (!state.hasFitted && state.charts.length > 0) {
    if (displaySnapshot.indicators.length > 0) {
      focusComputedBars(state.charts[0].chart, displaySnapshot);
    } else {
      focusRecentBars(state.charts[0].chart, displaySnapshot.candles.length);
    }
    state.hasFitted = true;
  }
}

async function refreshSnapshot() {
  const params = new URLSearchParams();
  params.set("symbol", getRequestedSymbol());
  params.set("duration_seconds", String(getRequestedDuration()));
  if (state.selectedIndicators.length) {
    params.set("indicators", state.selectedIndicators.join(","));
    const selectedParams = {};
    state.selectedIndicators.forEach((indicatorId) => {
      selectedParams[indicatorId] = state.indicatorParams[indicatorId] || {};
    });
    params.set("indicator_params", JSON.stringify(selectedParams));
  }
  const query = params.toString();
  const snapshot = await fetchJson(`/api/snapshot${query ? `?${query}` : ""}`);
  applySnapshot(snapshot);
}

function resizeCharts() {
  state.charts.forEach((entry) => {
    entry.chart.applyOptions({
      width: entry.container.clientWidth,
      height: entry.container.clientHeight,
    });
  });
}

async function boot() {
  state.config = await fetchJson("/api/config");
  state.activeSymbol = state.config.symbol;
  state.activeDurationSeconds = state.config.duration_seconds;
  state.selectedIndicators = [...state.config.default_indicator_ids];
  state.indicatorParams = getDefaultIndicatorParams(state.config.indicators);

  els.provider.textContent = state.config.provider;
  buildContractOptions(state.config.contracts || [], state.config.symbol);
  buildDurationOptions(state.config.duration_options || [state.config.duration_seconds], state.config.duration_seconds);
  syncMarketHeader(state.config.symbol_label || state.config.symbol, state.config.duration_seconds);

  els.symbolSelect.addEventListener("change", async () => {
    try {
      await refreshSnapshot();
    } catch (error) {
      els.error.textContent = error.message;
    }
  });
  els.durationSelect.addEventListener("change", async () => {
    try {
      await refreshSnapshot();
    } catch (error) {
      els.error.textContent = error.message;
    }
  });

  buildIndicatorSelector(state.config.indicators, state.config.default_indicator_ids);
  rebuildCharts();
  await refreshSnapshot();
  setInterval(async () => {
    try {
      await refreshSnapshot();
    } catch (error) {
      els.error.textContent = error.message;
    }
  }, state.config.refresh_ms);
}

window.addEventListener("resize", resizeCharts);

boot().catch((error) => {
  els.error.textContent = error.message;
});
