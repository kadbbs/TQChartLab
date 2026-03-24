const state = {
  config: null,
  activeProvider: "",
  activeSymbol: "",
  activeDurationSeconds: null,
  activeBarMode: "time",
  activeRangeTicks: 10,
  activeBrickLength: 10000,
  selectedIndicators: [],
  indicatorParams: {},
  charts: [],
  seriesByKey: new Map(),
  seriesChartByKey: new Map(),
  seriesDataByKey: new Map(),
  primarySeriesKeyByPane: new Map(),
  bandPrimitiveByKey: new Map(),
  currentPriceLine: null,
  hasFitted: false,
  isSyncingCrosshair: false,
  refreshTimerId: null,
  snapshotRequestId: 0,
  configRequestId: 0,
  requestedDataLength: null,
  historyExpandInFlight: false,
  pendingHistoryRange: null,
};

const DEFAULT_VISIBLE_BARS = 120;
const MIN_VISIBLE_DATA_BARS = 60;
const RANGE_RIGHT_PADDING_BARS = 8;
const PRICE_RANGE_TOP_PADDING = 0.1;
const PRICE_RANGE_BOTTOM_PADDING = 0.14;
const PRICE_RANGE_BOTTOM_PADDING_WITH_VOLUME_PANE = 0.02;
const RIGHT_PRICE_SCALE_MIN_WIDTH = 72;
const RENKO_DEFAULT_TICKS = 5;
const RANGE_DEFAULT_TICKS = 10;
const VOLUME_PANE_ID = "__volume__";
const HISTORY_EXPAND_LEFT_THRESHOLD = 20;
const MAX_DUCKDB_DATA_LENGTH = 50000;
const MAX_DUCKDB_BRICK_LENGTH = 100000;

function paneLabelConfig(paneId) {
  if (paneId === "pseudo_orderflow_5m") {
    return [
      { text: "Delta>0", top: "12%" },
      { text: "DeltaRatio>均值20", top: "29%" },
      { text: "dOI>0", top: "46%" },
      { text: "盘口尾值>0", top: "63%" },
      { text: "Efficiency>中位20", top: "80%" },
    ];
  }
  return [];
}

function formatAxisTimeLabel(time) {
  const resolved = resolveDisplayTime(time);
  if (resolved) {
    const [datePart, timePart = ""] = resolved.split(" ");
    const [, month = "", day = ""] = datePart.split("-");
    return timePart ? `${month}-${day} ${timePart.slice(0, 5)}` : `${month}-${day}`;
  }
  if (typeof time === "number") {
    return new Date(time * 1000).toLocaleString("zh-CN", {
      hour12: false,
      month: "2-digit",
      day: "2-digit",
      hour: "2-digit",
      minute: "2-digit",
    });
  }
  if (time && typeof time === "object" && "year" in time) {
    const month = String(time.month).padStart(2, "0");
    const day = String(time.day).padStart(2, "0");
    return `${month}-${day}`;
  }
  return "";
}

function resolveDisplayTime(time) {
  if (typeof time !== "number") {
    return null;
  }

  const candidates = [
    String(time),
    String(Math.round(time)),
    String(Math.floor(time)),
    String(Math.ceil(time)),
  ];

  if (Math.abs(time) < 1e9) {
    candidates.push(
      String(Math.round(time * 1000)),
      String(Math.floor(time * 1000)),
      String(Math.ceil(time * 1000))
    );
  }

  if (Math.abs(time) > 1e11) {
    candidates.push(
      String(Math.round(time / 1000)),
      String(Math.floor(time / 1000)),
      String(Math.ceil(time / 1000))
    );
  }

  for (const candidate of candidates) {
    const label = state.timeLabels.get(candidate);
    if (label) {
      return label;
    }
  }

  if (state.timeLabels.size > 0) {
    let nearestLabel = null;
    let nearestDiff = Number.POSITIVE_INFINITY;
    const maxDiff = 2;
    for (const [key, label] of state.timeLabels.entries()) {
      const numericKey = Number(key);
      if (!Number.isFinite(numericKey) || !label) {
        continue;
      }
      const diff = Math.abs(numericKey - time);
      if (diff < nearestDiff) {
        nearestDiff = diff;
        nearestLabel = label;
      }
    }
    if (nearestLabel && nearestDiff <= maxDiff) {
      return nearestLabel;
    }
  }

  return null;
}

class LineBandPrimitiveRenderer {
  constructor(source) {
    this._source = source;
  }

  draw(target) {
    const segments = this._source.coordinateSegments();
    if (!segments.length) {
      return;
    }

    target.useMediaCoordinateSpace((scope) => {
      const { context } = scope;
      context.save();
      context.fillStyle = this._source.fillColor();

      segments.forEach((segment) => {
        if (segment.length < 2) {
          return;
        }
        context.beginPath();
        context.moveTo(segment[0].x, segment[0].y1);
        for (let index = 1; index < segment.length; index += 1) {
          context.lineTo(segment[index].x, segment[index].y1);
        }
        for (let index = segment.length - 1; index >= 0; index -= 1) {
          context.lineTo(segment[index].x, segment[index].y2);
        }
        context.closePath();
        context.fill();
      });

      context.restore();
    });
  }
}

class LineBandPrimitivePaneView {
  constructor(source) {
    this._source = source;
    this._renderer = new LineBandPrimitiveRenderer(source);
  }

  zOrder() {
    return "bottom";
  }

  renderer() {
    return this._renderer;
  }
}

class LineBandPrimitive {
  constructor(fillColor) {
    this._fillColor = fillColor;
    this._chart = null;
    this._primarySeries = null;
    this._secondarySeries = null;
    this._primaryData = [];
    this._secondaryData = [];
    this._requestUpdate = null;
    this._paneViews = [new LineBandPrimitivePaneView(this)];
  }

  attached({ chart, series, requestUpdate }) {
    this._chart = chart;
    this._primarySeries = series;
    this._requestUpdate = requestUpdate;
  }

  detached() {
    this._chart = null;
    this._primarySeries = null;
    this._requestUpdate = null;
  }

  updateAllViews() {
    this._requestUpdate?.();
  }

  paneViews() {
    return this._paneViews;
  }

  fillColor() {
    return this._fillColor;
  }

  setFillColor(fillColor) {
    this._fillColor = fillColor;
    this.updateAllViews();
  }

  setSecondarySeries(series) {
    this._secondarySeries = series;
    this.updateAllViews();
  }

  setData(primaryData, secondaryData) {
    this._primaryData = primaryData || [];
    this._secondaryData = secondaryData || [];
    this.updateAllViews();
  }

  coordinateSegments() {
    if (!this._chart || !this._primarySeries || !this._secondarySeries) {
      return [];
    }

    const timeScale = this._chart.timeScale();
    const secondaryByTime = new Map(
      this._secondaryData
        .filter((point) => typeof point?.value === "number")
        .map((point) => [String(point.time), point.value])
    );

    const segments = [];
    let currentSegment = [];

    const flush = () => {
      if (currentSegment.length) {
        segments.push(currentSegment);
        currentSegment = [];
      }
    };

    this._primaryData.forEach((point) => {
      if (typeof point?.value !== "number") {
        flush();
        return;
      }

      const pairedValue = secondaryByTime.get(String(point.time));
      if (typeof pairedValue !== "number") {
        flush();
        return;
      }

      const x = timeScale.timeToCoordinate(point.time);
      const y1 = this._primarySeries.priceToCoordinate(point.value);
      const y2 = this._secondarySeries.priceToCoordinate(pairedValue);
      if (!Number.isFinite(x) || !Number.isFinite(y1) || !Number.isFinite(y2)) {
        flush();
        return;
      }

      currentSegment.push({ x, y1, y2 });
    });

    flush();
    return segments;
  }
}

const els = {
  title: document.getElementById("page-title"),
  provider: document.getElementById("provider-name"),
  providerSelect: document.getElementById("provider-select"),
  providerHint: document.getElementById("provider-hint"),
  symbol: document.getElementById("symbol-name"),
  duration: document.getElementById("duration-name"),
  symbolSelect: document.getElementById("symbol-select"),
  barModeSelect: document.getElementById("bar-mode-select"),
  durationSelect: document.getElementById("duration-select"),
  barSizeLabel: document.getElementById("bar-size-label"),
  rangeTicksInput: document.getElementById("range-ticks-input"),
  historySizeLabel: document.getElementById("history-size-label"),
  brickLengthInput: document.getElementById("brick-length-input"),
  lastPrice: document.getElementById("last-price"),
  lastUpdate: document.getElementById("last-update"),
  cursorTime: document.getElementById("cursor-time"),
  contractDetailCard: document.getElementById("contract-detail-card"),
  detailFirstTick: document.getElementById("detail-first-tick"),
  detailLastTick: document.getElementById("detail-last-tick"),
  detailTickCount: document.getElementById("detail-tick-count"),
  detailPriceTick: document.getElementById("detail-price-tick"),
  detailContractMonth: document.getElementById("detail-contract-month"),
  detailVolumeMultiple: document.getElementById("detail-volume-multiple"),
  indicatorForm: document.getElementById("indicator-form"),
  chartStack: document.getElementById("chart-stack"),
  error: document.getElementById("error-message"),
};

state.timeLabels = new Map();

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
    mode: LightweightCharts.CrosshairMode.Magnet,
    vertLine: { color: "#a24f2f", labelBackgroundColor: "#a24f2f" },
    horzLine: { color: "#a24f2f", labelBackgroundColor: "#a24f2f" },
  },
  rightPriceScale: {
    borderColor: "rgba(92, 70, 47, 0.18)",
    autoScale: true,
    minimumWidth: RIGHT_PRICE_SCALE_MIN_WIDTH,
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
    tickMarkFormatter: (time) => formatAxisTimeLabel(time),
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
    timeFormatter: (time) => {
      const resolved = resolveDisplayTime(time);
      if (resolved) {
        return resolved;
      }
      if (state.activeBarMode === "time") {
        return "";
      }
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
      return "";
    },
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
  return [...els.indicatorForm.querySelectorAll('input[data-role="indicator-toggle"]:checked')].map((item) => item.value);
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

function indicatorParamValue(indicatorId, param) {
  const currentValue = state.indicatorParams[indicatorId]?.[param.key];
  return currentValue ?? param.default;
}

function createIndicatorParamInput(indicatorId, param, enabled) {
  let input;
  const currentValue = indicatorParamValue(indicatorId, param);

  if (param.type === "bool") {
    input = document.createElement("input");
    input.type = "checkbox";
    input.checked = Boolean(currentValue);
  } else if (Array.isArray(param.options) && param.options.length > 0) {
    input = document.createElement("select");
    param.options.forEach((optionValue) => {
      const option = document.createElement("option");
      option.value = String(optionValue);
      option.textContent = String(optionValue);
      option.selected = String(currentValue) === String(optionValue);
      input.append(option);
    });
  } else if (param.type === "int" || param.type === "float") {
    input = document.createElement("input");
    input.type = "number";
    input.value = currentValue;
    input.step = param.step ?? "any";
    if (param.min !== undefined) {
      input.min = param.min;
    }
    if (param.max !== undefined) {
      input.max = param.max;
    }
  } else {
    input = document.createElement("input");
    input.type = "text";
    input.value = currentValue;
  }

  input.dataset.indicatorId = indicatorId;
  input.dataset.paramKey = param.key;
  input.disabled = !enabled;
  input.addEventListener("change", async (event) => {
    const nextValue = param.type === "bool" ? event.target.checked : event.target.value;
    updateIndicatorParamState(indicatorId, param.key, nextValue);
    if (enabled) {
      await refreshSnapshot();
    }
  });
  return input;
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

function formatBarModeLabel(barMode, durationSeconds, rangeTicks) {
  if (barMode === "tick") {
    return "Tick 图";
  }
  if (barMode === "range") {
    return `${rangeTicks} Tick Range`;
  }
  if (barMode === "renko") {
    return `${rangeTicks} Tick Renko`;
  }
  return formatDurationLabel(durationSeconds);
}

function syncMarketHeader(symbolLabel, durationSeconds, barMode, rangeTicks) {
  els.title.textContent = `${symbolLabel} 图表工作台`;
  els.symbol.textContent = symbolLabel;
  els.duration.textContent = formatBarModeLabel(barMode, durationSeconds, rangeTicks);
}

function formatDetailValue(value, fallback = "--") {
  if (value === null || value === undefined) {
    return fallback;
  }
  const text = String(value).trim();
  return text ? text : fallback;
}

function formatNumberValue(value, digits = null) {
  if (value === null || value === undefined || value === "") {
    return "--";
  }
  const number = Number(value);
  if (!Number.isFinite(number)) {
    return "--";
  }
  if (digits === null) {
    return number.toLocaleString("zh-CN");
  }
  return number.toLocaleString("zh-CN", {
    minimumFractionDigits: digits,
    maximumFractionDigits: digits,
  });
}

function renderProviderMeta(payload) {
  const provider = payload.provider || state.activeProvider || state.config?.provider || "";
  els.provider.textContent = provider || "--";
  els.providerHint.textContent = payload.provider_hint || "当前数据源暂无额外说明。";

  const detail = payload.contract_detail || {};
  const localCount =
    Number(detail.tick_count || 0) +
    Number(detail.bar_1m_count || 0) +
    Number(detail.bar_5m_count || 0) +
    Number(detail.bar_10m_count || 0) +
    Number(detail.bar_15m_count || 0);
  const hasLocalCoverage =
    provider === "duckdb" &&
    (detail.first_data_at || detail.last_data_at || localCount > 0);

  els.contractDetailCard.hidden = !hasLocalCoverage;
  els.detailFirstTick.textContent = formatDetailValue(detail.first_data_at || detail.first_tick_at);
  els.detailLastTick.textContent = formatDetailValue(detail.last_data_at || detail.last_tick_at);
  els.detailTickCount.textContent = formatNumberValue(localCount);
  els.detailPriceTick.textContent = formatNumberValue(detail.price_tick, 4);
  els.detailContractMonth.textContent = formatDetailValue(detail.contract_month);
  els.detailVolumeMultiple.textContent = formatNumberValue(detail.volume_multiple, 0);
}

function hasDuckdbLocalData(contract) {
  return (
    Number(contract?.tick_count || 0) > 0 ||
    Number(contract?.bar_1m_count || 0) > 0 ||
    Number(contract?.bar_5m_count || 0) > 0 ||
    Number(contract?.bar_10m_count || 0) > 0 ||
    Number(contract?.bar_15m_count || 0) > 0
  );
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

function buildProviderOptions(options, activeValue) {
  els.providerSelect.innerHTML = "";
  options.forEach((providerId) => {
    const option = document.createElement("option");
    option.value = providerId;
    option.textContent = providerId;
    option.selected = providerId === activeValue;
    els.providerSelect.append(option);
  });
}

function buildBarModeOptions(options, activeValue) {
  els.barModeSelect.innerHTML = "";
  options.forEach((mode) => {
    const option = document.createElement("option");
    option.value = mode.id;
    option.textContent = mode.label;
    option.selected = mode.id === activeValue;
    els.barModeSelect.append(option);
  });
}

function buildContractOptions(contracts, activeSymbol) {
  els.symbolSelect.innerHTML = "";
  const provider = getRequestedProvider() || state.activeProvider || state.config?.provider || "";
  const normalizedContracts = contracts.length
    ? contracts
    : [{ symbol: activeSymbol, label: activeSymbol }];
  normalizedContracts.forEach((contract) => {
    const option = document.createElement("option");
    option.value = contract.symbol;
    const hasLocalData = provider !== "duckdb" || hasDuckdbLocalData(contract);
    option.textContent = hasLocalData ? contract.label : `${contract.label}（无本地数据）`;
    option.disabled = !hasLocalData;
    option.selected = contract.symbol === activeSymbol;
    els.symbolSelect.append(option);
  });
}

function getRequestedSymbol() {
  return els.symbolSelect.value || state.activeSymbol || state.config.symbol;
}

function getRequestedProvider() {
  return els.providerSelect.value || state.config.provider;
}

function getRequestedDuration() {
  return Number(els.durationSelect.value || state.config.duration_seconds);
}

function getRequestedBarMode() {
  return els.barModeSelect.value || state.config.bar_mode || "time";
}

function getRequestedRangeTicks() {
  const value = Number(els.rangeTicksInput.value || state.config.range_ticks || 10);
  return Number.isFinite(value) && value > 0 ? Math.round(value) : 10;
}

function getRequestedBrickLength() {
  const value = Number(els.brickLengthInput.value || state.config.brick_length || 10000);
  return Number.isFinite(value) && value > 0 ? Math.round(value) : 10000;
}

function defaultTicksForBarMode(barMode) {
  if (barMode === "renko") {
    return RENKO_DEFAULT_TICKS;
  }
  if (barMode === "range") {
    return RANGE_DEFAULT_TICKS;
  }
  return state.config?.range_ticks || RANGE_DEFAULT_TICKS;
}

function syncBarModeControls(barMode) {
  const usesDuration = barMode === "time";
  const usesTicks = barMode === "range" || barMode === "renko";
  const usesHistoryLength = barMode === "tick" || barMode === "range" || barMode === "renko";
  els.durationSelect.disabled = !usesDuration;
  els.rangeTicksInput.disabled = !usesTicks;
  els.brickLengthInput.disabled = !usesHistoryLength;
  if (barMode === "renko") {
    els.barSizeLabel.textContent = "Renko Tick";
    els.historySizeLabel.textContent = "砖图根数";
  } else if (barMode === "range") {
    els.barSizeLabel.textContent = "Range Tick";
    els.historySizeLabel.textContent = "砖图根数";
  } else if (barMode === "tick") {
    els.barSizeLabel.textContent = "价格 Tick";
    els.historySizeLabel.textContent = "Tick 根数";
  } else {
    els.barSizeLabel.textContent = "价格 Tick";
    els.historySizeLabel.textContent = "显示根数";
  }
}

function sanitizePricePaneIndicators(snapshot) {
  if (snapshot.bar_mode !== "time" || snapshot.candles.length === 0) {
    return snapshot;
  }

  const lows = snapshot.candles.map((item) => item.low).filter((value) => Number.isFinite(value));
  const highs = snapshot.candles.map((item) => item.high).filter((value) => Number.isFinite(value));
  if (lows.length === 0 || highs.length === 0) {
    return snapshot;
  }

  const candleMin = Math.min(...lows);
  const candleMax = Math.max(...highs);
  const lowerBound = candleMin > 0 ? candleMin * 0.5 : candleMin - Math.abs(candleMax - candleMin) * 2;
  const upperBound = candleMax > 0 ? candleMax * 1.5 : candleMax + Math.abs(candleMax - candleMin) * 2;

  return {
    ...snapshot,
    indicators: snapshot.indicators.map((indicator) => {
      if (indicator.pane !== "price") {
        return indicator;
      }
      return {
        ...indicator,
        series: indicator.series.map((series) => ({
          ...series,
          data: series.data.map((point) => {
            if (typeof point?.value !== "number") {
              return point;
            }
            if (point.value < lowerBound || point.value > upperBound) {
              return { ...point, value: null };
            }
            return point;
          }),
        })),
      };
    }),
  };
}

function buildIndicatorSelector(indicators, defaults) {
  els.indicatorForm.innerHTML = "";
  indicators.forEach((indicator) => {
    const label = document.createElement("label");
    label.className = "indicator-item";

    const checkbox = document.createElement("input");
    checkbox.type = "checkbox";
    checkbox.value = indicator.id;
    checkbox.dataset.role = "indicator-toggle";
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

      const input = createIndicatorParamInput(indicator.id, param, checkbox.checked);

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
  if (indicators.some((item) => item.pane === "indicator")) {
    panes.push(VOLUME_PANE_ID);
  }
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

function removeSeriesByKey(seriesKey) {
  const primitive = state.bandPrimitiveByKey.get(seriesKey);
  const series = state.seriesByKey.get(seriesKey);
  if (primitive && series?.detachPrimitive) {
    series.detachPrimitive(primitive);
  }
  state.bandPrimitiveByKey.delete(seriesKey);

  const chart = state.seriesChartByKey.get(seriesKey);
  if (chart && series) {
    chart.removeSeries(series);
  }

  state.seriesByKey.delete(seriesKey);
  state.seriesChartByKey.delete(seriesKey);
  state.seriesDataByKey.delete(seriesKey);
  state.primarySeriesKeyByPane.forEach((value, key) => {
    if (value === seriesKey) {
      state.primarySeriesKeyByPane.delete(key);
    }
  });
}

function syncBandPrimitive(primaryKey, secondaryKey, fillColor) {
  const primarySeries = state.seriesByKey.get(primaryKey);
  const secondarySeries = state.seriesByKey.get(secondaryKey);
  if (!primarySeries || !secondarySeries || typeof primarySeries.attachPrimitive !== "function") {
    const existing = state.bandPrimitiveByKey.get(primaryKey);
    if (existing && primarySeries?.detachPrimitive) {
      primarySeries.detachPrimitive(existing);
      state.bandPrimitiveByKey.delete(primaryKey);
    }
    return;
  }

  let primitive = state.bandPrimitiveByKey.get(primaryKey);
  if (!primitive) {
    primitive = new LineBandPrimitive(fillColor);
    primarySeries.attachPrimitive(primitive);
    state.bandPrimitiveByKey.set(primaryKey, primitive);
  }

  primitive.setFillColor(fillColor);
  primitive.setSecondarySeries(secondarySeries);
  primitive.setData(
    state.seriesDataByKey.get(primaryKey) || [],
    state.seriesDataByKey.get(secondaryKey) || []
  );
}

function primarySeriesKeyForPane(paneId) {
  if (paneId === "price") {
    return "candles";
  }
  if (paneId === VOLUME_PANE_ID) {
    return "volume";
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
  const readyIndex = indicatorReadyIndex(snapshot);
  const maxTrim = Math.max(snapshot.candles.length - MIN_VISIBLE_DATA_BARS, 0);
  const trimCount = Math.min(Math.max(readyIndex, 0), maxTrim);
  if (trimCount <= 0) {
    return snapshot;
  }

  const timeLabels = Object.fromEntries(
    snapshot.candles.slice(trimCount).map((candle) => [String(candle.time), snapshot.time_labels?.[String(candle.time)] || ""])
  );

  return {
    ...snapshot,
    time_labels: timeLabels,
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

function paneHeights(panes) {
  const total = panes.length;
  const hasVolumePane = panes.includes(VOLUME_PANE_ID);

  if (!hasVolumePane) {
    if (total <= 1) {
      return [100];
    }
    if (total === 2) {
      return [80, 20];
    }
    if (total === 3) {
      return [72, 14, 14];
    }
    if (total === 4) {
      return [64, 12, 12, 12];
    }

    const pricePaneHeight = 58;
    const secondaryPaneHeight = (100 - pricePaneHeight) / (total - 1);
    return [pricePaneHeight, ...Array.from({ length: total - 1 }, () => secondaryPaneHeight)];
  }

  const indicatorCount = total - 2;
  if (indicatorCount <= 0) {
    return [82, 18];
  }
  if (indicatorCount === 1) {
    return [46, 14, 40];
  }
  if (indicatorCount === 2) {
    return [44, 12, 22, 22];
  }
  if (indicatorCount === 3) {
    return [42, 10, 16, 16, 16];
  }

  const pricePaneHeight = 46;
  const volumePaneHeight = 10;
  const secondaryPaneHeight = (100 - pricePaneHeight - volumePaneHeight) / indicatorCount;
  return [
    pricePaneHeight,
    volumePaneHeight,
    ...Array.from({ length: indicatorCount }, () => secondaryPaneHeight),
  ];
}

function volumeOverlayScaleMargins(totalPanes) {
  if (totalPanes <= 1) {
    return { top: 0.82, bottom: 0.02 };
  }
  if (totalPanes === 2) {
    return { top: 0.8, bottom: 0.02 };
  }
  if (totalPanes === 3) {
    return { top: 0.82, bottom: 0.02 };
  }
  return { top: 0.84, bottom: 0.02 };
}

function currentRequestedDataLength() {
  return state.requestedDataLength || state.config?.data_length || 800;
}

function maybeExpandDuckdbHistory(range) {
  if (state.activeProvider !== "duckdb" || state.historyExpandInFlight) {
    return;
  }
  if (!range || !Number.isFinite(range.from) || range.from > HISTORY_EXPAND_LEFT_THRESHOLD) {
    return;
  }

  let expanded = false;
  if (state.activeBarMode === "time") {
    const currentLength = currentRequestedDataLength();
    const nextLength = Math.min(
      MAX_DUCKDB_DATA_LENGTH,
      Math.max(currentLength + 500, Math.round(currentLength * 1.8))
    );
    if (nextLength > currentLength) {
      state.requestedDataLength = nextLength;
      expanded = true;
    }
  } else {
    const currentLength = getRequestedBrickLength();
    const nextLength = Math.min(
      MAX_DUCKDB_BRICK_LENGTH,
      Math.max(currentLength + 1000, Math.round(currentLength * 1.8))
    );
    if (nextLength > currentLength) {
      els.brickLengthInput.value = String(nextLength);
      expanded = true;
    }
  }

  if (!expanded) {
    return;
  }

  state.pendingHistoryRange = { from: range.from, to: range.to };
  state.historyExpandInFlight = true;
  refreshSnapshot().catch((error) => {
    els.error.textContent = error.message;
    state.historyExpandInFlight = false;
    state.pendingHistoryRange = null;
  });
}

function rebuildCharts() {
  state.charts.forEach((entry) => entry.chart.remove());
  state.charts = [];
  state.seriesByKey = new Map();
  state.seriesChartByKey = new Map();
  state.seriesDataByKey = new Map();
  state.primarySeriesKeyByPane = new Map();
  state.bandPrimitiveByKey = new Map();
  state.currentPriceLine = null;
  state.hasFitted = false;
  els.chartStack.innerHTML = "";

  const activeIndicators = state.config.indicators.filter((item) => state.selectedIndicators.includes(item.id));
  const panes = paneLayoutFor(activeIndicators);
  const heights = paneHeights(panes);
  els.chartStack.style.gap = panes.length <= 1 ? "8px" : panes.length === 2 ? "5px" : "3px";

  panes.forEach((paneId, index) => {
    const pane = document.createElement("div");
    pane.className = "chart-pane";
    pane.style.flexBasis = `${heights[index]}%`;
    pane.style.height = `${heights[index]}%`;
    pane.style.minHeight = paneId === "price" ? "220px" : paneId === VOLUME_PANE_ID ? "88px" : "96px";
    els.chartStack.appendChild(pane);

    const paneLabels = paneLabelConfig(paneId);
    if (paneLabels.length > 0) {
      const overlay = document.createElement("div");
      overlay.className = "pane-label-overlay";
      paneLabels.forEach((item) => {
        const label = document.createElement("div");
        label.className = "pane-label-tag";
        label.textContent = item.text;
        label.style.top = item.top;
        overlay.appendChild(label);
      });
      pane.appendChild(overlay);
    }

    const chart = LightweightCharts.createChart(pane, {
      width: pane.clientWidth || 800,
      height: pane.clientHeight || 300,
      ...chartTheme,
    });

    state.charts.push({ paneId, container: pane, chart });
    chart.subscribeCrosshairMove((param) => {
      syncCrosshair(paneId, param);
    });
    if (paneId === "price") {
      chart.timeScale().subscribeVisibleLogicalRangeChange((range) => {
        maybeExpandDuckdbHistory(range);
      });
    }
  });

  for (let i = 0; i < state.charts.length - 1; i += 1) {
    syncRange(state.charts[i].chart, state.charts[i + 1].chart);
    syncRange(state.charts[i + 1].chart, state.charts[i].chart);
  }

  const priceChart = state.charts[0].chart;
  const volumePaneEntry = state.charts.find((entry) => entry.paneId === VOLUME_PANE_ID);
  const volumeChart = volumePaneEntry?.chart || priceChart;
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
      const bottomPadding = volumePaneEntry
        ? PRICE_RANGE_BOTTOM_PADDING_WITH_VOLUME_PANE
        : PRICE_RANGE_BOTTOM_PADDING;
      return {
        ...autoscaleInfo,
        priceRange: {
          minValue: minValue - range * bottomPadding,
          maxValue: maxValue + range * PRICE_RANGE_TOP_PADDING,
        },
      };
    },
  });
  let volumeSeries;
  if (volumePaneEntry) {
    volumeSeries = volumeChart.addHistogramSeries({
      priceFormat: { type: "volume" },
    });
    volumeChart.priceScale("right").applyOptions({
      autoScale: true,
      minimumWidth: RIGHT_PRICE_SCALE_MIN_WIDTH,
      scaleMargins: { top: 0.08, bottom: 0.12 },
    });
  } else {
    volumeSeries = priceChart.addHistogramSeries({
      priceScaleId: "",
      priceFormat: { type: "volume" },
    });
    priceChart.priceScale("").applyOptions({
      scaleMargins: volumeOverlayScaleMargins(panes.length),
    });
  }

  state.charts.forEach((entry, index) => {
    const isLastPane = index === state.charts.length - 1;
    const isVolumePane = entry.paneId === VOLUME_PANE_ID;
    entry.chart.timeScale().applyOptions({
      visible: isLastPane,
    });

    if (entry.paneId !== "price" && !isVolumePane) {
      entry.chart.priceScale("right").applyOptions({
        autoScale: true,
        minimumWidth: RIGHT_PRICE_SCALE_MIN_WIDTH,
        scaleMargins: { top: 0.18, bottom: 0.18 },
      });
    }
  });

  state.seriesByKey.set("candles", candleSeries);
  state.seriesByKey.set("volume", volumeSeries);
  state.seriesChartByKey.set("candles", priceChart);
  state.seriesChartByKey.set("volume", volumeChart);
}

function createSeries(chart, definition) {
  const { fillToSeriesId, fillColor, markers, ...renderOptions } = definition.options || {};
  switch (definition.series_type) {
    case "line":
      return chart.addLineSeries(renderOptions);
    case "histogram":
      return chart.addHistogramSeries(renderOptions);
    case "area":
      return chart.addAreaSeries(renderOptions);
    default:
      throw new Error(`暂不支持的序列类型: ${definition.series_type}`);
  }
}

function indicatorPaneId(indicator) {
  return indicator.pane === "price" ? "price" : indicator.id;
}

function formatCrosshairTime(time) {
  const resolved = resolveDisplayTime(time);
  if (resolved) {
    return resolved;
  }
  if (state.activeBarMode === "time") {
    return "--";
  }
  if (typeof time === "number") {
    if (Math.abs(time) < 1e9) {
      return new Date(time * 1000 * 1000).toLocaleString("zh-CN", {
        hour12: false,
      });
    }
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
  const nextBarMode = snapshot.bar_mode || "time";
  const nextRangeTicks = snapshot.range_ticks || state.config.range_ticks || 10;
  const nextBrickLength = snapshot.brick_length || state.config.brick_length || 10000;
  const shouldRefit =
    snapshot.symbol !== state.activeSymbol ||
    snapshot.duration_seconds !== state.activeDurationSeconds ||
    nextBarMode !== state.activeBarMode ||
    nextRangeTicks !== state.activeRangeTicks ||
    nextBrickLength !== state.activeBrickLength;

  state.activeSymbol = snapshot.symbol;
  state.activeProvider = snapshot.provider || state.config.provider;
  state.activeDurationSeconds = snapshot.duration_seconds;
  state.activeBarMode = nextBarMode;
  state.activeRangeTicks = nextRangeTicks;
  state.activeBrickLength = nextBrickLength;
  state.config.symbol = snapshot.symbol;
  state.config.provider = state.activeProvider;
  state.config.duration_seconds = snapshot.duration_seconds;
  state.config.bar_mode = nextBarMode;
  state.config.range_ticks = nextRangeTicks;
  state.config.brick_length = nextBrickLength;
  state.timeLabels = new Map(Object.entries(snapshot.time_labels || {}));
  els.symbolSelect.value = snapshot.symbol;
  els.providerSelect.value = state.activeProvider;
  els.barModeSelect.value = state.activeBarMode;
  els.durationSelect.value = String(snapshot.duration_seconds);
  els.rangeTicksInput.value = String(state.activeRangeTicks);
  els.brickLengthInput.value = String(state.activeBrickLength);
  syncBarModeControls(state.activeBarMode);
  syncMarketHeader(
    snapshot.symbol_label || snapshot.symbol,
    snapshot.duration_seconds,
    state.activeBarMode,
    state.activeRangeTicks
  );
  renderProviderMeta(snapshot);
  els.lastPrice.textContent = snapshot.last_close.toFixed(2);
  els.lastPrice.style.color = snapshot.last_color;
  els.lastUpdate.textContent = snapshot.last_time;

  const sanitizedSnapshot = sanitizePricePaneIndicators(snapshot);
  const trimmedSnapshot = trimSnapshotForDisplay(sanitizedSnapshot);
  const displaySnapshot = trimmedSnapshot;
  state.timeLabels = new Map(Object.entries(displaySnapshot.time_labels || {}));
  const previousCandleCount = (state.seriesDataByKey.get("candles") || []).length;
  const candleSeries = state.seriesByKey.get("candles");
  const volumeSeries = state.seriesByKey.get("volume");
  setSeriesData("candles", candleSeries, displaySnapshot.candles);
  setSeriesData("volume", volumeSeries, displaySnapshot.volume);
  const activeBandPrimaryKeys = new Set();

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
    const activeSeriesKeys = new Set(["candles", "volume"]);
    const bandConfigs = [];
    const paneId = indicatorPaneId(indicator);
    const paneEntry = state.charts.find((item) => item.paneId === paneId);
    if (!paneEntry) {
      return;
    }

    indicator.series.forEach((seriesDefinition) => {
      const key = `indicator:${indicator.id}:${seriesDefinition.id}`;
      activeSeriesKeys.add(key);
      let series = state.seriesByKey.get(key);
      if (!series) {
        series = createSeries(paneEntry.chart, seriesDefinition);
        state.seriesByKey.set(key, series);
        state.seriesChartByKey.set(key, paneEntry.chart);
        if (!state.primarySeriesKeyByPane.has(paneId)) {
          state.primarySeriesKeyByPane.set(paneId, key);
        }
      }
      setSeriesData(key, series, seriesDefinition.data);
      if (typeof series.setMarkers === "function") {
        series.setMarkers(seriesDefinition.options?.markers || []);
      }

      if (seriesDefinition.options?.fillToSeriesId) {
        activeBandPrimaryKeys.add(key);
        bandConfigs.push({
          primaryKey: key,
          secondaryKey: `indicator:${indicator.id}:${seriesDefinition.options.fillToSeriesId}`,
          fillColor: seriesDefinition.options.fillColor || "rgba(255, 152, 0, 0.16)",
        });
      }
    });

    bandConfigs.forEach((config) => {
      syncBandPrimitive(config.primaryKey, config.secondaryKey, config.fillColor);
    });

    [...state.seriesByKey.keys()]
      .filter((key) => key.startsWith(`indicator:${indicator.id}:`) && !activeSeriesKeys.has(key))
      .forEach((key) => removeSeriesByKey(key));
  });

  [...state.bandPrimitiveByKey.keys()]
    .filter((key) => !activeBandPrimaryKeys.has(key))
    .forEach((key) => {
      const series = state.seriesByKey.get(key);
      const primitive = state.bandPrimitiveByKey.get(key);
      if (series?.detachPrimitive && primitive) {
        series.detachPrimitive(primitive);
      }
      state.bandPrimitiveByKey.delete(key);
    });

  if (shouldRefit) {
    state.hasFitted = false;
  }
  if (!state.hasFitted && state.charts.length > 0) {
    if (displaySnapshot.indicators.length > 0) {
      focusComputedBars(state.charts[0].chart, displaySnapshot);
    } else {
      focusRecentBars(state.charts[0].chart, displaySnapshot.candles.length);
    }
    state.hasFitted = true;
  }

  if (state.pendingHistoryRange && state.charts.length > 0) {
    const addedBars = Math.max(displaySnapshot.candles.length - previousCandleCount, 0);
    const priceChart = state.charts[0].chart;
    if (addedBars > 0) {
      priceChart.timeScale().setVisibleLogicalRange({
        from: state.pendingHistoryRange.from + addedBars,
        to: state.pendingHistoryRange.to + addedBars,
      });
    }
    state.pendingHistoryRange = null;
    state.historyExpandInFlight = false;
  }
}

async function refreshSnapshot() {
  const requestId = ++state.snapshotRequestId;
  const params = new URLSearchParams();
  params.set("provider", getRequestedProvider());
  params.set("symbol", getRequestedSymbol());
  params.set("duration_seconds", String(getRequestedDuration()));
  params.set("bar_mode", getRequestedBarMode());
  params.set("range_ticks", String(getRequestedRangeTicks()));
  params.set("brick_length", String(getRequestedBrickLength()));
  params.set("data_length", String(currentRequestedDataLength()));
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
  if (requestId !== state.snapshotRequestId) {
    return;
  }
  applySnapshot(snapshot);
  syncAutoRefresh(snapshot.refresh_ms ?? state.config?.refresh_ms ?? 0);
}

async function refreshConfig(provider) {
  const requestId = ++state.configRequestId;
  const params = new URLSearchParams();
  if (provider) {
    params.set("provider", provider);
  }
  const query = params.toString();
  const nextConfig = await fetchJson(`/api/config${query ? `?${query}` : ""}`);
  if (requestId !== state.configRequestId) {
    return;
  }
  state.config = nextConfig;
  state.activeProvider = nextConfig.provider;
  state.activeSymbol = nextConfig.symbol;
  state.activeBrickLength = nextConfig.brick_length || state.activeBrickLength || 10000;
  state.requestedDataLength = nextConfig.data_length || state.requestedDataLength || 800;
  buildProviderOptions(nextConfig.providers || [], nextConfig.provider);
  buildContractOptions(nextConfig.contracts || [], nextConfig.symbol);
  buildBarModeOptions(nextConfig.bar_modes || [{ id: "time", label: "时间 K 线" }], state.activeBarMode);
  buildDurationOptions(nextConfig.duration_options || [nextConfig.duration_seconds], nextConfig.duration_seconds);
  els.brickLengthInput.value = String(state.activeBrickLength);
  syncMarketHeader(
    nextConfig.symbol_label || nextConfig.symbol,
    nextConfig.duration_seconds,
    state.activeBarMode,
    state.activeRangeTicks
  );
  renderProviderMeta(nextConfig);
  syncAutoRefresh(nextConfig.refresh_ms ?? 0);
}

function syncAutoRefresh(refreshMs) {
  if (state.refreshTimerId) {
    window.clearInterval(state.refreshTimerId);
    state.refreshTimerId = null;
  }
  if (!Number.isFinite(refreshMs) || refreshMs <= 0) {
    return;
  }
  state.refreshTimerId = window.setInterval(async () => {
    try {
      await refreshSnapshot();
    } catch (error) {
      els.error.textContent = error.message;
    }
  }, refreshMs);
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
  state.activeProvider = state.config.provider;
  state.activeSymbol = state.config.symbol;
  state.activeDurationSeconds = state.config.duration_seconds;
  state.activeBarMode = state.config.bar_mode || "time";
  state.activeRangeTicks = state.config.range_ticks || 10;
  state.activeBrickLength = state.config.brick_length || 10000;
  state.requestedDataLength = state.config.data_length || 800;
  state.timeLabels = new Map();
  state.selectedIndicators = [...state.config.default_indicator_ids];
  state.indicatorParams = getDefaultIndicatorParams(state.config.indicators);

  els.provider.textContent = state.config.provider;
  buildProviderOptions(state.config.providers || [state.config.provider], state.config.provider);
  buildContractOptions(state.config.contracts || [], state.config.symbol);
  buildBarModeOptions(state.config.bar_modes || [{ id: "time", label: "时间 K 线" }], state.activeBarMode);
  buildDurationOptions(state.config.duration_options || [state.config.duration_seconds], state.config.duration_seconds);
  els.rangeTicksInput.value = String(state.activeRangeTicks);
  els.brickLengthInput.value = String(state.activeBrickLength);
  syncBarModeControls(state.activeBarMode);
  syncMarketHeader(
    state.config.symbol_label || state.config.symbol,
    state.config.duration_seconds,
    state.activeBarMode,
    state.activeRangeTicks
  );
  renderProviderMeta(state.config);

  els.symbolSelect.addEventListener("change", async () => {
    try {
      state.activeSymbol = getRequestedSymbol();
      state.config.symbol = state.activeSymbol;
      els.error.textContent = "";
      await refreshSnapshot();
    } catch (error) {
      els.error.textContent = error.message;
    }
  });
  els.providerSelect.addEventListener("change", async () => {
    const nextProvider = getRequestedProvider();
    try {
      await refreshConfig(nextProvider);
      await refreshSnapshot();
    } catch (error) {
      els.error.textContent = error.message;
    }
  });
  els.barModeSelect.addEventListener("change", async () => {
    const nextBarMode = getRequestedBarMode();
    const previousBarMode = state.activeBarMode;
    if ((nextBarMode === "renko" || nextBarMode === "range") && previousBarMode !== nextBarMode) {
      const previousDefault = defaultTicksForBarMode(previousBarMode);
      const currentTicks = getRequestedRangeTicks();
      if (currentTicks === previousDefault || !els.rangeTicksInput.value) {
        els.rangeTicksInput.value = String(defaultTicksForBarMode(nextBarMode));
      }
    }
    syncBarModeControls(nextBarMode);
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
  els.rangeTicksInput.addEventListener("change", async () => {
    try {
      await refreshSnapshot();
    } catch (error) {
      els.error.textContent = error.message;
    }
  });
  els.brickLengthInput.addEventListener("change", async () => {
    try {
      await refreshSnapshot();
    } catch (error) {
      els.error.textContent = error.message;
    }
  });

  buildIndicatorSelector(state.config.indicators, state.config.default_indicator_ids);
  rebuildCharts();
  await refreshSnapshot();
}

window.addEventListener("resize", resizeCharts);

boot().catch((error) => {
  els.error.textContent = error.message;
});
