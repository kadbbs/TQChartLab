const state = {
  config: null,
  selectedIndicators: [],
  indicatorParams: {},
  charts: [],
  seriesByKey: new Map(),
  currentPriceLine: null,
  hasFitted: false,
};

const els = {
  title: document.getElementById("page-title"),
  provider: document.getElementById("provider-name"),
  symbol: document.getElementById("symbol-name"),
  duration: document.getElementById("duration-name"),
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
      top: 0.08,
      bottom: 0.12,
    },
  },
  timeScale: {
    borderColor: "rgba(92, 70, 47, 0.18)",
    timeVisible: true,
    secondsVisible: false,
    rightOffset: 6,
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
    if (range) {
      targetChart.timeScale().setVisibleLogicalRange(range);
    }
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
  priceChart.subscribeCrosshairMove((param) => {
    if (!param || param.time === undefined) {
      els.cursorTime.textContent = "--";
      return;
    }
    els.cursorTime.textContent = formatCrosshairTime(param.time);
  });

  state.charts.slice(1).forEach((entry, index) => {
    entry.chart.timeScale().applyOptions({
      visible: index === state.charts.length - 2,
    });
    entry.chart.priceScale("right").applyOptions({
      autoScale: true,
      scaleMargins: { top: 0.12, bottom: 0.12 },
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
  els.lastPrice.textContent = snapshot.last_close.toFixed(2);
  els.lastPrice.style.color = snapshot.last_color;
  els.lastUpdate.textContent = snapshot.last_time;

  const candleSeries = state.seriesByKey.get("candles");
  const volumeSeries = state.seriesByKey.get("volume");
  candleSeries.setData(snapshot.candles);
  volumeSeries.setData(snapshot.volume);

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

  snapshot.indicators.forEach((indicator) => {
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
      }
      series.setData(seriesDefinition.data);
    });
  });

  if (!state.hasFitted && state.charts.length > 0) {
    state.charts[0].chart.timeScale().fitContent();
    state.hasFitted = true;
  }
}

async function refreshSnapshot() {
  const params = new URLSearchParams();
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
  state.selectedIndicators = [...state.config.default_indicator_ids];
  state.indicatorParams = getDefaultIndicatorParams(state.config.indicators);

  els.title.textContent = `${state.config.symbol} 图表工作台`;
  els.provider.textContent = state.config.provider;
  els.symbol.textContent = state.config.symbol;
  els.duration.textContent = `${Math.round(state.config.duration_seconds / 60)} 分钟`;

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
