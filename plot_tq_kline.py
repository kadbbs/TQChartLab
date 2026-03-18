import argparse
import os
import time

import matplotlib.pyplot as plt
import mplfinance as mpf
import pandas as pd
from dotenv import load_dotenv
from tqsdk import TqApi, TqAuth


DEFAULT_SYMBOL = "SHFE.rb2505"
DEFAULT_DURATION_SECONDS = 60 * 60
DEFAULT_DATA_LENGTH = 300
DEFAULT_REFRESH_MS = 800
TV_BACKGROUND = "#131722"
TV_PANEL = "#181c27"
TV_GRID = "#2a2e39"
TV_TEXT = "#b2b5be"
TV_UP = "#089981"
TV_DOWN = "#f23645"
TV_ACCENT = "#2962ff"
TV_SIGNAL = "#ff9800"
TV_UPPER = "#ff6d6d"
TV_LOWER = "#00c076"
TV_PRICE_LINE = "#787b86"


TRADINGVIEW_STYLE = mpf.make_mpf_style(
    base_mpf_style="nightclouds",
    marketcolors=mpf.make_marketcolors(
        up=TV_UP,
        down=TV_DOWN,
        edge={"up": TV_UP, "down": TV_DOWN},
        wick={"up": TV_UP, "down": TV_DOWN},
        volume={"up": TV_UP, "down": TV_DOWN},
        ohlc={"up": TV_UP, "down": TV_DOWN},
    ),
    figcolor=TV_BACKGROUND,
    facecolor=TV_PANEL,
    edgecolor=TV_GRID,
    gridcolor=TV_GRID,
    gridstyle="-",
    rc={
        "figure.facecolor": TV_BACKGROUND,
        "axes.facecolor": TV_PANEL,
        "axes.edgecolor": TV_GRID,
        "axes.labelcolor": TV_TEXT,
        "axes.titlecolor": TV_TEXT,
        "xtick.color": TV_TEXT,
        "ytick.color": TV_TEXT,
        "grid.color": TV_GRID,
        "grid.alpha": 0.35,
        "savefig.facecolor": TV_BACKGROUND,
        "savefig.edgecolor": TV_BACKGROUND,
    },
)


def build_api() -> TqApi:
    load_dotenv()
    user = os.getenv("TQ_USER")
    password = os.getenv("TQ_PASSWORD")
    if not user or not password:
        raise RuntimeError(
            "请先在 .env 文件中设置 TQ_USER 和 TQ_PASSWORD，再运行脚本。"
        )
    return TqApi(auth=TqAuth(user, password))


def wait_for_initial_data(klines: pd.DataFrame, api: TqApi, data_length: int) -> pd.DataFrame:
    while True:
        api.wait_update()
        if len(klines) >= data_length and not pd.isna(klines.iloc[-1]["close"]):
            break

    df = klines.copy()
    df["datetime"] = pd.to_datetime(df["datetime"], unit="ns")
    df = df.set_index("datetime")
    return df


def add_indicators(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    n = 14
    m = 1.5

    prev_close = df["close"].shift(1)
    tr1 = df["high"] - df["low"]
    tr2 = (df["high"] - prev_close).abs()
    tr3 = (df["low"] - prev_close).abs()
    df["tr"] = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    df["atr"] = df["tr"].rolling(n).mean()

    df["upper"] = df["high"] + df["atr"] * m
    df["lower"] = df["low"] - df["atr"] * m

    last_upper = df["upper"].iloc[-1]
    last_lower = df["lower"].iloc[-1]
    df["upper_last"] = last_upper
    df["lower_last"] = last_lower

    ema12 = df["close"].ewm(span=12, adjust=False).mean()
    ema26 = df["close"].ewm(span=26, adjust=False).mean()
    df["macd_diff"] = ema12 - ema26
    df["macd_dea"] = df["macd_diff"].ewm(span=9, adjust=False).mean()
    df["macd_hist"] = (df["macd_diff"] - df["macd_dea"]) * 2
    return df


def render_chart(
    price_ax: plt.Axes,
    volume_ax: plt.Axes,
    macd_ax: plt.Axes,
    df: pd.DataFrame,
    symbol: str,
) -> None:
    price_ax.clear()
    volume_ax.clear()
    macd_ax.clear()

    upper = mpf.make_addplot(df["upper"], ax=price_ax, color=TV_UPPER, width=1.0)
    lower = mpf.make_addplot(df["lower"], ax=price_ax, color=TV_LOWER, width=1.0)
    upper_last = mpf.make_addplot(df["upper_last"], ax=price_ax, color=TV_UPPER, width=1.0, linestyle="dashdot")
    lower_last = mpf.make_addplot(df["lower_last"], ax=price_ax, color=TV_LOWER, width=1.0, linestyle="dashdot")
    macd_diff = mpf.make_addplot(df["macd_diff"], ax=macd_ax, color=TV_ACCENT, ylabel="MACD")
    macd_dea = mpf.make_addplot(df["macd_dea"], ax=macd_ax, color=TV_SIGNAL)
    macd_hist = mpf.make_addplot(
        df["macd_hist"],
        ax=macd_ax,
        type="bar",
        color=[TV_DOWN if value >= 0 else TV_UP for value in df["macd_hist"]],
        alpha=0.8,
    )

    mpf.plot(
        df[["open", "high", "low", "close", "volume"]],
        type="candle",
        style=TRADINGVIEW_STYLE,
        ax=price_ax,
        volume=volume_ax,
        addplot=[upper, lower, upper_last, lower_last, macd_diff, macd_dea, macd_hist],
        datetime_format="%m-%d %H:%M",
        xrotation=10,
    )

    last_close = df["close"].iloc[-1]
    prev_close = df["close"].iloc[-2] if len(df) > 1 else last_close
    price_color = TV_UP if last_close >= prev_close else TV_DOWN

    price_ax.set_title(
        f"{symbol}  实时K线",
        loc="left",
        fontsize=14,
        fontweight="bold",
        pad=12,
    )
    price_ax.text(
        0.995,
        1.015,
        f"CLOSE {last_close:.2f}",
        transform=price_ax.transAxes,
        ha="right",
        va="bottom",
        color=price_color,
        fontsize=11,
    )
    price_ax.axhline(
        y=last_close,
        color=TV_PRICE_LINE,
        linestyle=(0, (4, 4)),
        linewidth=0.9,
        alpha=0.9,
        zorder=1,
    )
    price_ax.annotate(
        f"{last_close:.2f}",
        xy=(1, last_close),
        xycoords=("axes fraction", "data"),
        xytext=(8, 0),
        textcoords="offset points",
        ha="left",
        va="center",
        color="white",
        fontsize=9,
        bbox={
            "boxstyle": "round,pad=0.25,rounding_size=0.15",
            "fc": price_color,
            "ec": price_color,
            "lw": 0.0,
        },
        clip_on=False,
        zorder=5,
    )

    for ax in (price_ax, volume_ax, macd_ax):
        ax.set_facecolor(TV_PANEL)
        ax.grid(True, linestyle="-", linewidth=0.6, alpha=0.35)
        ax.tick_params(colors=TV_TEXT, labelsize=9)
        for spine in ax.spines.values():
            spine.set_color(TV_GRID)

    price_ax.margins(x=0.03)
    volume_ax.set_ylabel("VOL", color=TV_TEXT)
    macd_ax.set_ylabel("MACD", color=TV_TEXT)


def plot_chart_realtime(api: TqApi, symbol: str, duration_seconds: int, data_length: int, refresh_ms: int) -> None:
    klines = api.get_kline_serial(symbol, duration_seconds=duration_seconds, data_length=data_length)
    df = wait_for_initial_data(klines, api, data_length)
    df = add_indicators(df)

    plt.ion()
    fig, axes = plt.subplots(
        3,
        1,
        figsize=(14, 8),
        sharex=True,
        gridspec_kw={"height_ratios": [3, 1, 1]},
    )
    price_ax, volume_ax, macd_ax = axes
    fig.patch.set_facecolor(TV_BACKGROUND)
    render_chart(price_ax, volume_ax, macd_ax, df, symbol)
    plt.tight_layout()
    plt.show(block=False)

    try:
        while plt.fignum_exists(fig.number):
            changed = api.wait_update(deadline=time.time() + refresh_ms / 1000)
            if not changed:
                plt.pause(refresh_ms / 1000)
                continue

            if api.is_changing(klines):
                df = klines.copy()
                df["datetime"] = pd.to_datetime(df["datetime"], unit="ns")
                df = df.set_index("datetime")
                df = add_indicators(df)
                render_chart(price_ax, volume_ax, macd_ax, df, symbol)
                fig.canvas.draw_idle()

            plt.pause(0.001)
    except KeyboardInterrupt:
        pass


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="读取天勤 K 线并绘制自定义指标")
    parser.add_argument("--symbol", default=DEFAULT_SYMBOL, help="合约代码，例如 SHFE.rb2505")
    parser.add_argument(
        "--duration",
        type=int,
        default=DEFAULT_DURATION_SECONDS,
        help="K 线周期，单位秒，例如 60 表示 1 分钟，3600 表示 1 小时",
    )
    parser.add_argument("--length", type=int, default=DEFAULT_DATA_LENGTH, help="拉取 K 线数量")
    parser.add_argument(
        "--refresh-ms",
        type=int,
        default=DEFAULT_REFRESH_MS,
        help="图表刷新间隔，单位毫秒",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    api = build_api()
    try:
        plot_chart_realtime(api, args.symbol, args.duration, args.length, args.refresh_ms)
    finally:
        api.close()


if __name__ == "__main__":
    main()
