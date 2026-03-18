from __future__ import annotations

import argparse
import threading
import webbrowser
from pathlib import Path

from werkzeug.serving import make_server

from tq_app.service import MarketDataService
from tq_app.web import create_app

DEFAULT_PROVIDER = "tq"
DEFAULT_SYMBOL = "DCE.v2609"
DEFAULT_DURATION_SECONDS = 300
DEFAULT_DATA_LENGTH = 300
DEFAULT_REFRESH_MS = 800
DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 8050


class ServerThread(threading.Thread):
    def __init__(self, app, host: str, port: int) -> None:
        super().__init__(daemon=True)
        self.server = make_server(host, port, app)
        self.context = app.app_context()
        self.context.push()

    def run(self) -> None:
        self.server.serve_forever()

    def shutdown(self) -> None:
        self.server.shutdown()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="天勤量化浏览器图表工作台")
    parser.add_argument("--provider", default=DEFAULT_PROVIDER, help="数据源名称，当前支持 tq")
    parser.add_argument("--symbol", default=DEFAULT_SYMBOL, help="合约代码，例如 DCE.v2609")
    parser.add_argument("--duration", type=int, default=DEFAULT_DURATION_SECONDS, help="K 线周期，单位秒")
    parser.add_argument("--length", type=int, default=DEFAULT_DATA_LENGTH, help="拉取 K 线数量")
    parser.add_argument("--refresh-ms", type=int, default=DEFAULT_REFRESH_MS, help="刷新间隔，单位毫秒")
    parser.add_argument("--host", default=DEFAULT_HOST, help="监听地址")
    parser.add_argument("--port", type=int, default=DEFAULT_PORT, help="监听端口")
    parser.add_argument("--open-browser", action="store_true", help="启动后自动打开浏览器")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    project_root = Path(__file__).resolve().parent
    service = MarketDataService(
        provider=args.provider,
        symbol=args.symbol,
        duration_seconds=args.duration,
        data_length=args.length,
        refresh_ms=args.refresh_ms,
        project_root=project_root,
    )
    service.start()

    app = create_app(service, project_root)
    server = ServerThread(app, args.host, args.port)
    url = f"http://{args.host}:{args.port}"

    print(f"数据源: {args.provider}")
    print(f"图表地址: {url}")
    server.start()

    if args.open_browser:
        threading.Timer(1.0, lambda: webbrowser.open(url)).start()

    try:
        server.join()
    except KeyboardInterrupt:
        pass
    finally:
        server.shutdown()
        service.stop()


if __name__ == "__main__":
    main()
