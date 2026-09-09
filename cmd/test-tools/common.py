# -*- coding: utf-8 -*-
"""
ChomoSyncer Test & Validation Toolkit - Common Utilities
"""
import os
import sys
import time
import json
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple
import yaml

try:
    import redis
except ImportError:
    redis = None

try:
    import requests
except ImportError:
    requests = None


class Colors:
    GREEN = "\033[92m"
    RED = "\033[91m"
    YELLOW = "\033[93m"
    BLUE = "\033[94m"
    CYAN = "\033[96m"
    BOLD = "\033[1m"
    DIM = "\033[2m"
    RESET = "\033[0m"

    @classmethod
    def disable(cls):
        cls.GREEN = ""
        cls.RED = ""
        cls.YELLOW = ""
        cls.BLUE = ""
        cls.CYAN = ""
        cls.BOLD = ""
        cls.DIM = ""
        cls.RESET = ""


# Enable colorama on Windows if available
if os.name == "nt":
    try:
        import colorama
        colorama.init()
    except Exception:
        pass


def load_config(config_path: Optional[str] = None) -> Dict[str, Any]:
    """
    Searches and loads config.yaml or config.example.yaml.
    """
    candidates = []
    if config_path:
        candidates.append(config_path)

    # Search current, parent, workspace root directories
    search_dirs = [
        os.getcwd(),
        os.path.dirname(os.path.abspath(__file__)),
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
    ]

    for d in search_dirs:
        candidates.append(os.path.join(d, "config.yaml"))
        candidates.append(os.path.join(d, "config.example.yaml"))

    for c in candidates:
        if os.path.isfile(c):
            try:
                with open(c, "r", encoding="utf-8") as f:
                    data = yaml.safe_load(f)
                    if isinstance(data, dict):
                        return data
            except Exception as e:
                logging.warning(f"Failed to load config {c}: {e}")

    return {}


def parse_interval_to_ms(interval: str) -> int:
    """
    Converts Binance kline interval string ('1m', '1h', etc.) to milliseconds.
    """
    if not interval or len(interval) < 2:
        raise ValueError(f"Invalid interval: {interval}")
    unit = interval[-1]
    n = int(interval[:-1])
    if unit == "s":
        return n * 1000
    elif unit == "m":
        return n * 60 * 1000
    elif unit == "h":
        return n * 3600 * 1000
    elif unit == "d":
        return n * 86400 * 1000
    elif unit == "w":
        return n * 7 * 86400 * 1000
    elif unit == "M":
        return n * 30 * 86400 * 1000
    else:
        raise ValueError(f"Unknown interval unit in: {interval}")


def format_ms_to_utc(ms: int) -> str:
    """
    Formats millisecond epoch timestamp as UTC string: YYYY-MM-DD HH:MM:SS.
    """
    try:
        dt = datetime.fromtimestamp(ms / 1000.0, timezone.utc)
        return dt.strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return str(ms)


class ClickHouseClient:
    """
    Dual-engine ClickHouse client supporting both clickhouse-connect and direct HTTP API.
    """
    def __init__(self, host: str = "127.0.0.1", port: int = 8123, database: str = "market",
                 username: str = "default", password: str = "", timeout: int = 30):
        self.host = host
        self.port = port
        self.database = database
        self.username = username
        self.password = password
        self.timeout = timeout
        self._ch_client = None

        try:
            import clickhouse_connect
            self._ch_client = clickhouse_connect.get_client(
                host=self.host,
                port=self.port,
                database=self.database,
                username=self.username,
                password=self.password,
                connect_timeout=self.timeout,
                send_receive_timeout=self.timeout
            )
        except Exception:
            self._ch_client = None

    def query(self, sql: str) -> List[Dict[str, Any]]:
        """
        Executes a SQL query and returns rows as a list of dicts.
        """
        if self._ch_client is not None:
            try:
                res = self._ch_client.query(sql)
                cols = res.column_names
                return [dict(zip(cols, row)) for row in res.result_rows]
            except Exception:
                pass

        if requests is None:
            raise RuntimeError("Neither clickhouse-connect nor requests is available to query ClickHouse.")

        url = f"http://{self.host}:{self.port}/"
        clean_sql = sql.strip().rstrip(";")
        params = {
            "database": self.database,
            "query": clean_sql + " FORMAT JSON",
        }
        auth = (self.username, self.password) if self.password else None
        headers = {"Accept": "application/json"}

        resp = requests.post(url, params=params, auth=auth, headers=headers, timeout=self.timeout)
        if resp.status_code != 200:
            raise RuntimeError(f"ClickHouse HTTP query error {resp.status_code}: {resp.text}")

        data = resp.json()
        return data.get("data", [])

    def test_connection(self) -> Tuple[bool, str]:
        """
        Tests connection to ClickHouse.
        """
        try:
            res = self.query("SELECT 1 AS ok")
            if res and res[0].get("ok") == 1:
                return True, "Connected successfully"
            return False, "Unexpected response from ClickHouse"
        except Exception as e:
            return False, str(e)


def get_clickhouse_client(config: Dict[str, Any], args: Optional[Any] = None) -> ClickHouseClient:
    """
    Builds ClickHouseClient from config and CLI args.
    """
    ch_cfg = config.get("clickhouse", {})
    addrs = ch_cfg.get("addrs", ["localhost:9000"])
    addr = addrs[0] if addrs else "localhost:9000"

    host = "127.0.0.1"
    port = 8123
    if ":" in addr:
        parts = addr.split(":")
        host = parts[0]
        p = int(parts[1])
        # Native port 9000 -> HTTP port 8123 default
        port = 8123 if p == 9000 else p

    db = ch_cfg.get("database", "market")
    user = ch_cfg.get("username", "default")
    pwd = ch_cfg.get("password", "")

    if args:
        if getattr(args, "ch_host", None):
            host = args.ch_host
        if getattr(args, "ch_port", None):
            port = args.ch_port
        if getattr(args, "ch_db", None):
            db = args.ch_db
        if getattr(args, "ch_user", None):
            user = args.ch_user
        if getattr(args, "ch_password", None):
            pwd = args.ch_password

    return ClickHouseClient(host=host, port=port, database=db, username=user, password=pwd)


def get_redis_client(config: Dict[str, Any], args: Optional[Any] = None, for_live: bool = False):
    """
    Builds redis.Redis client from config and CLI args.
    """
    if redis is None:
        raise RuntimeError("redis-py is not installed. Run 'pip install redis'.")

    r_cfg = config.get("redis", {})
    addr = r_cfg.get("addr", "localhost:6379")
    db = r_cfg.get("db", 0)
    password = r_cfg.get("password", "")

    if for_live:
        live_cfg = r_cfg.get("live", {})
        if live_cfg.get("addr"):
            addr = live_cfg.get("addr")
        if "db" in live_cfg:
            db = live_cfg.get("db", db)
        if live_cfg.get("password"):
            password = live_cfg.get("password")

    host = "127.0.0.1"
    port = 6379
    if ":" in addr:
        parts = addr.split(":")
        host = parts[0]
        port = int(parts[1])

    if args:
        if getattr(args, "redis_host", None):
            host = args.redis_host
        if getattr(args, "redis_port", None):
            port = args.redis_port
        if getattr(args, "redis_db", None) is not None:
            db = args.redis_db
        if getattr(args, "redis_password", None):
            password = args.redis_password

    return redis.Redis(
        host=host,
        port=port,
        db=db,
        password=password if password else None,
        decode_responses=True,
        socket_timeout=5.0
    )


def fetch_active_symbols(config: Dict[str, Any], ch_client: Optional[ClickHouseClient] = None,
                         limit: Optional[int] = None) -> List[str]:
    """
    Fetches the list of active trading symbols:
    1. From Binance REST API /fapi/v1/exchangeInfo
    2. Fallback to ClickHouse distinct symbols
    3. Fallback to core top coins
    """
    u_cfg = config.get("universe", {})
    rest_url = u_cfg.get("rest_url", "https://fapi.binance.com")
    quote_assets = set(u_cfg.get("quote_assets", ["USDT"]))
    contract_type = u_cfg.get("contract_type", "PERPETUAL")
    status = u_cfg.get("status", "TRADING")

    symbols = []
    if requests is not None:
        try:
            resp = requests.get(f"{rest_url}/fapi/v1/exchangeInfo", timeout=10)
            if resp.status_code == 200:
                data = resp.json()
                for s in data.get("symbols", []):
                    if (s.get("status") == status and
                        s.get("contractType") == contract_type and
                        s.get("quoteAsset") in quote_assets):
                        symbols.append(s.get("symbol"))
        except Exception as e:
            logging.debug(f"Binance exchangeInfo fetch failed: {e}")

    if not symbols and ch_client is not None:
        try:
            rows = ch_client.query("SELECT DISTINCT symbol FROM market.fapi_kline_1m FINAL ORDER BY symbol")
            symbols = [r["symbol"] for r in rows if "symbol" in r]
        except Exception as e:
            logging.debug(f"ClickHouse symbol fetch failed: {e}")

    if not symbols:
        symbols = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT", "DOGEUSDT", "ADAUSDT"]

    symbols.sort()
    if limit and limit > 0:
        symbols = symbols[:limit]
    return symbols


def print_table(headers: List[str], rows: List[List[Any]], alignments: Optional[List[str]] = None):
    """
    Prints a formatted ASCII/Unicode table using tabulate or fallback.
    """
    try:
        from tabulate import tabulate
        print(tabulate(rows, headers=headers, tablefmt="rounded_outline"))
    except ImportError:
        import re
        col_widths = [len(h) for h in headers]
        for row in rows:
            for i, val in enumerate(row):
                s = str(val)
                clean = re.sub(r'\033\[[0-9;]*m', '', s)
                col_widths[i] = max(col_widths[i], len(clean))

        sep = "+" + "+".join(["-" * (w + 2) for w in col_widths]) + "+"
        print(sep)
        header_line = "|" + "|".join([f" {h:<{col_widths[i]}} " for i, h in enumerate(headers)]) + "|"
        print(header_line)
        print(sep)
        for row in rows:
            row_line = "|" + "|".join([f" {str(val):<{col_widths[i]}} " for i, val in enumerate(row)]) + "|"
            print(row_line)
        print(sep)
