"""Configuration loader for the project.

This module provides a tiny, dependency-free dotenv loader and a
`Config` dataclass that reads commonly used environment variables.

Usage:
    from config import load_dotenv, load_config
    load_dotenv()            # populates os.environ from .env (if present)
    cfg = load_config()      # returns a Config object with typed fields

The loader is intentionally simple: it supports lines like `KEY=VALUE`,
ignores comments and blank lines, and handles single/double-quoted values.
"""
from dataclasses import dataclass
import os
from typing import Optional


def load_dotenv(path: str = ".env", override: bool = False) -> None:
    """Load environment variables from a .env file into os.environ.

    - path: path to the .env file
    - override: if True, overwrite existing environment variables
    """
    try:
        with open(path, "r", encoding="utf-8") as fh:
            for raw in fh:
                line = raw.strip()
                if not line or line.startswith("#"):
                    continue
                # support: export KEY=VALUE or KEY=VALUE
                if line.startswith("export "):
                    line = line[len("export "):]
                if "=" not in line:
                    continue
                key, val = line.split("=", 1)
                key = key.strip()
                val = val.strip()
                # strip single/double quotes
                if (val.startswith('"') and val.endswith('"')) or (
                    val.startswith("'") and val.endswith("'")
                ):
                    val = val[1:-1]
                # don't overwrite existing env vars unless requested
                if override or key not in os.environ:
                    os.environ[key] = val
    except FileNotFoundError:
        # no .env found — that's fine
        return


@dataclass
class Config:
    # Application
    APP_ENV: str = os.getenv("APP_ENV", "development")
    APP_HOST: str = os.getenv("APP_HOST", "0.0.0.0")
    APP_PORT: int = int(os.getenv("APP_PORT", "8000"))
    SECRET_KEY: Optional[str] = os.getenv("SECRET_KEY")
    JWT_SECRET: Optional[str] = os.getenv("JWT_SECRET")

    # Database / cache / runtime
    DATABASE_URL: Optional[str] = os.getenv("DATABASE_URL")
    REDIS_URL: Optional[str] = os.getenv("REDIS_URL")
    MAX_WORKERS: int = int(os.getenv("MAX_WORKERS", "4"))
    REQUEST_TIMEOUT: int = int(os.getenv("REQUEST_TIMEOUT", "10"))

    # External APIs
    COINMARKETCAP_API_KEY: Optional[str] = os.getenv("COINMARKETCAP_API_KEY")
    BINANCE_API_KEY: Optional[str] = os.getenv("BINANCE_API_KEY")
    BINANCE_API_SECRET: Optional[str] = os.getenv("BINANCE_API_SECRET")
    # Add other exchange API keys below as needed

    # Observability / email
    SENTRY_DSN: Optional[str] = os.getenv("SENTRY_DSN")
    LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO")
    SMTP_HOST: Optional[str] = os.getenv("SMTP_HOST")
    SMTP_PORT: Optional[str] = os.getenv("SMTP_PORT")


def load_config(env_path: str = ".env", override_env: bool = False) -> Config:
    """Load .env into os.environ and return a Config instance.

    - env_path: path to .env file
    - override_env: if True, override current environment variables from file
    """
    load_dotenv(env_path, override=override_env)
    return Config()


__all__ = ["load_dotenv", "Config", "load_config"]
