"""Settings for one environment, read from ``config/<env>.yml``.

The environment is chosen by ``PLATFORM_ENV`` (default ``local``). A value in the file may
refer to an environment variable as ``${NAME}`` or ``${NAME:-default}``, so the same file
works on a laptop and on a server, and no secret is ever written in it.
"""

import os
import re
import sys
from dataclasses import dataclass, field
from pathlib import Path

import yaml

# The project folder: where config/, pipelines/ and dags/ are. A checkout finds itself; an
# installed copy (a container image, say) is told with PLATFORM_ROOT.
ROOT = Path(os.environ.get("PLATFORM_ROOT") or Path(__file__).resolve().parents[2])
_VARIABLE = re.compile(r"\$\{(\w+)(?::-([^}]*))?\}")


def project_on_path() -> None:
    """Make the ``pipelines`` package importable from any working directory."""
    if str(ROOT) not in sys.path:
        sys.path.insert(0, str(ROOT))


class ConfigError(Exception):
    """The configuration is missing something or refers to a variable that is not set."""


@dataclass(frozen=True)
class Settings:
    env: str
    connections: dict[str, str]  # name -> URI
    schemas: dict[str, str] = field(default_factory=dict)  # role -> schema name

    def schema(self, role: str) -> str:
        """The schema that plays ``role`` (staging, core, marts, platform) here."""
        return self.schemas.get(role, role)


def expand(value: str, environ: dict[str, str]) -> str:
    """Replace ``${NAME}`` and ``${NAME:-default}`` in ``value``."""

    def replace(match: re.Match[str]) -> str:
        name, default = match.group(1), match.group(2)
        if name in environ:
            return environ[name]
        if default is not None:
            return default
        raise ConfigError(f"environment variable {name} is not set")

    return _VARIABLE.sub(replace, value)


def load_settings(env: str | None = None, root: Path = ROOT, environ: dict[str, str] | None = None) -> Settings:
    environ = dict(os.environ if environ is None else environ)
    env = env or environ.get("PLATFORM_ENV", "local")
    path = root / "config" / f"{env}.yml"
    if not path.exists():
        known = sorted(p.stem for p in (root / "config").glob("*.yml"))
        raise ConfigError(f"no config/{env}.yml. Environments here: {', '.join(known)}")
    raw = yaml.safe_load(path.read_text()) or {}
    connections = {}
    for name, value in (raw.get("connections") or {}).items():
        # PLATFORM_CONN_<NAME> overrides the file, which is how a deployment injects secrets.
        override = environ.get(f"PLATFORM_CONN_{name.upper()}")
        try:
            connections[name] = override or expand(str(value), environ)
        except ConfigError as error:
            raise ConfigError(f"connection {name!r} in config/{env}.yml: {error}") from None
    return Settings(env=env, connections=connections, schemas=dict(raw.get("schemas") or {}))
