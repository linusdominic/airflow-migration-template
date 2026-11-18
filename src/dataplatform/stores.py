"""Object stores: a folder on disk, or any cloud bucket through fsspec."""

from pathlib import Path


class LocalStore:
    """Files under one folder. Paths are relative to it and cannot climb out of it."""

    def __init__(self, root: str | Path) -> None:
        self.root = Path(root).resolve()

    def _path(self, path: str) -> Path:
        full = (self.root / path).resolve()
        if self.root != full and self.root not in full.parents:
            raise ValueError(f"{path!r} is outside the store")
        return full

    def read_text(self, path: str) -> str:
        return self._path(path).read_text()

    def write_text(self, path: str, text: str) -> None:
        target = self._path(path)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(text)

    def exists(self, path: str) -> bool:
        return self._path(path).is_file()

    def list(self, prefix: str = "") -> list[str]:
        if not self.root.exists():
            return []
        names = (str(p.relative_to(self.root)) for p in self.root.rglob("*") if p.is_file())
        return sorted(name for name in names if name.startswith(prefix))

    def delete(self, path: str) -> None:
        self._path(path).unlink(missing_ok=True)


class FsspecStore:
    """Any store fsspec can open: ``s3://``, ``gs://``, ``abfs://``, ``memory://`` and more.

    The cloud back ends each need their own package (s3fs, gcsfs, adlfs). Only ``memory://``
    is exercised by the tests. The cloud ones were not run.
    """

    def __init__(self, url: str) -> None:
        try:
            import fsspec
        except ImportError:
            raise ImportError("cloud object stores need fsspec: install dataplatform[cloud]") from None
        self._fs, root = fsspec.core.url_to_fs(url)
        self._root = root.rstrip("/")

    def _path(self, path: str) -> str:
        return f"{self._root}/{path.lstrip('/')}"

    def read_text(self, path: str) -> str:
        with self._fs.open(self._path(path), "r") as handle:
            return handle.read()

    def write_text(self, path: str, text: str) -> None:
        full = self._path(path)
        self._fs.makedirs(full.rsplit("/", 1)[0], exist_ok=True)
        with self._fs.open(full, "w") as handle:
            handle.write(text)

    def exists(self, path: str) -> bool:
        return self._fs.isfile(self._path(path))

    def list(self, prefix: str = "") -> list[str]:
        if not self._fs.exists(self._root):
            return []
        start = len(self._root) + 1
        names = (name[start:] for name in self._fs.find(self._root))
        return sorted(name for name in names if name.startswith(prefix))

    def delete(self, path: str) -> None:
        if self.exists(path):
            self._fs.rm(self._path(path))
