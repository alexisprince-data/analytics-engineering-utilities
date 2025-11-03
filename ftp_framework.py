"""
ftp_framework.py
Production-style, class-based template for FTP/SFTP ingestion.
- Uses paramiko for SFTP if available (falls back to ftplib for FTP)
- Pure standard-library logging
- Hash validation helpers
"""
from __future__ import annotations
import dataclasses, fnmatch, hashlib, logging
from pathlib import Path
from typing import List, Optional, Tuple

# Optional dependency for SFTP
try:
    import paramiko  # type: ignore
    _HAS_PARAMIKO = True
except Exception:
    _HAS_PARAMIKO = False

import ftplib  # built-in FTP

LOG = logging.getLogger("ftp_framework")
if not LOG.handlers:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")

@dataclasses.dataclass(frozen=True)
class FileSpec:
    path: str
    size: Optional[int] = None
    md5: Optional[str] = None

@dataclasses.dataclass
class IngestResult:
    downloaded: List[Path]
    skipped: List[str]
    errors: List[str]

class Hashing:
    @staticmethod
    def md5_of_file(path: Path, chunk_size: int = 1 << 20) -> str:
        m = hashlib.md5()
        with path.open("rb") as f:
            for chunk in iter(lambda: f.read(chunk_size), b""):
                m.update(chunk)
        return m.hexdigest()

class _SFTPClientWrapper:
    def __init__(self, host: str, username: str, password: str, port: int = 22):
        if not _HAS_PARAMIKO:
            raise RuntimeError("paramiko is not installed. `pip install paramiko` to enable SFTP.")
        import paramiko  # local import to satisfy type checkers
        self._transport = paramiko.Transport((host, port))
        self._transport.connect(username=username, password=password)
        self._sftp = paramiko.SFTPClient.from_transport(self._transport)

    def listdir(self, path: str) -> List[str]:
        return [f.filename for f in self._sftp.listdir_attr(path)]

    def download(self, remote_path: str, local_path: Path) -> None:
        local_path.parent.mkdir(parents=True, exist_ok=True)
        self._sftp.get(remote_path, str(local_path))

    def stat(self, remote_path: str):
        return self._sftp.stat(remote_path)

    def close(self) -> None:
        try:
            self._sftp.close()
        finally:
            self._transport.close()

class _FTPClientWrapper:
    def __init__(self, host: str, username: str, password: str, port: int = 21, timeout: int = 60):
        import ftplib as _ftplib
        self._ftp = _ftplib.FTP()
        self._ftp.connect(host, port, timeout=timeout)
        self._ftp.login(user=username, passwd=password)

    def listdir(self, path: str) -> List[str]:
        orig = self._ftp.pwd()
        try:
            self._ftp.cwd(path)
            files = self._ftp.nlst()
        finally:
            self._ftp.cwd(orig)
        return files

    def download(self, remote_path: str, local_path: Path) -> None:
        local_path.parent.mkdir(parents=True, exist_ok=True)
        with local_path.open("wb") as f:
            self._ftp.retrbinary(f"RETR {remote_path}", f.write)

    def stat(self, remote_path: str):
        # Not all FTP servers support SIZE; handle gracefully
        size = None
        try:
            size = self._ftp.size(remote_path)
        except Exception:
            pass
        FTPStat = dataclasses.make_dataclass("FTPStat", [("st_size", Optional[int])])
        return FTPStat(size)

    def close(self) -> None:
        try:
            self._ftp.quit()
        except Exception:
            self._ftp.close()

@dataclasses.dataclass
class FTPIngestor:
    host: str
    username: str
    password: str
    remote_dir: str
    local_dir: Path
    use_sftp: bool = True
    filename_glob: str = "*"
    enforce_size_match: bool = False
    enforce_md5_match: bool = False

    def _client(self):
        if self.use_sftp:
            LOG.info("Connecting via SFTP to %s", self.host)
            return _SFTPClientWrapper(self.host, self.username, self.password)
        LOG.info("Connecting via FTP to %s", self.host)
        return _FTPClientWrapper(self.host, self.username, self.password)

    def list_remote(self) -> List[FileSpec]:
        client = self._client()
        try:
            entries = client.listdir(self.remote_dir)
            matched = [e for e in entries if fnmatch.fnmatch(e, self.filename_glob)]
            result: List[FileSpec] = []
            for name in matched:
                rp = f"{self.remote_dir.rstrip('/')}/{name}"
                size = None
                try:
                    st = client.stat(rp)
                    size = getattr(st, "st_size", None)
                except Exception:
                    pass
                result.append(FileSpec(path=rp, size=size))
            return result
        finally:
            client.close()

    def _validate_download(self, remote: FileSpec, local: Path) -> Tuple[bool, List[str]]:
        reasons: List[str] = []
        ok = True
        if self.enforce_size_match and remote.size is not None:
            actual = local.stat().st_size
            if int(actual) != int(remote.size):
                ok = False
                reasons.append(f"size mismatch local={actual} remote={remote.size}")
        if self.enforce_md5_match and remote.md5:
            digest = Hashing.md5_of_file(local)
            if digest.lower() != remote.md5.lower():
                ok = False
                reasons.append(f"md5 mismatch local={digest} remote={remote.md5}")
        return ok, reasons

    def download_all(self) -> IngestResult:
        client = self._client()
        downloaded: List[Path] = []
        skipped: List[str] = []
        errors: List[str] = []
        try:
            for spec in self.list_remote():
                local_path = self.local_dir / Path(spec.path).name
                try:
                    LOG.info("Downloading %s -> %s", spec.path, local_path)
                    client.download(spec.path, local_path)
                    ok, reasons = self._validate_download(spec, local_path)
                    if not ok:
                        errors.append(f"validation failed for {spec.path}: {', '.join(reasons)}")
                        continue
                    downloaded.append(local_path)
                except Exception as e:
                    errors.append(f"{spec.path}: {e}")
            return IngestResult(downloaded=downloaded, skipped=skipped, errors=errors)
        finally:
            client.close()

if __name__ == "__main__":
    # Example — replace with real values before running.
    ing = FTPIngestor(
        host="sftp.example.com",
        username="USER",
        password="PASS",
        remote_dir="/incoming/costfeeds",
        local_dir=Path("./_downloads"),
        use_sftp=True,
        filename_glob="*.csv",
        enforce_size_match=False,
    )
    # result = ing.download_all()
    # print(result)
