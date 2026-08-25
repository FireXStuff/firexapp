#!/usr/bin/env python3
"""Compare legacy setuptools artifacts with artifacts produced by ``uv build``."""

from __future__ import annotations

import argparse
import hashlib
import tarfile
import zipfile
from pathlib import Path
from typing import Callable


ArchiveReader = Callable[[Path], dict[str, bytes]]


def _single_artifact(directory: Path, pattern: str) -> Path:
    matches = sorted(directory.glob(pattern))
    if len(matches) != 1:
        raise ValueError(
            f"Expected one {pattern!r} artifact in {directory}, found: "
            f"{[path.name for path in matches]}"
        )
    return matches[0]


def _read_wheel(path: Path) -> dict[str, bytes]:
    with zipfile.ZipFile(path) as archive:
        # RECORD contains hashes of the archive payload and is derived rather
        # than package content. Compare every other file byte-for-byte.
        return {
            name: archive.read(name)
            for name in archive.namelist()
            if not name.endswith(".dist-info/RECORD")
        }


def _read_sdist(path: Path) -> dict[str, bytes]:
    result: dict[str, bytes] = {}
    roots: set[str] = set()
    with tarfile.open(path, "r:gz") as archive:
        for member in archive.getmembers():
            parts = Path(member.name).parts
            if not parts:
                continue
            roots.add(parts[0])
            if not member.isfile() or len(parts) == 1:
                continue
            extracted = archive.extractfile(member)
            if extracted is None:
                raise ValueError(f"Could not read {member.name} from {path}")
            result[Path(*parts[1:]).as_posix()] = extracted.read()
    if len(roots) != 1:
        raise ValueError(f"Expected one top-level directory in {path}, found {sorted(roots)}")
    return result


def _digest(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _compare_contents(
    artifact_name: str,
    legacy: dict[str, bytes],
    uv: dict[str, bytes],
) -> list[str]:
    errors: list[str] = []
    legacy_names = set(legacy)
    uv_names = set(uv)
    if only_legacy := sorted(legacy_names - uv_names):
        errors.append(f"{artifact_name}: only in legacy artifact: {only_legacy}")
    if only_uv := sorted(uv_names - legacy_names):
        errors.append(f"{artifact_name}: only in uv artifact: {only_uv}")
    for name in sorted(legacy_names & uv_names):
        if legacy[name] != uv[name]:
            errors.append(
                f"{artifact_name}: content differs for {name} "
                f"(legacy sha256={_digest(legacy[name])}, uv sha256={_digest(uv[name])})"
            )
    return errors


def compare_artifact_directories(legacy_dir: Path, uv_dir: Path) -> None:
    comparisons: tuple[tuple[str, str, ArchiveReader], ...] = (
        ("wheel", "*.whl", _read_wheel),
        ("sdist", "*.tar.gz", _read_sdist),
    )
    errors: list[str] = []
    for artifact_name, pattern, reader in comparisons:
        legacy_path = _single_artifact(legacy_dir, pattern)
        uv_path = _single_artifact(uv_dir, pattern)
        comparison_errors = _compare_contents(
            artifact_name, reader(legacy_path), reader(uv_path)
        )
        errors.extend(comparison_errors)
        if not comparison_errors:
            print(
                f"{artifact_name}: {legacy_path.name} and {uv_path.name} "
                "have matching contents"
            )
    if errors:
        raise SystemExit("Artifact comparison failed:\n- " + "\n- ".join(errors))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("legacy_dir", type=Path)
    parser.add_argument("uv_dir", type=Path)
    args = parser.parse_args()
    compare_artifact_directories(args.legacy_dir, args.uv_dir)


if __name__ == "__main__":
    main()
