"""Validate distribution artifacts produced for the FireX uv workspace."""

from __future__ import annotations

import argparse
import re
import tarfile
import zipfile
from dataclasses import dataclass
from email.parser import BytesParser
from pathlib import Path, PurePosixPath

try:
    import tomllib
except ModuleNotFoundError:  # Python 3.10
    import tomli as tomllib


def canonicalize_name(name: str) -> str:
    return re.sub(r"[-_.]+", "-", name).lower()


@dataclass(frozen=True)
class WorkspaceProject:
    name: str
    path: Path
    module_roots: frozenset[str]

    @property
    def canonical_name(self) -> str:
        return canonicalize_name(self.name)


def read_project(path: Path) -> WorkspaceProject:
    pyproject = path / "pyproject.toml"
    with pyproject.open("rb") as pyproject_file:
        data = tomllib.load(pyproject_file)

    try:
        name = data["project"]["name"]
        module_roots = data["tool"]["firex"]["workspace-validation"]["module-roots"]
    except KeyError as exc:
        raise ValueError(
            f"Missing workspace validation metadata in {pyproject}"
        ) from exc

    if not module_roots or any("/" in root for root in module_roots):
        raise ValueError(f"Invalid module-roots in {pyproject}: {module_roots!r}")
    return WorkspaceProject(name, path, frozenset(module_roots))


def discover_projects(root: Path) -> dict[str, WorkspaceProject]:
    root_project = read_project(root)
    with (root / "pyproject.toml").open("rb") as pyproject_file:
        root_data = tomllib.load(pyproject_file)

    try:
        member_patterns = root_data["tool"]["uv"]["workspace"]["members"]
    except KeyError as exc:
        raise ValueError("Root pyproject.toml does not declare a uv workspace") from exc

    member_paths = {
        match.resolve()
        for pattern in member_patterns
        for match in root.glob(pattern)
        if (match / "pyproject.toml").is_file()
    }
    projects = [root_project, *(read_project(path) for path in sorted(member_paths))]
    projects_by_name = {project.canonical_name: project for project in projects}
    if len(projects_by_name) != len(projects):
        raise ValueError("Workspace contains duplicate normalized distribution names")
    return projects_by_name


def metadata_name(metadata: bytes, artifact: Path) -> str:
    name = BytesParser().parsebytes(metadata).get("Name")
    if not name:
        raise ValueError(f"Artifact metadata has no Name field: {artifact}")
    return canonicalize_name(name)


def validate_wheel(
    artifact: Path,
    projects: dict[str, WorkspaceProject],
) -> str:
    with zipfile.ZipFile(artifact) as wheel:
        names = wheel.namelist()
        metadata_files = [
            name for name in names if name.endswith(".dist-info/METADATA")
        ]
        if len(metadata_files) != 1:
            raise ValueError(
                f"Expected one METADATA file in {artifact}, found {metadata_files}"
            )
        project_name = metadata_name(wheel.read(metadata_files[0]), artifact)

    if project_name not in projects:
        raise ValueError(f"Unexpected distribution {project_name!r} in {artifact}")

    payload_roots = {
        PurePosixPath(name).parts[0]
        for name in names
        if name and not PurePosixPath(name).parts[0].endswith((".dist-info", ".data"))
    }
    expected_roots = projects[project_name].module_roots
    if payload_roots != expected_roots:
        raise ValueError(
            f"Unexpected wheel roots in {artifact}: expected "
            f"{sorted(expected_roots)}, found {sorted(payload_roots)}"
        )
    return project_name


def validate_sdist(
    artifact: Path,
    projects: dict[str, WorkspaceProject],
) -> str:
    with tarfile.open(artifact, "r:gz") as sdist:
        members = [member.name for member in sdist.getmembers() if member.isfile()]
        metadata_files = [
            name
            for name in members
            if len(PurePosixPath(name).parts) == 2 and name.endswith("/PKG-INFO")
        ]
        if len(metadata_files) != 1:
            raise ValueError(
                f"Expected one top-level PKG-INFO in {artifact}, found {metadata_files}"
            )
        extracted_metadata = sdist.extractfile(metadata_files[0])
        if extracted_metadata is None:
            raise ValueError(f"Could not read {metadata_files[0]} from {artifact}")
        project_name = metadata_name(extracted_metadata.read(), artifact)

    if project_name not in projects:
        raise ValueError(f"Unexpected distribution {project_name!r} in {artifact}")

    relative_parts = [PurePosixPath(name).parts[1:] for name in members]
    if not any(parts == ("pyproject.toml",) for parts in relative_parts):
        raise ValueError(f"Missing pyproject.toml in {artifact}")

    own_roots = projects[project_name].module_roots
    missing_roots = {
        root for root in own_roots if not any(root in parts for parts in relative_parts)
    }
    if missing_roots:
        raise ValueError(
            f"Missing package roots in {artifact}: {sorted(missing_roots)}"
        )

    other_roots = {
        root
        for name, project in projects.items()
        if name != project_name
        for root in project.module_roots
    }
    leaked_roots = {
        root for root in other_roots if any(root in parts for parts in relative_parts)
    }
    if leaked_roots:
        raise ValueError(f"Foreign package roots in {artifact}: {sorted(leaked_roots)}")
    return project_name


def validate_artifacts(
    artifact_dir: Path,
    requested_packages: list[str],
) -> None:
    root = Path(__file__).resolve().parents[1]
    projects = discover_projects(root)
    expected_names = (
        {canonicalize_name(name) for name in requested_packages}
        if requested_packages
        else set(projects)
    )
    unknown_requested = expected_names - set(projects)
    if unknown_requested:
        raise ValueError(
            f"Unknown requested workspace packages: {sorted(unknown_requested)}"
        )

    artifacts = sorted(path for path in artifact_dir.iterdir() if path.is_file())
    unexpected_files = [
        path.name
        for path in artifacts
        if not (path.name.endswith(".whl") or path.name.endswith(".tar.gz"))
    ]
    if unexpected_files:
        raise ValueError(f"Unexpected files in {artifact_dir}: {unexpected_files}")

    artifacts_by_project: dict[str, set[str]] = {}
    for artifact in artifacts:
        if artifact.name.endswith(".whl"):
            project_name = validate_wheel(artifact, projects)
            kind = "wheel"
        else:
            project_name = validate_sdist(artifact, projects)
            kind = "sdist"
        if project_name not in expected_names:
            raise ValueError(f"Unexpected workspace artifact: {artifact.name}")
        kinds = artifacts_by_project.setdefault(project_name, set())
        if kind in kinds:
            raise ValueError(f"Duplicate {kind} for {project_name} in {artifact_dir}")
        kinds.add(kind)

    expected_kinds = {"wheel", "sdist"}
    actual_names = set(artifacts_by_project)
    if actual_names != expected_names:
        raise ValueError(
            f"Workspace artifact names differ: expected {sorted(expected_names)}, "
            f"found {sorted(actual_names)}"
        )
    for project_name, kinds in artifacts_by_project.items():
        if kinds != expected_kinds:
            raise ValueError(
                f"Expected wheel and sdist for {project_name}, found {sorted(kinds)}"
            )

    print(
        "Validated workspace artifacts for: " + ", ".join(sorted(artifacts_by_project))
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("artifact_dir", type=Path)
    parser.add_argument("--package", action="append", default=[])
    args = parser.parse_args()
    validate_artifacts(args.artifact_dir, args.package)


if __name__ == "__main__":
    main()
