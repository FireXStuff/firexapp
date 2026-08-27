"""Validate that the installed firexapp artifact owns the FireXKit code."""

import re
from importlib import metadata, resources
from pathlib import Path


def main() -> None:
    firexapp_distribution = metadata.distribution("firexapp")

    try:
        metadata.distribution("firexkit")
    except metadata.PackageNotFoundError:
        pass
    else:
        raise AssertionError("The standalone firexkit distribution is installed")

    requirements = firexapp_distribution.requires or []
    requirement_names = {
        match.group(1).lower().replace("_", "-")
        for requirement in requirements
        if (match := re.match(r"\s*([A-Za-z0-9._-]+)", requirement))
    }
    if "firexkit" in requirement_names:
        raise AssertionError("firexapp still declares a dependency on firexkit")

    distribution_files = {
        str(path) for path in firexapp_distribution.files or []
    }
    expected_files = {
        "firexkit/__init__.py",
        "firexkit/task.py",
        "firexkit/resources/firex.css",
        "firexkit/resources/firex_logo.png",
        "firexkit/templates/log_template.html",
        "firexkit/install_resources/cloud-ci-install-configs.json",
    }
    missing_files = expected_files - distribution_files
    if missing_files:
        raise AssertionError(
            f"firexapp artifact is missing FireXKit files: {sorted(missing_files)}"
        )

    import firexapp
    import firexkit

    if firexkit.__version__ != firexapp.__version__:
        raise AssertionError(
            "firexkit and firexapp must report the same owning distribution version"
        )

    firexkit_path = Path(firexkit.__file__).resolve()
    distribution_root = Path(firexapp_distribution.locate_file("")).resolve()
    if not firexkit_path.is_relative_to(distribution_root):
        raise AssertionError(
            f"firexkit was imported from {firexkit_path}, outside {distribution_root}"
        )

    for resource_path in (
        resources.files("firexkit") / "resources" / "firex.css",
        resources.files("firexkit") / "templates" / "log_template.html",
        resources.files("firexkit")
        / "install_resources"
        / "cloud-ci-install-configs.json",
    ):
        if not resource_path.is_file():
            raise AssertionError(f"Missing packaged resource: {resource_path}")

    firex_core_entry_points = {
        entry_point.name: entry_point.value
        for entry_point in firexapp_distribution.entry_points
        if entry_point.group == "firex.core"
    }
    expected_entry_points = {
        "firexapp": "firexapp",
        "firexkit": "firexkit",
    }
    unexpected_entry_points = {
        name: firex_core_entry_points.get(name)
        for name, value in expected_entry_points.items()
        if firex_core_entry_points.get(name) != value
    }
    if unexpected_entry_points:
        raise AssertionError(
            "Missing or changed firex.core entry points: "
            f"{unexpected_entry_points!r}"
        )

    print(
        "Validated consolidated firexapp artifact "
        f"{firexapp_distribution.version} with the firexkit namespace"
    )


if __name__ == "__main__":
    main()
