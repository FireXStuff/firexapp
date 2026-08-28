"""Validate the public contract of an installed ``firexapp`` artifact."""

import importlib
import re
from importlib import metadata, resources
from pathlib import Path


PUBLIC_NAMESPACES = (
    "firexapp",
    "firexkit",
    "firex_blaze",
    "firex_bundle_ci",
    "firex_keeper",
    "firex_flame",
    "firex_flame_ui",
)

LEGACY_DISTRIBUTIONS = (
    "firexkit",
    "firex-blaze",
    "firex-bundle-ci",
    "firex-keeper",
    "firex-flame",
    "firex-flame-ui",
)

EXPECTED_ENTRY_POINTS = {
    ("console_scripts", "firexapp"): "firexapp.application:main",
    ("console_scripts", "firex_blaze"): "firex_blaze.__main__:main",
    ("console_scripts", "firex_flame"): "firex_flame.__main__:main",
    ("console_scripts", "firex_keeper"): "firex_keeper.__main__:main",
    ("console_scripts", "flow_tests"): (
        "firexapp.testing.test_infra:default_main"
    ),
    ("console_scripts", "firex_shutdown"): "firexapp.submit.shutdown:main",
    ("firex.core", "firexapp"): "firexapp",
    ("firex.core", "firexkit"): "firexkit",
    ("firex.bundles", "firex-bundle-ci"): "firex_bundle_ci",
    ("firex_tracking_service", "firex_blaze_launcher"): (
        "firex_blaze.blaze_launcher:FireXBlazeLauncher"
    ),
    ("firex_tracking_service", "firex_keeper_launcher"): (
        "firex_keeper.keeper_launcher:FireXKeeperLauncher"
    ),
    ("firex_tracking_service", "flame_launcher"): (
        "firex_flame.launcher:FlameLauncher"
    ),
}

REQUIRED_RESOURCES = (
    ("firexkit", "resources", "firex.css"),
    ("firexkit", "resources", "firex_logo.png"),
    ("firexkit", "templates", "log_template.html"),
    (
        "firexkit",
        "install_resources",
        "cloud-ci-install-configs.json",
    ),
    ("firex_flame", "templates", "index.html"),
    ("firex_flame_ui", "COMMITHASH"),
    ("firex_flame_ui", "VERSION"),
    ("firex_flame_ui", "index.html"),
    ("firex_flame_ui", "send-firex-user-config.html"),
)


def _validate_distribution_ownership(
    firexapp_distribution: metadata.Distribution,
) -> None:
    for distribution_name in LEGACY_DISTRIBUTIONS:
        try:
            metadata.distribution(distribution_name)
        except metadata.PackageNotFoundError:
            continue
        raise AssertionError(
            "Legacy distribution is installed alongside firexapp: "
            f"{distribution_name}"
        )

    distribution_root = Path(
        firexapp_distribution.locate_file("")
    ).resolve()
    owner = importlib.import_module("firexapp")
    for namespace_name in PUBLIC_NAMESPACES:
        namespace = importlib.import_module(namespace_name)
        if namespace.__version__ != owner.__version__:
            raise AssertionError(
                f"{namespace_name} reports {namespace.__version__}, but "
                f"firexapp reports {owner.__version__}"
            )
        namespace_path = Path(namespace.__file__).resolve()
        if not namespace_path.is_relative_to(distribution_root):
            raise AssertionError(
                f"{namespace_name} was imported from {namespace_path}, "
                f"outside {distribution_root}"
            )


def _validate_resources() -> None:
    for package_name, *relative_parts in REQUIRED_RESOURCES:
        resource = resources.files(package_name).joinpath(*relative_parts)
        if not resource.is_file():
            raise AssertionError(f"Missing packaged resource: {resource}")

    flame_ui_root = resources.files("firex_flame_ui")
    assets_root = flame_ui_root / "assets"
    if not assets_root.is_dir():
        raise AssertionError("Installed Flame UI has no assets directory")

    asset_names = {
        asset.name
        for asset in assets_root.iterdir()
        if asset.is_file()
    }
    missing_asset_types = {
        suffix
        for suffix in (".js", ".css", ".png")
        if not any(name.endswith(suffix) for name in asset_names)
    }
    if missing_asset_types:
        raise AssertionError(
            "Installed Flame UI is missing asset types: "
            f"{sorted(missing_asset_types)}"
        )

    flame_ui_index = (flame_ui_root / "index.html").read_text(
        encoding="utf-8"
    )
    referenced_assets = set(
        re.findall(
            r'(?:src|href)=["\']/flame/(assets/[^"\']+\.(?:js|css))["\']',
            flame_ui_index,
        )
    )
    missing_reference_types = {
        suffix
        for suffix in (".js", ".css")
        if not any(path.endswith(suffix) for path in referenced_assets)
    }
    if missing_reference_types:
        raise AssertionError(
            "Flame UI index is missing asset references for: "
            f"{sorted(missing_reference_types)}"
        )

    missing_referenced_assets = {
        asset
        for asset in referenced_assets
        if not flame_ui_root.joinpath(*asset.split("/")).is_file()
    }
    if missing_referenced_assets:
        raise AssertionError(
            "Flame UI index references missing assets: "
            f"{sorted(missing_referenced_assets)}"
        )


def _validate_entry_points(
    firexapp_distribution: metadata.Distribution,
) -> None:
    installed_entry_points = {
        (entry_point.group, entry_point.name): entry_point.value
        for entry_point in firexapp_distribution.entry_points
    }
    changed_entry_points = {
        key: installed_entry_points.get(key)
        for key, expected_value in EXPECTED_ENTRY_POINTS.items()
        if installed_entry_points.get(key) != expected_value
    }
    if changed_entry_points:
        raise AssertionError(
            "Missing or changed firexapp entry points: "
            f"{changed_entry_points!r}"
        )


def main() -> None:
    firexapp_distribution = metadata.distribution("firexapp")
    distribution_files = {
        str(path) for path in firexapp_distribution.files or ()
    }
    if not any(
        path.endswith(".dist-info/licenses/LICENSE")
        for path in distribution_files
    ):
        raise AssertionError("firexapp artifact is missing its license")

    _validate_distribution_ownership(firexapp_distribution)
    _validate_resources()
    _validate_entry_points(firexapp_distribution)

    print(
        f"Validated firexapp artifact {firexapp_distribution.version}: "
        + ", ".join(PUBLIC_NAMESPACES)
    )


if __name__ == "__main__":
    main()
