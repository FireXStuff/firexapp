"""Validate that the installed firexapp artifact owns absorbed OSS code."""

import re
from importlib import metadata, resources
from pathlib import Path


def _canonical_name(name: str) -> str:
    return name.lower().replace("_", "-").replace(".", "-")


def main() -> None:
    firexapp_distribution = metadata.distribution("firexapp")

    absorbed_distributions = (
        "firexkit",
        "firex-blaze",
        "firex-bundle-ci",
        "firex-keeper",
        "firex-flame",
        "firex-flame-ui",
    )
    for distribution_name in absorbed_distributions:
        try:
            metadata.distribution(distribution_name)
        except metadata.PackageNotFoundError:
            pass
        else:
            raise AssertionError(
                f"The standalone {distribution_name} distribution is installed"
            )

    requirements = firexapp_distribution.requires or []
    requirement_names = {
        _canonical_name(match.group(1))
        for requirement in requirements
        if (match := re.match(r"\s*([A-Za-z0-9._-]+)", requirement))
    }
    unexpected_requirements = set(absorbed_distributions) & requirement_names
    if unexpected_requirements:
        raise AssertionError(
            "firexapp still declares absorbed distribution dependencies: "
            f"{sorted(unexpected_requirements)}"
        )
    if not any(
        re.fullmatch(
            r"\s*confluent[-_.]kafka\s*==\s*2\.12\.0\s*",
            requirement,
            flags=re.IGNORECASE,
        )
        for requirement in requirements
    ):
        raise AssertionError(
            "firexapp must own Blaze's confluent-kafka==2.12.0 dependency"
        )
    for dependency_name in ("lxml", "xunitmerge"):
        if _canonical_name(dependency_name) not in requirement_names:
            raise AssertionError(
                "firexapp must own firex-bundle-ci's direct dependency: "
                f"{dependency_name}"
            )
    if not any(
        re.fullmatch(
            r"\s*sqlalchemy\s*<\s*2\s*",
            requirement,
            flags=re.IGNORECASE,
        )
        for requirement in requirements
    ):
        raise AssertionError(
            "firexapp must own Keeper's SQLAlchemy<2 dependency"
        )
    flame_exact_requirements = {
        "Flask==3.1.0": r"\s*flask\s*==\s*3\.1\.0\s*",
        "Flask-AutoIndex==0.6.6": (
            r"\s*flask[-_.]autoindex\s*==\s*0\.6\.6\s*"
        ),
        "Werkzeug==3.1.3": r"\s*werkzeug\s*==\s*3\.1\.3\s*",
        "python-socketio==5.3.0": (
            r"\s*python[-_.]socketio\s*==\s*5\.3\.0\s*"
        ),
        "jsonpath-ng==1.7.0": (
            r"\s*jsonpath[-_.]ng\s*==\s*1\.7\.0\s*"
        ),
        "cryptography==42.0.7": (
            r"\s*cryptography\s*==\s*42\.0\.7\s*"
        ),
        "bcrypt==4.1.3": r"\s*bcrypt\s*==\s*4\.1\.3\s*",
    }
    for (
        expected_requirement,
        requirement_pattern,
    ) in flame_exact_requirements.items():
        if not any(
            re.fullmatch(
                requirement_pattern,
                requirement,
                flags=re.IGNORECASE,
            )
            for requirement in requirements
        ):
            raise AssertionError(
                "firexapp must own Flame's exact dependency: "
                f"{expected_requirement}"
            )
    for dependency_name in (
        "requests",
        "beautifulsoup4",
        "paramiko",
        "gevent-websocket",
        "gevent",
        "importlib-resources",
    ):
        if _canonical_name(dependency_name) not in requirement_names:
            raise AssertionError(
                "firexapp must own Flame's direct dependency: "
                f"{dependency_name}"
            )

    distribution_files = {str(path) for path in firexapp_distribution.files or []}
    expected_files = {
        "firexkit/__init__.py",
        "firexkit/task.py",
        "firexkit/resources/firex.css",
        "firexkit/resources/firex_logo.png",
        "firexkit/templates/log_template.html",
        "firexkit/install_resources/cloud-ci-install-configs.json",
        "firex_blaze/__init__.py",
        "firex_blaze/__main__.py",
        "firex_blaze/_version.py",
        "firex_blaze/blaze_event_consumer.py",
        "firex_blaze/blaze_helper.py",
        "firex_blaze/blaze_launcher.py",
        "firex_blaze/fast_blaze_helper.py",
        "firex_bundle_ci/__init__.py",
        "firex_bundle_ci/_version.py",
        "firex_bundle_ci/tasks.py",
        "firex_keeper/__init__.py",
        "firex_keeper/__main__.py",
        "firex_keeper/_version.py",
        "firex_keeper/db_model.py",
        "firex_keeper/keeper_event_consumer.py",
        "firex_keeper/keeper_helper.py",
        "firex_keeper/keeper_launcher.py",
        "firex_keeper/persist.py",
        "firex_keeper/task_query.py",
        "firex_flame/__init__.py",
        "firex_flame/__main__.py",
        "firex_flame/_version.py",
        "firex_flame/api.py",
        "firex_flame/controller.py",
        "firex_flame/event_broker_processor.py",
        "firex_flame/event_file_processor.py",
        "firex_flame/flame_helper.py",
        "firex_flame/flame_task_graph.py",
        "firex_flame/launcher.py",
        "firex_flame/main_app.py",
        "firex_flame/model_dumper.py",
        "firex_flame/templates/index.html",
        "firex_flame/web_app.py",
        "firex_flame_ui/COMMITHASH",
        "firex_flame_ui/VERSION",
        "firex_flame_ui/__init__.py",
        "firex_flame_ui/assets/firex_logo.6409b05e.png",
        "firex_flame_ui/assets/index.0fe0735d.js",
        "firex_flame_ui/assets/index.560b9848.css",
        "firex_flame_ui/index.html",
        "firex_flame_ui/send-firex-user-config.html",
    }
    missing_files = expected_files - distribution_files
    if missing_files:
        raise AssertionError(
            "firexapp artifact is missing absorbed component files: "
            f"{sorted(missing_files)}"
        )
    if not any(
        path.endswith(".dist-info/licenses/LICENSE")
        for path in distribution_files
    ):
        raise AssertionError("firexapp artifact is missing its root license")

    import firexapp
    import firex_blaze
    import firex_bundle_ci
    import firex_flame
    import firex_flame_ui
    import firex_keeper
    import firexkit

    for namespace in (
        firexkit,
        firex_blaze,
        firex_bundle_ci,
        firex_keeper,
        firex_flame,
        firex_flame_ui,
    ):
        if namespace.__version__ != firexapp.__version__:
            raise AssertionError(
                f"{namespace.__name__} and firexapp must report the same "
                "owning distribution version"
            )

    distribution_root = Path(firexapp_distribution.locate_file("")).resolve()
    for namespace in (
        firexkit,
        firex_blaze,
        firex_bundle_ci,
        firex_keeper,
        firex_flame,
        firex_flame_ui,
    ):
        namespace_path = Path(namespace.__file__).resolve()
        if not namespace_path.is_relative_to(distribution_root):
            raise AssertionError(
                f"{namespace.__name__} was imported from {namespace_path}, "
                f"outside {distribution_root}"
            )

    for resource_path in (
        resources.files("firexkit") / "resources" / "firex.css",
        resources.files("firexkit") / "templates" / "log_template.html",
        resources.files("firexkit")
        / "install_resources"
        / "cloud-ci-install-configs.json",
        resources.files("firex_flame") / "templates" / "index.html",
    ):
        if not resource_path.is_file():
            raise AssertionError(f"Missing packaged resource: {resource_path}")

    flame_ui_root = resources.files("firex_flame_ui")
    for resource_path in (
        flame_ui_root / "COMMITHASH",
        flame_ui_root / "VERSION",
        flame_ui_root / "index.html",
        flame_ui_root / "send-firex-user-config.html",
    ):
        if not resource_path.is_file():
            raise AssertionError(f"Missing Flame UI resource: {resource_path}")
    if not (flame_ui_root / "assets").is_dir():
        raise AssertionError("Installed Flame UI is missing its assets directory")

    installed_entry_points = {
        (entry_point.group, entry_point.name): entry_point.value
        for entry_point in firexapp_distribution.entry_points
    }
    expected_entry_points = {
        ("firex.core", "firexapp"): "firexapp",
        ("firex.core", "firexkit"): "firexkit",
        ("firex.bundles", "firex-bundle-ci"): "firex_bundle_ci",
        ("console_scripts", "firex_blaze"): "firex_blaze.__main__:main",
        ("console_scripts", "firex_flame"): "firex_flame.__main__:main",
        ("console_scripts", "firex_keeper"): "firex_keeper.__main__:main",
        (
            "firex_tracking_service",
            "firex_blaze_launcher",
        ): "firex_blaze.blaze_launcher:FireXBlazeLauncher",
        (
            "firex_tracking_service",
            "firex_keeper_launcher",
        ): "firex_keeper.keeper_launcher:FireXKeeperLauncher",
        (
            "firex_tracking_service",
            "flame_launcher",
        ): "firex_flame.launcher:FlameLauncher",
    }
    changed_entry_points = {
        key: installed_entry_points.get(key)
        for key, value in expected_entry_points.items()
        if installed_entry_points.get(key) != value
    }
    if changed_entry_points:
        raise AssertionError(
            "Missing or changed consolidated entry points: "
            f"{changed_entry_points!r}"
        )

    from firex_blaze.blaze_launcher import FireXBlazeLauncher

    blaze_version = FireXBlazeLauncher.get_pkg_version_info()
    if blaze_version.pkg != "firex-blaze":
        raise AssertionError(
            f"Blaze changed its reported package name: {blaze_version.pkg!r}"
        )
    if blaze_version.version != firexapp.__version__:
        raise AssertionError(
            "Blaze tracking service must report the owning firexapp version"
        )

    from firex_keeper.keeper_launcher import FireXKeeperLauncher

    keeper_version = FireXKeeperLauncher.get_pkg_version_info()
    if keeper_version.pkg != "firex-keeper":
        raise AssertionError(
            f"Keeper changed its reported package name: {keeper_version.pkg!r}"
        )
    if keeper_version.version != firexapp.__version__:
        raise AssertionError(
            "Keeper tracking service must report the owning firexapp version"
        )

    from firex_flame.launcher import FlameLauncher

    flame_version = FlameLauncher.get_pkg_version_info()
    if flame_version.pkg != "firex-flame":
        raise AssertionError(
            f"Flame changed its reported package name: {flame_version.pkg!r}"
        )
    if flame_version.version != firexapp.__version__:
        raise AssertionError(
            "Flame tracking service must report the owning firexapp version"
        )

    from firex_bundle_ci import tasks as bundle_ci_tasks

    expected_bundle_ci_tasks = {
        "AggregateCoverage",
        "AggregateXunit",
        "CollectXunits",
        "GenerateHtmlCoverage",
        "RunAllIntegrationTests",
        "RunIntegrationTests",
        "RunUnitAndIntegrationTests",
        "RunUnitTests",
    }
    missing_bundle_ci_tasks = {
        task_name
        for task_name in expected_bundle_ci_tasks
        if not hasattr(bundle_ci_tasks, task_name)
    }
    if missing_bundle_ci_tasks:
        raise AssertionError(
            "firexapp artifact is missing firex-bundle-ci tasks: "
            f"{sorted(missing_bundle_ci_tasks)}"
        )

    print(
        "Validated consolidated firexapp artifact "
        f"{firexapp_distribution.version} with the firexkit, firex_blaze, "
        "firex_bundle_ci, firex_keeper, firex_flame, and firex_flame_ui "
        "namespaces"
    )


if __name__ == "__main__":
    main()
