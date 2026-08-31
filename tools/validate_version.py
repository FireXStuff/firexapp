"""Validate static project, tag, editable, and generated UI versions."""

import argparse
import os
import re
import subprocess
from importlib import metadata
from pathlib import Path

import firexapp


ROOT = Path(__file__).resolve().parent.parent
FULL_GIT_SHA = re.compile(r"[0-9a-f]{40}")


def _git_head() -> str:
    return subprocess.check_output(
        ["git", "rev-parse", "HEAD"],
        cwd=ROOT,
        text=True,
    ).strip()


def _validate_generated_ui() -> None:
    ui_root = ROOT / "firex_flame_ui"
    ui_version = (ui_root / "VERSION").read_text(encoding="utf-8").strip()
    ui_revision = (ui_root / "COMMITHASH").read_text(
        encoding="utf-8"
    ).strip()

    if ui_version != firexapp.__version__:
        raise AssertionError(
            f"Flame UI version {ui_version!r} does not match "
            f"FireXApp {firexapp.__version__!r}"
        )
    if FULL_GIT_SHA.fullmatch(ui_revision) is None:
        raise AssertionError(f"Invalid Flame UI commit hash: {ui_revision!r}")
    expected_revision = _git_head()
    if ui_revision != expected_revision:
        raise AssertionError(
            f"Flame UI commit {ui_revision} does not match {expected_revision}"
        )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--generated-ui",
        action="store_true",
        help="also validate the production UI version and commit stamps",
    )
    args = parser.parse_args()

    distribution_version = metadata.version("firexapp")
    if distribution_version != firexapp.__version__:
        raise AssertionError(
            f"Installed metadata reports {distribution_version!r}, but "
            f"firexapp reports {firexapp.__version__!r}"
        )

    ci_tag = os.environ.get("CI_COMMIT_TAG")
    if ci_tag and ci_tag != distribution_version:
        raise AssertionError(
            f"Release tag {ci_tag!r} does not match project version "
            f"{distribution_version!r}"
        )

    if args.generated_ui:
        _validate_generated_ui()

    print(f"Validated FireXApp project version {distribution_version}")


if __name__ == "__main__":
    main()
