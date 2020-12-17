from typing import NamedTuple, Optional
import json
from urllib.parse import urljoin, urlparse
import shutil
import os

from firexkit.resources import get_packaged_install_config_path
from firexapp.submit.uid import Uid
from firexapp.common import render_template

INSTALL_CONFIGS_RUN_BASENAME = 'install-configs.json'

class FireXViewerTemplates(NamedTuple):
    viewer_base: str = ""
    run_path_template: str = ""
    task_path_template: str = ""
    run_logs_root_path_template: str = ""
    run_logs_entry_path_template: str = ""


# Data-only representation of the config. This is expected to EXACTLY reflect the contents of the config file.
# Utilities on top of this data should go in the FireXInstallConfigs class.
class FireXRawInstallConfigs(NamedTuple):
    viewer_templates: Optional[FireXViewerTemplates] = None
    required_tracking_services: list = []


class FireXInstallConfigError(Exception):
    pass


def install_config_path_from_logs_dir(logs_dir):
    return os.path.join(logs_dir, Uid.debug_dirname, INSTALL_CONFIGS_RUN_BASENAME)


def load_existing_raw_install_config(logs_dir) -> FireXRawInstallConfigs:
    install_config_path = install_config_path_from_logs_dir(logs_dir)
    try:
        with open(install_config_path) as fp:
            install_configs_dict = json.load(fp)
    except (OSError, json.JSONDecodeError) as e:
        raise FireXInstallConfigError(f"Failed to load install config from {install_config_path}") from e
    else:
        if install_configs_dict.get('viewer_templates'):
            viewer_config = FireXViewerTemplates(**install_configs_dict['viewer_templates'])
        else:
            viewer_config = None
        return FireXRawInstallConfigs(**{**install_configs_dict, 'viewer_templates': viewer_config})


class FireXInstallConfigs:
    """Utility functionality on top of data-only representation of configs."""

    def __init__(self, firex_id: str, logs_dir: str, raw_configs: FireXRawInstallConfigs):
        self.firex_id = firex_id
        self.logs_dir = logs_dir
        self.raw_configs = raw_configs
        self.run_url = self.get_run_url() if self.has_viewer() else None

    def has_viewer(self):
        return self.raw_configs.viewer_templates is not None

    def get_run_url(self) -> str:
        return self._template_viewer_url(self.raw_configs.viewer_templates.run_path_template,
                                         {'firex_id': self.firex_id})

    def get_log_entry_url(self, log_entry_rel_run_root) -> str:
        return self._template_viewer_url(self.raw_configs.viewer_templates.run_logs_entry_path_template,
                                         {'firex_id': self.firex_id,
                                          'run_logs_dir': self.logs_dir,
                                          'log_entry_rel_run_root': log_entry_rel_run_root})

    def get_logs_root_url(self) -> str:
        return self._template_viewer_url(self.raw_configs.viewer_templates.run_logs_root_path_template,
                                         {'firex_id': self.firex_id,
                                          'run_logs_dir': self.logs_dir})

    def _template_viewer_url(self, template_str: str, template_args: dict) -> str:
        assert self.has_viewer(), "Callers must verify install configs specify URLs."

        rendered_template = render_template(template_str, template_args)
        parsed_template = urlparse(rendered_template)
        if parsed_template.scheme and parsed_template.netloc:
            # If the template can be parsed as a URL with scheme and netloc (host), it's already absolute,
            # so don't prepend base_url:
            return rendered_template
        # Assume rendered template is only path portion of URL and needs base prepended to become absolute.
        return urljoin(self.raw_configs.viewer_templates.viewer_base, rendered_template)

    def is_tracking_service_required(self, name) -> bool:
        return name in self.raw_configs.required_tracking_services


def load_existing_install_configs(firex_id: str, logs_dir: str) -> FireXInstallConfigs:
    return FireXInstallConfigs(firex_id, logs_dir, load_existing_raw_install_config(logs_dir))


def load_new_install_configs(firex_id: str, logs_dir: str, install_config_path: str) -> FireXInstallConfigs:
    install_config_copy_path = install_config_path_from_logs_dir(logs_dir)

    # Either write default configs or copy input file.
    if install_config_path is None:
        # default configs
        raw_configs = FireXRawInstallConfigs(None, [])._asdict()
        with open(install_config_copy_path, 'w') as fp:
            json.dump(raw_configs, fp)
    else:
        try:
            if not os.path.isabs(install_config_path) and not os.path.isfile(install_config_path):
                # A non-absolute file that doesn't exist locally can be loaded from firexkit resources.
                resource_install_config = get_packaged_install_config_path(install_config_path)
                if os.path.isfile(resource_install_config):
                    install_config_path = resource_install_config
            shutil.copyfile(install_config_path, install_config_copy_path)
        except OSError as e:
            raise FireXInstallConfigError(f"Failed to load install config from {install_config_path}") from e

    return load_existing_install_configs(firex_id, logs_dir)
