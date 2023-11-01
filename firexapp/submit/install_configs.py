from typing import NamedTuple, Optional
import json
from urllib.parse import urljoin, urlparse
import shutil
import os

from firexkit.resources import get_packaged_install_config_path
from firexapp.submit.uid import Uid
from firexapp.common import render_template

INSTALL_CONFIGS_ENV_NAME = 'firex_install_config'
INSTALL_CONFIGS_RUN_BASENAME = 'install-configs.json'


class FireXViewerTemplates(NamedTuple):
    viewer_base: Optional[str] = ""
    run_path_template: str = ""
    task_path_template: str = ""
    run_logs_root_path_template: str = ""
    run_logs_entry_path_template: str = ""


# Data-only representation of the config. This is expected to EXACTLY reflect the contents of the config file.
# Utilities on top of this data should go in the FireXInstallConfigs class.
class FireXRawInstallConfigs(NamedTuple):
    viewer_templates: Optional[FireXViewerTemplates] = None

    # None means "use all installed tracking services". A list means only listed tracking services will be started,
    # and a service with each requested name must be installed. If a service is included here but not present during
    # start, the run will fail.
    requested_tracking_services: Optional[list] = None

    submit_args: Optional[dict] = None


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
        assert self.has_viewer(), "Callers must verify install configs specify URLs."
        return self._template_viewer_url(self.raw_configs.viewer_templates.run_path_template,
                                         {'firex_id': self.firex_id})

    def get_log_entry_url(self, log_entry_rel_run_root) -> str:
        assert self.has_viewer(), "Callers must verify install configs specify URLs."
        return self._template_viewer_url(self.raw_configs.viewer_templates.run_logs_entry_path_template,
                                         {'firex_id': self.firex_id,
                                          'run_logs_dir': self.logs_dir,
                                          'log_entry_rel_run_root': log_entry_rel_run_root})

    def get_logs_root_url(self) -> str:
        assert self.has_viewer(), "Callers must verify install configs specify URLs."
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

    def get_submit_args(self) -> dict:
        return self.raw_configs.submit_args


def load_existing_install_configs(firex_id: str, logs_dir: str) -> FireXInstallConfigs:
    return FireXInstallConfigs(firex_id, logs_dir, load_existing_raw_install_config(logs_dir))


# See https://stackoverflow.com/questions/33181170/how-to-convert-a-nested-namedtuple-to-a-dict/39235373
def isnamedtupleinstance(x):
    _type = type(x)
    bases = _type.__bases__
    if len(bases) != 1 or bases[0] != tuple:
        return False
    fields = getattr(_type, '_fields', None)
    if not isinstance(fields, tuple):
        return False
    return all(type(i)==str for i in fields)


def recursive_named_tuple_asdict(obj):
    if isinstance(obj, dict):
        return {key: recursive_named_tuple_asdict(value) for key, value in obj.items()}
    elif isinstance(obj, list):
        return [recursive_named_tuple_asdict(value) for value in obj]
    elif isnamedtupleinstance(obj):
        return {key: recursive_named_tuple_asdict(value) for key, value in obj._asdict().items()}
    elif isinstance(obj, tuple):
        return tuple(recursive_named_tuple_asdict(value) for value in obj)
    else:
        return obj


def load_new_install_configs(firex_id: str, logs_dir: str, install_config_path: Optional[str],
                             raw_install_config: Optional[FireXRawInstallConfigs] = None) -> FireXInstallConfigs:
    """
    Copies supplied install configs to supplied logs_dir and returns loaded (i.e. deserialized)
    FireXInstallConfigs object. If no install_config_path is supplied, internally-defined default install config is
    used. Install config file is guaranteed to be written to logs_dir.
    """
    assert not (install_config_path and raw_install_config), \
        "Cannot specify both a file to load config from and an explicit config object."
    install_config_copy_path = install_config_path_from_logs_dir(logs_dir)

    # Either write configs or copy input file.
    if install_config_path is None:
        if raw_install_config is not None:
            raw_configs_to_write = raw_install_config
        else:
            # built-in default configs
            raw_configs_to_write = FireXRawInstallConfigs(viewer_templates=None, requested_tracking_services=None)
        with open(install_config_copy_path, 'w') as fp:
            json.dump(recursive_named_tuple_asdict(raw_configs_to_write), fp)
    else:
        # Copy supplied JSON file specifying config.
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
