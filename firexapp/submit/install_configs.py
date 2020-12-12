from typing import NamedTuple
import json
from urllib.parse import urljoin, urlparse

from firexapp.common import render_template
from firexapp.submit.uid import Uid


class FireXViewerTemplates(NamedTuple):
    viewer_base: str = ""
    run_path_template: str = ""
    task_path_template: str = ""
    run_logs_root_path_template: str = ""
    run_logs_entry_path_template: str = ""


# Data-only representation of the config. This is expected to EXACTLY reflect the contents of the config file.
# Utilities on top of this data should go in the FireXInstallConfigs class.
class FireXRawInstallConfigs(NamedTuple):
    viewer_templates: FireXViewerTemplates = None
    required_tracking_services: list = []

class FireXInstallConfigError(Exception):
    pass


def load_install_configs(uid: Uid, install_config_path: str):
    if install_config_path is None:
        # default configs
        raw_configs = FireXRawInstallConfigs(None, [])
    else:
        try:
            with open(install_config_path) as fp:
                # TODO: consider copying the install configs to debug dir within run dir, then loading the copied file.
                install_configs_dict = json.load(fp)
        except (OSError, json.JSONDecodeError) as e:
            raise FireXInstallConfigError(f"Failed to load install config from {install_config_path}") from e
        else:
            if install_configs_dict.get('viewer_templates'):
                viewer_config = FireXViewerTemplates(**install_configs_dict['viewer_templates'])
            else:
                viewer_config = None
            raw_configs = FireXRawInstallConfigs(**{**install_configs_dict, 'viewer_templates': viewer_config})
    return FireXInstallConfigs(uid, raw_configs)


class FireXInstallConfigs:
    """Utility functionality on top of data-only representation of configs."""

    def __init__(self, uid: Uid, raw_configs: FireXRawInstallConfigs):
        self.raw_configs = raw_configs
        self.run_url = self.get_run_url(str(uid)) if self.has_viewer() else None

    def has_viewer(self):
        return self.raw_configs.viewer_templates is not None

    def get_run_url(self, firex_id: str) -> str:
        return self._template_viewer_url(self.raw_configs.viewer_templates.run_path_template,
                                         {'firex_id': firex_id})

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
