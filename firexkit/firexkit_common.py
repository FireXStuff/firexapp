import os

from jinja2 import Environment, FileSystemLoader, select_autoescape

REPLACEMENT_TASK_NAME_POSTFIX = "_orig"

JINJA_ENV = Environment(
    # Cannot use PackageLoader because we override pkg_resources for module load speed.
    loader=FileSystemLoader(os.path.join(os.path.dirname(__file__), "templates")),
    autoescape=select_autoescape(["html", "xml"]),
)


def get_link(
    url, text=None, html_class=None, title_attribute=None, attrs=None, other_elements=""
):
    """Creates an html anchor."""
    if attrs is None:
        attrs = {}
    return JINJA_ENV.get_template("link.html").render(
        url=url,
        text=text,
        html_class=html_class,
        title_attribute=title_attribute,
        attrs=attrs,
        other_elements=other_elements,
    )


def sec2hms(seconds: float) -> str:
    """
    A duration in seconds, human readable as ``[[<N>h]<N>m]<N>s``.

    Trailing units are always present and leading ones only when non-zero, so
    ``3723`` reads as ``1h2m3s`` and ``90`` as ``1m30s``. Fractions are truncated.
    """
    sign = "-" if seconds < 0 else ""
    seconds = abs(seconds)

    h = int(seconds / (60 * 60))
    seconds -= h * (60 * 60)
    m = int(seconds / 60)
    seconds -= m * 60

    # int(), not a :.0f format spec: %u truncated the leftover fraction and :.0f
    # would round it, so 119.6s would turn into 2m0s rather than 1m59s.
    hms = f"{int(seconds)}s"
    if m:
        hms = f"{m}m" + hms
    if h:
        hms = f"{h}h" + hms
    return sign + hms


class ModifyUmask:
    def __init__(self, new_umask):
        self.new_umask = new_umask

    def __enter__(self):
        self.original_umask = os.umask(self.new_umask)

    def __exit__(self, exc_type, exc_val, exc_tb):
        os.umask(self.original_umask)
