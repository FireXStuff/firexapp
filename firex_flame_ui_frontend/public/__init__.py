
import pathlib
__version__ = pathlib.Path(
    pathlib.Path(__file__).resolve().parent,
    'VERSION',
).read_text(encoding='utf-8').strip()
