import sys
from pathlib import Path

sys.path.append(Path('~', '.zipline').expanduser().as_posix())

from zipline.data.bundles import register
from quandl_custom_bundle import quandl_to_bundle


register('quandl_custom_bundle',
         quandl_to_bundle(),
         calendar_name='NYSE',
         )
