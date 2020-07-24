import os

from .element import ZeroNumeric
from .genius import GeniusAccessor
from .io import odbc
from .io.text import get_output_template
from .lib import CleaningGuide, SupplementGuide, RedistributionGuide
from .util import transmutation, nullable


def _setup():
    from .lib import service
    return service.gather_custom_transmutations(os.getcwd())


custom_tms = _setup()

__all__ = [
    'element', 'genius', 'util', 'CleaningGuide', 'SupplementGuide',
    'RedistributionGuide', 'ZeroNumeric', 'transmutation', 'nullable',
    'get_output_template', 'custom_tms'
]



