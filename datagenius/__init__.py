import os

from .element import ZeroNumeric
from .genius import GeniusAccessor
from .io import odbc
from .io.text import get_output_template
from .lib import CleaningGuide, SupplementGuide, RedistributionGuide
from .util import transmutation, nullable

__all__ = [
    'element', 'genius', 'util', 'CleaningGuide', 'SupplementGuide',
    'RedistributionGuide', 'ZeroNumeric', 'transmutation', 'nullable',
    'get_output_template'
]


def _setup():
    from .lib import service
    return service.gather_custom_transmutations(os.getcwd())


# custom_tms = _setup()
