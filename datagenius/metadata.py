from typing import Callable


class GeniusMetadata(Callable):
    """
    When coupled with Genius operations, tracks their activity and
    provides methods for reporting out on it.
    """
    def __init__(self):
        self._transformations: dict = dict()
        self._transmutations: dict = dict()

    def __call__(self, *transmutations):
        pass

