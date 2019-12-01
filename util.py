"""
Utilities to support the fantasy football analysis modules e.g. file I/O
"""


def load_stat_file(stat_type, period=None):
    """
    Loads a requested stat file, or downloads it if not yet saved. Raises an exception if the
    requested period has not yet started.
    :param stat_type: str 'week' or 'season'
    :param period: the week requested, or None for current week
    :return: a dict representing the file contents
    """

