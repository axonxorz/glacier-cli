import os


def get_user_cache_dir():
    xdg_cache_home = os.getenv('XDG_CACHE_HOME')
    if xdg_cache_home is not None:
        return xdg_cache_home

    home = os.getenv('HOME')
    if home is None:
        raise RuntimeError('Cannot find user home directory')
    return os.path.join(home, '.cache')
