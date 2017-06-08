from __future__ import print_function
from __future__ import unicode_literals

import os
from ConfigParser import SafeConfigParser
from StringIO import StringIO


def get_user_cache_dir():
    xdg_cache_home = os.getenv('XDG_CACHE_HOME')
    if xdg_cache_home is not None:
        return xdg_cache_home

    home = os.getenv('HOME')
    if home is None:
        raise RuntimeError('Cannot find user home directory')
    return os.path.join(home, '.cache')


def get_user_config_dir():
    xdg_config_home = os.getenv('XDG_CONFIG_HOME')
    if xdg_config_home is not None:
        return xdg_config_home

    home = os.getenv('HOME')
    if home is None:
        raise RuntimeError('Cannot find user home directory')
    return os.path.join(home, '.config')


class Configuration(object):

    DEFAULT_CONFIG = """[database]
driver=sqlite://%(user_cache_dir)s/db'
"""
    config = None

    def read(self, path=None):
        if path is None:
            # Read from config dir
            path = os.path.join(get_user_config_dir(), 'glacier-cli', 'config.ini')
        defaults = {'user_cache_dir': get_user_cache_dir(),
                    'user_config_dir': get_user_config_dir()}
        parser = SafeConfigParser()
        parser.readfp(self.default_buf(), '<default>')
        parser.read([path])
        self.config = {}
        for section in parser.sections():
            for option in parser.options(section):
                conf_section = self.config.setdefault(section, {})
                conf_section[option] = parser.get(section, option, vars=defaults)

    def default_buf(self):
        buf = StringIO()
        buf.write(self.DEFAULT_CONFIG)
        buf.seek(0)
        return buf

    def __getitem__(self, item):
        return self.config.__getitem__(item)


configuration = Configuration()
