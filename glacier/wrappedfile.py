import os
from functools import wraps


class SeekPastEndError(IOError):
    """Attempted to seek past end of file window"""
    def __init__(self, position, whence):
        super(SeekPastEndError, self).__init__('Attempted to seek past end of file window ({}, {})'.format(position, whence))


def wrap_file_fn(fn):
    @wraps(fn)
    def new_fn(self, *args, **kwargs):
        file_fn = getattr(self.file, fn.__name__)
        return file_fn(*args, **kwargs)
    return new_fn


def wrap_file_prop(prop):
    @property
    @wraps(prop)
    def new_prop(self, *args, **kwargs):
        real_prop = getattr(self.file, prop.__name__)
        return real_prop
    return new_prop


class WrappedFile(object):
    """Wrap a file-like object and only support read-write operations within a specified window.
    Do not make modifications to the underlying .file object, doing so will de-sync the wrapper window
    with the assumed position of the file."""

    def __init__(self, file, start, end):
        self.file = file
        if end < start:
            raise ValueError('WrappedFile end ({}) was before start ({})'.format(end, start))
        self.start = start  # Absolute underlying file byte range start
        self.end = end  # Absolute underlying file byte range end
        self.pos = start  # Absolute underlying file seek position
        self.file.seek(start)

    @wrap_file_fn
    def close(self): pass

    @wrap_file_fn
    def flush(self): pass

    @wrap_file_fn
    def fileno(self): pass

    @wrap_file_fn
    def isatty(self): pass

    def next(self):
        raise NotImplementedError()

    def read(self, size=None):
        """Read up to size bytes, or until the end of the file window"""
        if not size:
            rlen = self.end - self.pos
        else:
            rlen = size
        if rlen == 0:
            return ''  # Fake EOF
        # Clamp rlen to window end
        if self.pos + rlen > self.end:
            rlen = self.end - self.pos
        data = self.file.read(rlen)
        self.pos += len(data)
        return data

    def readline(self, size=None):
        raise NotImplementedError()

    def readlines(self, sizehint=None):
        raise NotImplementedError()

    def seek(self, offset, whence=os.SEEK_SET):
        if whence == os.SEEK_SET:
            new_pos = self.start + offset
            if new_pos > self.end:
                raise SeekPastEndError(offset, whence)
            self.file.seek(self.start + offset)
            self.pos = self.start + offset
        elif whence == os.SEEK_CUR:
            new_pos = self.pos + offset
            if new_pos > self.end:
                raise SeekPastEndError(offset, whence)
            self.pos = new_pos
            self.file.seek(new_pos)
        elif whence == os.SEEK_END:
            new_pos = self.end + offset
            if new_pos > self.end:
                raise SeekPastEndError(offset, whence)
            self.pos = new_pos
            self.file.seek(new_pos)
        else:
            raise ValueError('Unknown seek mode: {}'.format(whence))

    def tell(self):
        return self.pos - self.start

    def write(self, str):
        raise NotImplementedError()

    def writelines(self, sequence):
        raise NotImplementedError

    @wrap_file_prop
    def closed(self): pass

    @wrap_file_prop
    def encoding(self): pass

    @wrap_file_prop
    def mode(self): pass

    @wrap_file_prop
    def newlines(self): pass

    @wrap_file_prop
    def softspace(): pass

