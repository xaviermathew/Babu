def chunker(seq, size):
    return ((pos, seq[pos:pos + size]) for pos in xrange(0, len(seq), size))


class classproperty(object):
    def __init__(self, fget):
        self.fget = fget

    def __get__(self, owner_self, owner_cls):
        return self.fget(owner_cls)
