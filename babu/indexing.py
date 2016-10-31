import ctypes
import string

CHARSET = string.ascii_letters + string.digits


class DataPointerArray(ctypes.Array):
    _length_ = 10
    _type_ = ctypes.c_int


class IndexPointerArray(ctypes.Array):
    _length_ = len(CHARSET)
    _type_ = ctypes.c_int


class Page(ctypes.Structure):
    _fields_ = [
        ('curr', ctypes.c_char),
        ('data_ptrs', DataPointerArray),
        ('index_ptrs', IndexPointerArray),
        ('next_ptr', ctypes.c_int)
    ]

    def goto(self, char):
        idx = CHARSET.index(char)
        node_ptr = self.index_ptrs[idx]
        if node_ptr:
            return BLOCK.pages[node_ptr]

    def next(self):
        ptr = self.next_ptr
        if ptr:
            return BLOCK.pages[ptr]

    def add_data_ptr(self, data_ptr):
        ptrs = self.data_ptrs
        for i, p in enumerate(ptrs):
            if not p:
                break
        ptrs[i] = data_ptr


class Pages(ctypes.Array):
    _type_ = Page
    _length_ = 1000


class Block(ctypes.Structure):
    _fields_ = [
        ('free', ctypes.c_char),
        ('pages', Pages),
    ]

    @property
    def root(self):
        return self.pages[0]

    def add_page(self, page):
        if not self.free:
            self.free = 1
        self.pages[self.free] = page


BLOCK = None


class FieldIndex(object):
    max_depth = 3

    def __init__(self, field):
        self.field = field
        self.model = field.model
        self.index_name = '%s.%s.index' % (self.model.name, self.field.name)
#         self.init_index()

    def create_index(self):
        from babu.utils import pack_struct

        s = pack_struct(Pages())
        block_file = open(self.index_name, 'w')
        block_file.write(s)
        block_file.close()

    def init_index(self):
        import mmap
        from babu.storage import ProgrammingError

        global BLOCK

        try:
            block_file = open(self.index_name, 'r+b')
        except IOError:
            raise ProgrammingError

        block_buffer = mmap.mmap(block_file.fileno(), 0)
        BLOCK = Block.from_buffer(block_buffer)
        self.block = BLOCK

    def find(self, needle):
        node = self.block.root
        for char in needle[:self.max_depth]:
            node = node.goto(char)
            if node is None:
                break

        if node:
            return node.data_ptrs

    def add_to_index(self, instance):
        value = getattr(instance, self.field.name)
        block = self.block
        node = block.root
        for depth, char in enumerate(value[:self.max_depth]):
            _node = node.goto(char)
            node.add_data_ptr(instance.pk)
            if _node:
                node = _node
            else:
                break
        if depth < self.max_depth:
            for char in value[depth:self.max_depth]:
                page = Page(curr=char, data_ptrs=DataPointerArray(instance.pk))
                block.add_page(page)

    def remove_from_index(self, instance):
        raise NotImplementedError
