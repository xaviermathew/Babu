class classproperty(object):
    def __init__(self, fget):
        self.fget = fget

    def __get__(self, owner_self, owner_cls):
        return self.fget(owner_cls)


def unpack_into_struct(struct_instance, s):
    import ctypes

    fit = min(len(s), ctypes.sizeof(struct_instance))
    ctypes.memmove(ctypes.addressof(struct_instance), s, fit)


def pack_struct(s):
    return buffer(s)[:]


def model_to_struct(model):
    import ctypes

    fields = [('is_deleted', ctypes.c_bool)]
    for field_name, field_class in model.fields.items():
        fields.append((field_name, field_class.ctype))

    class Record(ctypes.Structure):
        _fields_ = fields
    return Record
