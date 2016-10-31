import ctypes

from babu.storage import StructStorage
from babu.utils import classproperty

MAX_INT = 2 ** 9


class Field(object):
    name = None
    max_length = None
    db_index = False
    index = None

    def __init__(self, value):
        self.value = value

    def __repr__(self):
        return '<%s:%s>' % (self.__class__.__name__, self.value)

    @classmethod
    def from_db(cls, value):
        return cls(value=value)

    def to_db(self):
        return self.value

    @classmethod
    def field_factory(cls, name, model, max_length=None, db_index=False):
        from babu.indexing import FieldIndex

        bases = (cls,)
        data = {'name': name,
                'model': model,
                'db_index': db_index,}
        if max_length is not None:
            data['max_length'] = max_length
        new_cls = type(cls.__name__, bases, data)
        if db_index:
            new_cls.index = FieldIndex(new_cls)
        return new_cls


class IntegerField(Field):
    max_length = len(str(MAX_INT))
    ctype = ctypes.c_int

    @classmethod
    def from_db(cls, value):
        return cls(value=int(value))

    def to_db(self):
        return str(self.value)


class CharField(Field):
    @classproperty
    def ctype(cls):
        return ctypes.c_char * cls.max_length

    @classmethod
    def create_index(cls):
        from babu.indexing import FieldIndex
        FieldIndex(cls).create_index()

    @classmethod
    def init_index(cls):
        from babu.indexing import FieldIndex
        FieldIndex(cls).init_index()


class Manager(object):
    connections = {}
    storage_class = StructStorage

    def __init__(self, model):
        self.model = model

    def get_or_create_connection(self):
        db_name = self.model.name
        connections = Manager.connections
        if db_name not in connections:
            connections[db_name] = self.storage_class(self.model)
        return connections[db_name]

    @property
    def connection(self):
        return self.get_or_create_connection()

    def create_db(self):
        self.connection.create_db()

    def create(self, instance):
        self.connection.insert(instance)

    def update(self, instance):
        self.connection.update(instance)

    def save(self, instance):
        if instance.pk is not None:
            self.update(instance)
        else:
            self.create(instance)

    def all(self):
        return list(self.connection.all())

    def filter(self, **kwargs):
        return self.connection.filter(**kwargs)

    def get(self, pk):
        return self.connection.get(pk)

    def count(self):
        return self.connection.count()

    def delete(self, instance):
        if instance.pk is not None:
            self.connection.delete(instance.pk)


class Model(object):
    name = None
    fields = {}
    pk = None

    def __init__(self, **kwargs):
        d = {}
        for field_name, field_class in self.fields.items():
            d[field_name] = field_class(value=kwargs.get(field_name))
        self.data = d

    @classproperty
    def objects(cls):
        return Manager(cls)

    def __repr__(self):
        return '<%s - %s:%s' % (self.__class__.__name__, self.pk, self.data)

    def save(self):
        self.objects.save(self)

    def delete(self):
        self.objects.delete(self)
