from babu.storage import FixedLengthRecordStorage
from babu.utils import classproperty

MAX_INT = 2 ** 9


class Field(object):
    max_length = None

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
    def field_factory(cls, max_length):
        bases = (cls,)
        data = {'max_length': max_length}
        return type(cls.__name__, bases, data)


class IntegerField(Field):
    max_length = len(str(MAX_INT))

    @classmethod
    def from_db(cls, value):
        return cls(value=int(value))

    def to_db(self):
        return str(self.value)


class CharField(Field):
    pass


class Manager(object):
    connections = {}
    storage_class = FixedLengthRecordStorage

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
            d[field_name] = field_class(kwargs.get(field_name))
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
