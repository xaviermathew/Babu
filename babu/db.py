from babu.connections import FileConnection, FILL_CHAR
from babu.utils import classproperty

MAX_INT = 2 ** 9


class Field(object):
    fill_char = FILL_CHAR
    max_length = None

    def __init__(self, value):
        self.value = value

    def __repr__(self):
        return '<%s:%s>' % (self.__class__.__name__, self.value)

    @classmethod
    def from_db(cls, value):
        return cls(value=value)

    def serialize(self):
        return self.value

    def to_db(self):
        return self.serialize().ljust(self.max_length, self.fill_char)

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

    def serialize(self):
        return str(self.value)


class CharField(Field):
    pass


class Model(object):
    connections = {}
    connection_class = FileConnection
    connection = None

    name = None
    fields = {}
    pk = None

    @classmethod
    def get_or_create_connection(cls):
        db_name = cls.name
        connections = Model.connections
        if db_name not in connections:
            connections[db_name] = cls.connection_class(db_name, cls.row_width)
        return connections[db_name]

    def __init__(self, **kwargs):
        d = {}
        for field_name, value in kwargs.items():
            field_class = self.fields[field_name]
            d[field_name] = field_class(value)
        self.data = d

    def __repr__(self):
        return '<%s - %s:%s' % (self.__class__.__name__, self.pk, self.data)

    @classproperty
    def row_width(cls):
        w = 0
        for field_class in cls.fields.values():
            w += field_class.max_length
        return w

    @classproperty
    def connection(cls):
        return cls.get_or_create_connection()

    def to_db(self):
        s = ''
        for _field_name, field in self.data.items():
            s += field.to_db()
        return s

    @classmethod
    def from_db(cls, row):
        d = {}
        pos = 0
        for field_name, field_class in cls.fields.items():
            d[field_name] = row[pos: pos + field_class.max_length]
            pos += field_class.max_length
        return cls(**d)

    def create(self):
        row = self.to_db()
        self.pk = self.connection.insert(row)

    def update(self):
        row = self.to_db()
        self.connection.update(self.pk, row)

    def save(self):
        if self.pk is not None:
            self.update()
        else:
            self.create()

    @classmethod
    def iterator(cls):
        data = cls.connection.fetch_all()
        for pk, row in data:
            instance = cls.from_db(row)
            instance.pk = pk
            yield instance

    @classmethod
    def all(cls):
        return list(cls.iterator())

    @classmethod
    def get(cls, pk):
        row = cls.connection.get(pk)
        instance = cls.from_db(row)
        instance.pk = pk
        return instance

    @classmethod
    def count(cls):
        return cls.connection.count()

    def delete(self):
        if self.pk:
            self.connection.delete(self.pk)
