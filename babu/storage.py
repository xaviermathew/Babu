import os


class ObjectDoesNotExist(Exception):
    pass


class IntegrityError(Exception):
    pass


class BaseStorage(object):
    def __init__(self, model):
        self.model = model

    def insert(self, row):
        raise NotImplementedError

    def update(self, pk, row):
        raise NotImplementedError

    def get(self, pk):
        raise NotImplementedError

    def fetch_all(self):
        raise NotImplementedError

    def count(self):
        raise NotImplementedError

    def delete(self, pk):
        raise NotImplementedError


class FixedLengthRecordStorage(BaseStorage):
    FILL_CHAR = '\000'

    def __init__(self, model):
        super(FixedLengthRecordStorage, self).__init__(model)
        self.db_name = model.name
        self.row_width = self.calc_row_width(model)
        db_name = self.db_name + '.db'
        try:
            self.file = open(db_name, 'r+b')
        except IOError:
            f = open(db_name, 'w')
            f.close()
            self.file = open(db_name, 'r+b')

    @staticmethod
    def calc_row_width(model):
        w = 0
        for field_class in model.fields.values():
            w += field_class.max_length
        return w

    def _normalize_field(self, field):
        value = field.to_db()
        if value is None:
            value = ''
        return value.ljust(field.max_length, self.FILL_CHAR)

    def to_db(self, instance):
        s = ''
        for field_name in self.model.fields.keys():
            field = instance.data[field_name]
            s += self._normalize_field(field)
        return s

    def from_db(self, row):
        d = {}
        pos = 0
        for field_name, field_class in self.model.fields.items():
            d[field_name] = row[pos: pos + field_class.max_length]
            pos += field_class.max_length
        return self.model(**d)

    def _get_pos_from_pk(self, pk):
        return pk * self.row_width

    def _get_pk_from_pos(self, pos):
        return pos / self.row_width

    def _is_deleted_row(self, row):
        return row == self.FILL_CHAR * self.row_width

    def _assert_pk_valid(self, pk):
        # need this bcs seek()-ing to a non-existent offset doesnt
        # raise errors 
        if pk >= self.total_count():
            raise ObjectDoesNotExist

    def _assert_data_valid(self, row):
        if len(row) > self.row_width:
            raise IntegrityError('Row length cant be greater than %s' % self.row_width)

    def _assert_row_valid(self, row):
        if self._is_deleted_row(row):
            raise ObjectDoesNotExist

    def insert(self, instance):
        row = self.to_db(instance)
        self._assert_data_valid(row)
        self.file.seek(0, os.SEEK_END)
        f = self.file
        f.write(row)
        instance.pk = self._get_pk_from_pos(f.tell() - 1)

    def update(self, instance):
        pk = instance.pk
        row = self.to_db(instance)
        # .update() is allowed to write to deleted rows.
        # so no need to do self._assert_row_valid()
        self._assert_pk_valid(pk)
        self._assert_data_valid(row)
        f = self.file
        pos = self._get_pos_from_pk(pk)
        f.seek(pos, 0)
        return f.write(row)

    def get(self, pk):
        self._assert_pk_valid(pk)

        f = self.file
        pos = self._get_pos_from_pk(pk)
        f.seek(pos, 0)
        row = f.read(self.row_width)
        self._assert_row_valid(row)

        instance = self.from_db(row)
        return instance

    def all(self):
        f = self.file
        f.seek(0)
        pos = 0
        while True:
            row = f.read(self.row_width)
            if not row:
                break
            pos += self.row_width
            if not self._is_deleted_row(row):
                instance = self.from_db(row)
                instance.pk = self._get_pk_from_pos(pos)
                yield instance

    def count(self):
        c = 0
        f = self.file
        f.seek(0)
        while True:
            row = f.read(self.row_width)
            if not row:
                break
            if not self._is_deleted_row(row):
                c += 1
        return c

    def total_count(self):
        f = self.file
        f.seek(0, os.SEEK_END)
        pos = f.tell()
        return self._get_pk_from_pos(pos)

    def delete(self, pk):
        self._assert_pk_valid(pk)
        f = self.file
        pos = self._get_pos_from_pk(pk)
        f.seek(pos, 0)
        null_row = self.FILL_CHAR * self.row_width
        f.write(null_row)
