import os

from babu.utils import chunker

FILL_CHAR = '\000'


class ObjectDoesNotExist(Exception):
    pass


class IntegrityError(Exception):
    pass


class BaseConnection(object):
    def __init__(self, db_name, row_width):
        self.db_name = db_name
        self.row_width = row_width

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


class FileConnection(BaseConnection):
    def __init__(self, db_name, row_width):
        super(FileConnection, self).__init__(db_name, row_width)
        db_name = db_name + '.db'
        try:
            self.file = open(db_name, 'r+b')
        except IOError:
            f = open(db_name, 'w')
            f.close()
            self.file = open(db_name, 'r+b')

    def _get_pos_from_pk(self, pk):
        return pk * self.row_width

    def _get_pk_from_pos(self, pos):
        return pos / self.row_width

    def _is_deleted_row(self, row):
        return row == FILL_CHAR * self.row_width

    def _assert_pk_valid(self, pk):
        # need this bcs seek()-ing to a non-existent offset doesnt
        # raise errors 
        if pk >= self.count():
            raise ObjectDoesNotExist

    def _assert_data_valid(self, row):
        if len(row) > self.row_width:
            raise IntegrityError('Row length cant be greater than %s' % self.row_width)

    def _assert_row_valid(self, row):
        if self._is_deleted_row(row):
            raise ObjectDoesNotExist

    def insert(self, row):
        self._assert_data_valid(row)
        self.file.seek(0, os.SEEK_END)
        f = self.file
        f.write(row)
        return self._get_pk_from_pos(f.tell() - 1)

    def update(self, pk, row):
        # .update() is allowed to write to deleted rows.
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
        return row

    def fetch_all(self):
        f = self.file
        f.seek(0)
        rows = f.read()
        for pos, row in chunker(rows, self.row_width):
            if not self._is_deleted_row(row):
                yield self._get_pk_from_pos(pos), row

    def count(self):
        f = self.file
        f.seek(0, os.SEEK_END)
        pos = f.tell()
        return self._get_pk_from_pos(pos)

    def delete(self, pk):
        self._assert_pk_valid(pk)
        row = FILL_CHAR * self.row_width
        self.update(pk, row)
