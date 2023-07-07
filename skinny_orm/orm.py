from skinny_orm.sqlite_orm import SqliteOrm


class Orm:

    def __new__(cls, connection, create_tables_if_not_exists=True, parse_fields=True):
        if 'sqlite3' in str(connection.__class__):
            return SqliteOrm(connection, create_tables_if_not_exists, parse_fields)
        else:
            raise NotImplementedError
