import sqlite3
import dateparser
from datetime import datetime

from skinny_orm.base_field import BaseField
from skinny_orm.base_orm import BaseOrm
from skinny_orm.exceptions import ParseError, NotValidComparator, NotValidEntity


class SqliteOrm(BaseOrm):
    PYTHON_TYPES_TO_SQLITE_MAPPING = {
        int: 'INTEGER',
        str: 'TEXT',
        float: 'REAL',
        datetime: 'TEXT',
    }

    def __init__(self, connection, create_tables_if_not_exists=True, parse_fields=True):
        self.connection = connection
        self.current_query = None
        self.current_entity = None
        self.current_params = []
        self.current_where = 'where '
        self.current_update_set = 'set '
        self.is_delete_query = False
        self.is_update_query = False
        self.is_bulk_update_query = False
        self.parse_fields = parse_fields
        self.create_tables_if_not_exists = create_tables_if_not_exists
        self.update_instances = []

    def select(self, entity) -> BaseOrm:
        self._re_init()
        self.current_entity = entity
        self.current_query = self._generate_select_query(entity)
        self._create_class_fields(entity)
        return self

    def where(self, where_clause):
        base_field: BaseField = where_clause
        base_field.and_or_s = iter(base_field.and_or_s)
        for comp in base_field.comparators:
            self.current_params.append(comp.other)
            self.current_where += f"{self.current_entity.__name__}.{comp.field_name} {comp.comparator} ? "
            and_or = next(base_field.and_or_s, None)
            if and_or is not None:
                self.current_where += f"{and_or} "

        if self.is_update_query:
            self.current_query += f" {self.current_update_set}"

        self.current_query += f" {self.current_where}"

        if self.is_delete_query or self.is_update_query:
            return self._final()
        return self

    def limit(self, row_num: int):
        row_num = int(row_num)
        self.current_query = self.current_query + f" limit {row_num}"
        return self.all()

    def all(self, commit=False) -> list:
        self.is_delete_query = False
        cursor = self.connection.cursor()
        try:
            res = cursor.execute(self.current_query, tuple(self.current_params)).fetchall()
            cursor.close()
            if commit:
                self.connection.commit()
            return [self.current_entity(*self._parse_and_get_new_tuple(r)) for r in res]
        except sqlite3.OperationalError as e:
            if self.create_tables_if_not_exists and 'no such table' in str(e):
                self._create_table(self.current_entity, cursor)
                return self.all(commit)
            else:
                raise e
        except Exception as e:
            cursor.close()
            raise e

    def first(self):
        cursor = self.connection.cursor()
        try:
            res = cursor.execute(self.current_query, tuple(self.current_params)).fetchone()
            if res is None:
                return None
            cursor.close()
            return self.current_entity(*self._parse_and_get_new_tuple(res))
        except sqlite3.OperationalError as e:
            if self.create_tables_if_not_exists and 'no such table' in str(e):
                self._create_table(self.current_entity, cursor)
                self.first()
            else:
                raise e
        except Exception as e:
            cursor.close()
            raise e

    def insert(self, instance, commit=True):
        self._re_init()
        self.current_query = self._generate_insert_query(instance)
        self.current_params.extend(self._get_current_params_for_instance(instance))
        cursor = self.connection.cursor()
        try:
            cursor.execute(self.current_query, tuple(self.current_params))
            if commit:
                self.connection.commit()
            cursor.close()
        except sqlite3.OperationalError as e:
            if self.create_tables_if_not_exists and 'no such table' in str(e):
                self._create_table(instance.__class__, cursor)
                self.insert(instance, commit)
            else:
                raise e
        except Exception as e:
            cursor.close()
            raise e

    def bulk_insert(self, instances, commit=True):
        self._re_init()
        if len(instances) == 0:
            return

        self.current_query = self._generate_insert_query(instances[0])
        for inst in instances:
            self.current_params.append(self._get_current_params_for_instance(inst))
        cursor = self.connection.cursor()
        try:
            cursor.executemany(self.current_query, self.current_params)
            if commit:
                self.connection.commit()
            cursor.close()
        except sqlite3.OperationalError as e:
            if self.create_tables_if_not_exists and 'no such table' in str(e):
                self._create_table(instances[0].__class__, cursor)
                self.bulk_insert(instances, commit)
            else:
                raise e
        except Exception as e:
            cursor.close()
            raise e

    def update(self, entity_or_instance, commit=True) -> 'SqliteOrm':
        self._re_init()
        if isinstance(entity_or_instance, type):
            self.current_entity = entity_or_instance
        else:
            self.current_entity = entity_or_instance.__class__
            self.update_instances.append(entity_or_instance)
        self.current_query = f"update {self.current_entity.__name__} "
        self._create_class_fields(self.current_entity)
        self.is_update_query = True
        return self

    def bulk_update(self, instances, commit=True):
        self._re_init()
        self.is_bulk_update_query = True
        self.current_entity = instances[0].__class__
        self.update_instances = instances
        self.current_query = f"update {self.current_entity.__name__} "
        self._create_class_fields(self.current_entity)
        self.is_update_query = True
        return self

    def upsert(self, instance, commit=True):
        raise NotImplementedError

    def bulk_upsert(self, instances, commit=True):
        raise NotImplementedError

    def set(self, set_clause) -> 'SqliteOrm':
        comparator = set_clause.comparators[0].comparator
        if comparator != '=':
            raise NotValidComparator
        add_comma = ', '
        if self.current_update_set == 'set ':
            add_comma = ''
        self.current_update_set += f"{add_comma}{set_clause.field_name} {comparator} ? "
        self.current_params.append(set_clause.comparators[0].other)
        return self

    def delete(self, entity):
        self._re_init()
        self.current_entity = entity
        self.is_delete_query = True
        self.current_query = f"delete from {entity.__name__}"
        self._create_class_fields(entity)
        return self

    def _final(self, bulk=False):
        self.is_delete_query = False
        self.is_update_query = False
        cursor = self.connection.cursor()
        try:
            if bulk:
                cursor.executemany(self.current_query, self.current_params)
            else:
                cursor.execute(self.current_query, self.current_params)
            self.connection.commit()
            cursor.close()
        except Exception as e:
            cursor.close()
            raise Exception(f'Woups! => {e}')

    def using(self, *args):
        self.current_update_set += ', '.join([f"{field} = ?" for field in self._dataclass_fields(self.current_entity)])
        self.current_where += ', '.join([f"{arg.field_name} = ?" for arg in args])
        self.current_query += f" {self.current_update_set} {self.current_where}"
        for inst in self.update_instances:
            self.current_params.append(
                [getattr(inst, field_name) for field_name in self._dataclass_fields(self.current_entity)] + \
                [getattr(inst, arg.field_name) for arg in args])

        if self.is_bulk_update_query is False:
            self.current_params = self.current_params[0]
        self._final(bulk=self.is_bulk_update_query)

    def _generate_select_query(self, entity) -> str:
        """
        Generate query like 'select User.id, User.name, User.age, User.birth, User.percentage from User;'
        :param entity:
        :return:
        """
        try:
            class_name = entity.__name__
            all_fields_query_string_format = ', '.join([f"{class_name}.{field_name}"
                                                        for field_name in self._dataclass_fields(entity)])
            query = f"select {all_fields_query_string_format} from {class_name}"
            return query
        except AttributeError:
            raise NotValidEntity(entity)

    def _generate_insert_query(self, instance) -> str:
        fields = []
        question_marks = []
        for field_name, field_inst in self._dataclass_fields(instance).items():
            fields.append(field_name)
            question_marks.append('?')
        joined_fields = ', '.join(fields)
        joined_question_marks = ', '.join(question_marks)
        query = f"INSERT INTO {instance.__class__.__name__} ({joined_fields}) VALUES ({joined_question_marks})"
        return query

    def _get_current_params_for_instance(self, instance) -> list:
        cur_params = []
        for field_name, field_inst in self._dataclass_fields(instance).items():
            cur_params.append(getattr(instance, field_name))
        return cur_params

    def _create_class_fields(self, entity):
        for field_name, field in self._dataclass_fields(entity).items():
            setattr(entity, field_name, BaseField(field_name))

    def _parse_and_get_new_tuple(self, tuple_obj: tuple) -> tuple:
        if not self.parse_fields:
            return tuple_obj
        res = []
        for index, (field_name, field) in enumerate(self._dataclass_fields(self.current_entity).items()):
            try:
                if field.type == datetime:
                    res.append(dateparser.parse(tuple_obj[index]))
                    continue
                res.append(field.type(tuple_obj[index]))
            except (TypeError, ValueError):
                raise ParseError(field_name=field_name, field_type=field.type)
        return tuple(res)

    def _re_init(self):
        self.current_query = None
        self.current_entity = None
        self.current_params = []
        self.current_where = 'where '
        self.current_update_set = 'set '
        self.is_delete_query = False
        self.is_update_query = False
        self.is_bulk_update_query = False
        self.update_instances = []

    def _create_table(self, entity, cursor):
        params = ', '.join([f"{field_name}   {self.PYTHON_TYPES_TO_SQLITE_MAPPING[field.type]}"
                            for field_name, field in self._dataclass_fields(entity).items()])
        q = f"""CREATE TABLE "{entity.__name__}"({params});"""
        cursor.execute(q)

    @staticmethod
    def _dataclass_fields(entity_or_instance):
        return {key: val for key, val in entity_or_instance.__dataclass_fields__.items()}
