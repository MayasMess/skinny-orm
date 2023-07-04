import sqlite3
from typing import List
import dateparser
from datetime import datetime


class ParseError(Exception):
    def __init__(self, field_name, field_type):
        msg = f"Impossible to parse {field_name} to {field_type}"
        super(ParseError, self).__init__(msg)


class Comparator:
    def __init__(self, field_name, comparator, other):
        self.field_name = field_name
        self.comparator = comparator
        self.other = other


class BaseField:
    def __init__(self, field_name: str):
        self.field_name = field_name
        self.comparators: List[Comparator] = []
        self.and_or_s = []

    def __eq__(self, other):
        self.comparators.append(Comparator(self.field_name, '=', other))
        return self

    def __gt__(self, other):
        self.comparators.append(Comparator(self.field_name, '>', other))
        return self

    def __ge__(self, other):
        self.comparators.append(Comparator(self.field_name, '>=', other))
        return self

    def __le__(self, other):
        self.comparators.append(Comparator(self.field_name, '<=', other))
        return self

    def __lt__(self, other):
        self.comparators.append(Comparator(self.field_name, '<', other))
        return self

    def __ne__(self, other):
        self.comparators.append(Comparator(self.field_name, '!=', other))
        return self

    def __and__(self, other):
        self.comparators.extend(other.comparators)
        self.and_or_s.append('and')
        return self

    def __or__(self, other):
        self.comparators.extend(other.comparators)
        self.and_or_s.append('or')
        return self


class Orm:
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
        self.is_delete_query = False
        self.parse_fields = parse_fields
        self.create_tables_if_not_exists = create_tables_if_not_exists

    def select(self, entity) -> 'Orm':
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
        self.current_query += f" {self.current_where}"
        if self.is_delete_query:
            return self._final_delete()
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

    def delete(self, entity):
        self._re_init()
        self.current_entity = entity
        self.is_delete_query = True
        self.current_query = f"delete from {entity.__name__}"
        self._create_class_fields(entity)
        return self

    def _final_delete(self):
        self.is_delete_query = False
        cursor = self.connection.cursor()
        try:
            cursor.execute(self.current_query, self.current_params)
            self.connection.commit()
            cursor.close()
        except Exception as e:
            cursor.close()
            raise Exception(f'Woups! => {e}')

    @staticmethod
    def _generate_select_query(entity) -> str:
        """
        Generate query like 'select User.id, User.name, User.age, User.birth, User.percentage from User;'
        :param entity:
        :return:
        """
        class_name = entity.__name__
        all_fields_query_string_format = ', '.join([f"{class_name}.{field_name}"
                                                    for field_name in entity.__dataclass_fields__])
        query = f"select {all_fields_query_string_format} from {class_name}"
        return query

    @staticmethod
    def _generate_insert_query(instance) -> str:
        fields = []
        question_marks = []
        for field_name, field_inst in instance.__dataclass_fields__.items():
            fields.append(field_name)
            question_marks.append('?')
        joined_fields = ', '.join(fields)
        joined_question_marks = ', '.join(question_marks)
        query = f"INSERT INTO {instance.__class__.__name__} ({joined_fields}) VALUES ({joined_question_marks})"
        return query

    @staticmethod
    def _get_current_params_for_instance(instance) -> list:
        cur_params = []
        for field_name, field_inst in instance.__dataclass_fields__.items():
            cur_params.append(getattr(instance, field_name))
        return cur_params

    @staticmethod
    def _create_class_fields(entity):
        for field_name, field in entity.__dataclass_fields__.items():
            setattr(entity, field_name, BaseField(field_name))

    def _parse_and_get_new_tuple(self, tuple_obj: tuple) -> tuple:
        if not self.parse_fields:
            return tuple_obj
        res = []
        for index, (field_name, field) in enumerate(self.current_entity.__dataclass_fields__.items()):
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
        self.current_params = []
        self.current_where = 'where '

    def _create_table(self, entity, cursor):
        params = ', '.join([f"{field_name}   {self.PYTHON_TYPES_TO_SQLITE_MAPPING[field.type]}"
                            for field_name, field in entity.__dataclass_fields__.items()])
        q = f"""CREATE TABLE "{entity.__name__}"({params});"""
        cursor.execute(q)
