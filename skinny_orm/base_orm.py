from abc import ABC, abstractmethod


class BaseOrm(ABC):

    @abstractmethod
    def __init__(self, connection, create_tables_if_not_exists=True, parse_fields=True):
        ...

    @abstractmethod
    def select(self, entity) -> 'BaseOrm':
        ...

    @abstractmethod
    def where(self, where_clause) -> 'BaseOrm':
        ...

    @abstractmethod
    def limit(self, row_num: int):
        ...

    @abstractmethod
    def all(self, commit=False):
        ...

    @abstractmethod
    def first(self):
        ...

    @abstractmethod
    def insert(self, instance, commit=True):
        ...

    @abstractmethod
    def bulk_insert(self, instances, commit=True):
        ...

    @abstractmethod
    def update(self, instance, commit=True) -> 'BaseOrm':
        ...

    @abstractmethod
    def bulk_update(self, instances, commit=True):
        ...

    @abstractmethod
    def upsert(self, instance, commit=True):
        ...

    @abstractmethod
    def bulk_upsert(self, instances, commit=True):
        ...

    @abstractmethod
    def delete(self, entity) -> 'BaseOrm':
        ...

    @abstractmethod
    def set(self, set_clause) -> 'BaseOrm':
        ...

    @abstractmethod
    def using(self, *args):
        ...
