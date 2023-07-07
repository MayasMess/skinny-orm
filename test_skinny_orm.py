import unittest
from dataclasses import dataclass
from datetime import datetime
import sqlite3
from typing import List

from skinny_orm.exceptions import ParseError, NotValidComparator, NotValidEntity
from skinny_orm.orm import Orm


@dataclass
class User:
    id: int
    name: str
    age: int
    birth: datetime
    percentage: float

    @property
    def misterify_name(self):
        return "Mr. " + self.name


@dataclass
class Animal:
    id: int
    name: str


@dataclass
class RockBand:
    name: str


connection = sqlite3.connect(':memory:')
orm = Orm(connection)


class TestSkinnyOrm(unittest.TestCase):
    def setUp(self) -> None:
        self.users = [
            User(id=1, name='Naruto', age=13, birth=datetime.now(), percentage=0.99),
            User(id=2, name='Sasuke', age=13, birth=datetime.now(), percentage=0.89),
            User(id=3, name='Sakura', age=13, birth=datetime.now(), percentage=0.79),
            User(id=4, name='Kakashi', age=27, birth=datetime.now(), percentage=0.59),
            User(id=5, name='Obito', age=27, birth=datetime.now(), percentage=0.59),
            User(id=6, name='Itachi', age=18, birth=datetime.now(), percentage=0.59),
            User(id=7, name='Minato', age=35, birth=datetime.now(), percentage=0.59),
            User(id=8, name='Boruto', age=12, birth=datetime.now(), percentage=0.59),
            User(id=9, name='Tsunade', age=50, birth=datetime.now(), percentage=0.59),
            User(id=10, name='Jiraya', age=50, birth=datetime.now(), percentage=0.59),
            User(id=11, name='Oroshimaru', age=50, birth=datetime.now(), percentage=0.59),
        ]
        self.goku = User(id=9001, name='Goku', age=45, birth=datetime.now(), percentage=0.99)
        self.bra = User(id=50, name='Bra', age=3, birth=datetime.now(), percentage=0.99)

    def test_simple_insert(self):
        orm.insert(self.goku)
        goku: User = orm.select(User).where(User.id == 9001).first()
        self.assertEqual(goku.id, 9001)
        self.assertEqual(goku.name, 'Goku')
        self.assertEqual(goku.age, 45)

    def test_bulk_insert(self):
        orm.bulk_insert(self.users)
        users = orm.select(User).all()
        self.assertGreater(len(users), 10)

    def test_simple_select(self):
        orm.bulk_insert(self.users)
        users = orm.select(User).all()
        self.assertEqual(
            orm.current_query,
            'select User.id, User.name, User.age, User.birth, User.percentage from User')
        self.assertIsInstance(users[0], User)

    def test_select_with_limit(self):
        orm.bulk_insert(self.users)
        users = orm.select(User).limit(5)
        self.assertEqual(
            orm.current_query,
            'select User.id, User.name, User.age, User.birth, User.percentage from User limit 5')
        self.assertEqual(len(users), 5)

    def test_select_first(self):
        orm.insert(self.goku)
        user = orm.select(User).first()
        self.assertEqual(
            orm.current_query,
            'select User.id, User.name, User.age, User.birth, User.percentage from User')
        self.assertIsInstance(user, User)

    def test_select_if_fields_are_converted(self):
        orm.insert(self.goku)
        goku: User = orm.select(User).where(User.id == self.goku.id).first()
        self.assertEqual(goku.id, self.goku.id)
        self.assertIsInstance(goku.age, int)
        self.assertIsInstance(goku.name, str)
        self.assertIsInstance(goku.id, int)
        self.assertIsInstance(goku.percentage, float)
        self.assertIsInstance(goku.birth, datetime)

    def test_select_user_not_exist(self):
        user = orm.select(User).where(User.name == "I don't exist").first()
        self.assertIsNone(user)

    def test_select_users_not_exists(self):
        users = orm.select(User).where(User.name == "We do not exist").all()
        self.assertEqual(len(users), 0)

    def test_select_with_where_clause(self):
        orm.insert(self.bra)
        users = orm.select(User).where((User.id > 5) & (User.age < 7)).all()
        self.assertEqual(
            orm.current_query,
            'select User.id, User.name, User.age, User.birth, User.percentage '
            'from User where User.id > ? and User.age < ? ')
        self.assertIsInstance(users[0], User)

    def test_delete(self):
        orm.insert(self.goku)
        orm.delete(User).where(User.id == 9001)
        user = orm.select(User).where(User.id == 9001).first()
        self.assertIsNone(user)

    def test_delete_all(self):
        orm.delete(User).all(commit=True)

    def test_user_with_wrong_type_raise_parse_error(self):
        @dataclass
        class User:
            id: int
            name: str
            age: datetime
            birth: int
            percentage: float

        orm.insert(self.goku)
        with self.assertRaises(ParseError):
            user = orm.select(User).first()

    def test_user_with_wrong_type_ok_if_parse_fields_is_false(self):
        @dataclass
        class User:
            id: int
            name: str
            age: datetime
            birth: int
            percentage: float
        orm = Orm(connection, parse_fields=False)
        orm.insert(self.goku)
        user: User = orm.select(User).first()
        self.assertIsInstance(user.age, int)

    def test_with_table_that_does_not_exists(self):
        orm.insert(Animal(id=1, name='Fluffy'))
        animal = orm.select(Animal).where(Animal.id == 1).first()
        self.assertEqual(animal.name, 'Fluffy')
        connection.execute('drop table Animal')

    def test_with_table_that_does_not_exists_raise_exception(self):
        orm = Orm(connection, create_tables_if_not_exists=False)
        with self.assertRaises(sqlite3.OperationalError):
            orm.insert(RockBand(name='Simple Plan'))

    def test_select_not_existing_table(self):
        animals = orm.select(Animal).all()
        connection.execute('drop table Animal')
        animals = orm.select(Animal).first()
        connection.execute('drop table Animal')

    def test_select_table_that_does_not_exists_raise_exception(self):
        orm = Orm(connection, create_tables_if_not_exists=False)
        with self.assertRaises(sqlite3.OperationalError):
            orm.select(RockBand).all()
        with self.assertRaises(sqlite3.OperationalError):
            orm.select(RockBand).first()

    def test_update_raise_not_valid_comparator(self):
        with self.assertRaises(NotValidComparator):
            orm.update(User).set(User.id > 2)

    def test_update_set_clause_is_ok(self):
        result = orm.update(User).set(User.name == 'Hello World').set(User.percentage == 5.5)
        self.assertEqual(result.current_update_set, 'set name = ? , percentage = ? ')
        self.assertEqual(result.current_params, ['Hello World', 5.5])

    def test_update(self):
        orm.delete(User).all(commit=True)
        orm.bulk_insert(self.users)
        orm.update(User).set(User.name == 'Hello World').set(User.percentage == 5.5).where(User.id < 5)
        users: List[User] = orm.select(User).all()
        for user in users:
            if user.id < 5:
                self.assertEqual(user.name, 'Hello World')
                self.assertEqual(user.percentage, 5.5)
        first_user: User = orm.select(User).where(User.id == 1).first()
        first_user.name = first_user.misterify_name
        orm.update(first_user).using(User.id)
        res = orm.select(User).where(User.name == first_user.name).first()
        self.assertEqual(res.name, 'Mr. Hello World')

    def test_bulk_update(self):
        users = [
            User(id=1, name='Naruto', age=15, birth=datetime.now(), percentage=9.99),
            User(id=2, name='Sasuke', age=15, birth=datetime.now(), percentage=9.89),
            User(id=3, name='Sakura', age=15, birth=datetime.now(), percentage=9.79),
        ]
        orm.delete(User).all(commit=True)
        orm.bulk_insert(self.users)
        orm.bulk_update(users).using(User.id)
        result = orm.select(User).all()
        self.assertEqual(users, result[:3])

    def test_select_none_entity(self):
        with self.assertRaises(NotValidEntity):
            orm.select(None).all()
