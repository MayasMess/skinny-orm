from typing import List


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
        self.and_or_s.append('or')
        return self
