class ParseError(Exception):
    def __init__(self, field_name, field_type):
        msg = f"Impossible to parse {field_name} to {field_type}"
        super(ParseError, self).__init__(msg)


class NotValidComparator(Exception):
    def __init__(self):
        msg = f"The comparator used in the 'set' clause is not valid"
        super(NotValidComparator, self).__init__(msg)


class NotValidEntity(Exception):
    def __init__(self, not_valid_entity):
        msg = f"'{not_valid_entity}' is not a valid Entity to select from"
        super(NotValidEntity, self).__init__(msg)
