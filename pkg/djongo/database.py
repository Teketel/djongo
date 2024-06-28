from logging import getLogger
from pymongo import MongoClient
from pymongo.errors import InvalidOperation

logger = getLogger(__name__)


class Database:

    def __init__(self) -> None:
        self.clients = {}

    def connect(self, db, **kwargs):
        try:
            conn_client = self.clients[db]
            conn_client.server_info()
        except KeyError:
            logger.debug('New MongoClient connection')
            self.clients[db] = MongoClient(**kwargs, connect=False)
        except InvalidOperation:
            logger.debug('New MongoClient connection')
            self.clients[db] = MongoClient(**kwargs, connect=False)
        return self.clients[db]


class Error(Exception):  # NOQA: StandardError undefined on PY3
    pass


class InterfaceError(Error):
    pass


class DatabaseError(Error):
    pass


class DataError(DatabaseError):
    pass


class OperationalError(DatabaseError):
    pass


class IntegrityError(DatabaseError):
    pass


class InternalError(DatabaseError):
    pass


class ProgrammingError(DatabaseError):
    pass


class NotSupportedError(DatabaseError):
    pass


def Binary(value):
    return value
