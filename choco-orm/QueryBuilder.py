from abc import ABC
from typing import Optional

class QueryBuilder(ABC):
    
    @staticmethod
    def Postgres(
            database: Optional[str] = None,
            host: Optional[str] = None, 
            port: Optional[str] = None,
            user_name: Optional[str] = None, 
            password: Optional[str] = None
        ):
        """
        Creates a new Postgres instance with the provided database connection details.

        Args:
            database (Optional[str]): The name of the database.
            host (Optional[str]): The hostname of the database server.
            port (Optional[str]): The port number to connect to.
            user_name (Optional[str]): The username for authentication.
            password (Optional[str]): The password for authentication.

        Returns:
            A Postgres instance configured with the specified connection details.
        """
        from Postgres import Postgres  # Local import inside the method
        return Postgres(database, host, port, user_name, password)


# class SQL(QueryBuilder):
#     def where(self, field, operator, value, combinator='AND'):
#         print(f"WHERE {field} {operator} {value} {combinator}")
#         return self

#     @abstractmethod
#     def select(self, *fields):
#         pass