from abc import ABC, abstractmethod
from typing import Any, List, Union, Tuple, Callable, Optional

from QueryBuilder import QueryBuilder

class QuerySql(QueryBuilder):
    def __init__(self):
        
        self.database_source = None
        self.has_schema: bool = False

        # self.schema_name: str = None
        self.from_schema_name = []
        self.with_query = []
        self.selected_fields = []
        self.from_table_name = []
        self.joins_conditions = []
        self.where_conditions = ['2']
        self.group_by_conditions = []
        self.having_conditions = []
        self.order_by_conditions = []
        self.limit_value: int = None
        self.offset_value: int = None

        self._show_query: bool = False
        self._return_query: bool = False

        self.query: str = None

        # SUBQUERY
        self._current_attribute = None
        self._has_a_subquery: bool = False
        self._is_in_subquery: bool = False
        self.subquery_depth = 0 # LEVEL DA SUBQUERY
        self._is_start_subquery: bool = False
        self.subquery_args = []
        self.current_subquery = []

        self.is_anonymous_subquery: bool = False

        # Results type
        self._rows_only = False
        self._rows_with_headers = False
        self._rows_are_dictionary = False

        self._attributes_by_function = {
            'WHERE': 'where_conditions',
            'SELECT': 'selected_fields',
            'WITH': 'with_query',
            'SUBQUERY': 'current_subquery'
        }

    def reset(self):
        self.database_source = None
        self.has_schema: bool = False
        self.schema_name: str = None
        self.with_query = []
        self.selected_fields = []
        self.table_name: str = None
        self.joins_conditions = []
        self.where_conditions = []
        self.group_by_conditions = []
        self.having_conditions = []
        self.order_by_conditions = []
        self.limit_value: int = None
        self.offset_value: int = None
        self._is_start_subquery: bool = False
        self._is_end_subquery: bool = False
        self._current_attribute = ''
 

        self.query: str = None


    @abstractmethod
    def format_value(self, value: Any):
        """
        Converts python variable values to the standard format 
        depending on the type of database.

        Exemple:
        For Postgres
        format_value(None), results in 'NULL'
        format_value(True), results in 'TRUE'
        """
        pass

    def from_table(self, table_name):
        self.table_name = table_name
        return self
    
    def limit(self, limit_value: int):
        self.limit_value = limit_value
        return self
    
    def offset(self, offset_value: int):
        self.offset_value = offset_value
        return self
    


    # BUILD QUERIES
    # Auxiliary functions
    def build_where_conditions(self) -> str:
        """
        Builds conditions in the format supported by the database.
        """
        where_clauses = []

        for condition in self.conditions:
            for index_where, where in enumerate(condition['wheres']):

                if (len(where_clauses) != 0 and condition['is_group'] == True and index_where == 0):
                    where_clauses.append(f"{condition['boolean']}")

                if (condition['is_group'] == True and index_where == 0):
                    where_clauses.append('(')

                if (len(where_clauses) != 0 and condition['is_group'] == False):
                    where_clauses.append(f"{where['boolean']}")

                if (len(where_clauses) != 0 and index_where != 0):
                    where_clauses.append(f"{where['boolean']}")

                where_clauses.append(f"'{where['field']}' {where['operator']} {self.format_value(where['value'])}")

                if (condition['is_group'] == True and index_where == (len(condition['wheres']) - 1)):
                    where_clauses.append(')')

        string_unica = " ".join(where_clauses)
        return 'WHERE ' + string_unica
    


    @abstractmethod
    def select(self, fields: List[str]) -> 'QueryBuilder':
        pass

    # # ACTION METHODS
    # @abstractmethod
    # def get(self) -> 'QueryBuilder':
    #     pass

    # @abstractmethod
    # def first(self) -> 'QueryBuilder':
    #     pass

    @abstractmethod
    def insert_many (self, *args: Any) -> 'QueryBuilder':
        pass

    # @abstractmethod
    # def update (self) -> 'QueryBuilder':
    #     pass

    # @abstractmethod
    # def delete (self) -> 'QueryBuilder':
    #     pass

    # Temporários
    def print_conditions(self):
        import json
        print(json.dumps(self.conditions, indent=4))

    
# # print(teste2)