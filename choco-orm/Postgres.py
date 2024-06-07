from QuerySql import QuerySql
from typing import (
    Any,
    List,
    Optional,
    Union,
    Dict,
    Tuple,
    Callable
)

import psycopg2
from psycopg2 import sql

class Postgres(QuerySql):
    def __init__(
            self,
            database: Optional[str],
            host: Optional[str], 
            port: Union[str, int],
            user_name: Optional[str], 
            password: Optional[str]
        ):
        super().__init__()  # inicializa atributos conforme definido em SqlQuery
        self.reset()
        self.has_schema = True
        self.DB_POSTGRES_DATABASE = database
        self.DB_POSTGRES_HOST = host
        self.DB_POSTGRES_PORT = port
        self.DB_POSTGRES_USERNAME = user_name
        self.DB_POSTGRES_PASSWORD = password

    def open_connection(self):
        if not hasattr(self, 'conn') or self.conn.closed:
            self.conn = psycopg2.connect(
                dbname=self.DB_POSTGRES_DATABASE,
                user=self.DB_POSTGRES_USERNAME,
                password=self.DB_POSTGRES_PASSWORD,
                host=self.DB_POSTGRES_HOST,
                port=self.DB_POSTGRES_PORT,
            )

    def close_connection(self):
        if hasattr(self, 'conn'):
            self.conn.close()

    def __reset_query_parameters(self):
        self.from_schema_name = []
        self.with_query = []
        self.selected_fields = []
        self.from_table_name = []
        self.joins_conditions = []
        self.where_conditions = []
        self.group_by_conditions = []
        self.having_conditions = []
        self.order_by_conditions = []
        self.limit_value: int = None
        self.offset_value: int = None

        self._is_in_subquery: bool = False
        self.subquery_depth = 0 # LEVEL DA SUBQUERY
        self._current_attribute = None
        self._is_start_subquery: bool = False
        
        self._show_query: bool = False
        self._return_query: bool = False
        self.query: str = None
        return self
    


    def print_query(self) -> 'Postgres':
        self._show_query = True
        return self
    
    def get_query(self) -> 'Postgres':
        self._return_query = True
        return self
   
    def from_schema(self, schema_name) -> 'Postgres':
        schema_name = self.to_postgres_field(schema_name)
        self.from_schema_name = schema_name
        return self

    # def select(self, fields: List[str]) -> 'Postgres':
    #     self.selected_fields = fields
    #     self.query_historic.append('select')
    #     return self
    
    def format_value(self, value) -> 'Postgres':
        if value is None:
            return "NULL"
        elif isinstance(value, bool):
            return 'TRUE' if value else 'FALSE'
        elif isinstance(value, int):
            return str(value)
        elif isinstance(value, str):
            return f"'{value}'"
        elif isinstance(value, (list, tuple)):
            return value
        elif isinstance(value, dict):
            return value
        else:
            raise ValueError(f"Unsupported value type: {type(value)}")



    def insert_many(self, fields: List[str], *values: Tuple[Any, ...]) -> 'Postgres':
        """
        Constrói e exibe uma consulta SQL para inserção múltipla.

        Args:
            fields (List[str]): Lista de nomes de colunas.
            values (Tuple[Any, ...]): Sequência de tuplas, onde cada tupla contém valores correspondentes às colunas.

        Returns:
            None: A função imprime a consulta SQL construída.
        """
        # Montar a string de colunas
        fields_str = ', '.join(fields)
        
        # Montar a string de placeholders para os valores
        placeholders_str = ', '.join(['%s'] * len(fields))

        # Criar a consulta SQL completa
        query = f"INSERT INTO {self.table_name} ({fields_str}) VALUES ({placeholders_str});"

        # Imprimir cada consulta com os valores reais para demonstração
        print("Generated SQL Query:")
        for value in values:
            # Substituir placeholders por valores reais apenas para mostrar como ficaria a consulta
            filled_query = query.replace("%s", "{}").format(*value)
            print(filled_query)


    # INFO FUNCTIONS
    def get_schemas(self) -> 'Postgres':
        self.open_connection()
        query = sql.SQL("""
            SELECT schema_name 
            FROM information_schema.schemata
            ORDER BY schema_name;
        """)
        
        with self.conn.cursor() as cursor:
            cursor.execute(query)
            schemas = cursor.fetchall()
        self.close_connection()
        return [schema[0] for schema in schemas]


    def get_tables(self, schema: Optional[str] = None) -> List[str]:
        """
        Recupera uma lista de nomes de tabelas do esquema especificado.

        :param schema: O esquema do qual recuperar os nomes das tabelas. Se nenhum esquema for especificado,
                    será usado o esquema atualmente definido para a instância da classe.
        :return: Uma lista de strings, onde cada string é o nome de uma tabela no esquema especificado.
        """
        self.open_connection()
        schema = schema if schema else self.schema_name
        query = sql.SQL("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = %s
            ORDER BY table_name;
        """)
        
        with self.conn.cursor() as cursor:
            cursor.execute(query, (schema,))
            tables = cursor.fetchall()
        
        self.close_connection()

        return [table[0] for table in tables]


    def get_info_table(self, table_name: str) -> Dict[str, str]:
        """
        Retorna um dicionário com os nomes das colunas e seus tipos detalhados para uma tabela especificada.
        
        :param table_name: O nome da tabela para a qual recuperar os metadados.
        :return: Um dicionário onde cada chave é o nome de uma coluna e o valor é o tipo de dado detalhado da coluna.
        """
        self.open_connection()
        
        query = sql.SQL("""
            SELECT column_name, udt_name, character_maximum_length, numeric_precision, numeric_scale, datetime_precision, interval_type 
            FROM information_schema.columns 
            WHERE table_name = %s
        """)
        
        column_info = {}
        with self.conn.cursor() as cursor:
            cursor.execute(query, (table_name,))
            for column, udt_name, char_len, num_prec, num_scale, datetime_prec, interval_type in cursor.fetchall():
                data_type = udt_name
                if char_len:
                    data_type += f'({char_len})'
                elif num_prec and num_scale is not None:
                    data_type += f'({num_prec},{num_scale})'
                elif datetime_prec:
                    data_type += f'({datetime_prec})'
                elif interval_type:
                    data_type += f' {interval_type}'

                column_info[column] = data_type
        
        self.close_connection()
        return column_info
    
    # def select(self, fields: List[str], *lambda_functions: Optional[Callable[[], None]]) -> 'Postgres':      
    #     if not self._current_attribute_name:
    #         self._current_attribute_name = 'selected_fields'

    #     current_query = getattr(self, self._current_attribute_name)
    #     current_query.extend(fields)

    #     # if callable(lambda_function):
    #     #         lambda_function()
    #     # print(len(*lambda_functions))
    #     # self.subquery(*lambda_functions)
    #     # current_query.extend('(')
    #     for lambda_function in lambda_functions:
    #         if callable(lambda_function):
    #             lambda_function()
    #     # current_query.extend(')')
        
    #     self._is_start_subquery = False
    #     self._is_end_subquery = False
    #     # name = self._current_attribute_name
    #     # print(self._current_attribute_name)
    #     # print(hasattr(self, 'selected_fields')) # verifica se existe
    #     # setattr(self, 'selected_fields', ['idade'])
    #     # print(getattr(self, 'selected_fields')) # faz referencia ao atributo
    #     # hasattr(p, 'nome');
    #     # getattr(p, 'nome');
    #     # setattr(p, 'idade', 31)
        
        
    #     self._current_attribute_name = None
    #     return self

    # def where(self, field: str, operator: str, value: Any, combinator: str = 'AND'):
    #     if not self._current_attribute_name:
    #         self._current_attribute_name = 'where_conditions'
        
    #     current_query = getattr(self, self._current_attribute_name)

    #     print(self._is_end_subquery)

    #     if self._is_start_subquery:
    #         if len(current_query) > 0 and self._subquery_prefix:
    #             current_query.append(self._subquery_prefix)    
    #         current_query.extend('(')

    #     if len(current_query) == 0 or self._is_start_subquery:
    #         current_query.extend([field, operator, value])
    #     else:
    #         current_query.extend([combinator, field, operator, value])

    #     if self._is_end_subquery:
    #         current_query.extend(')')

    #     self._current_attribute_name = None
    #     return self

    # def where(self, field: str, operator: str, value: Any, combinator: str = 'AND'):
    #     print(" ")
    #     print(f"where: {field}, {operator}, {value}")
    #     print(f"start subquery: {self._is_start_subquery}")
    #     print(f"end subquery: {self._is_end_subquery}")
    #     print(" ")

    #     if self._is_start_subquery:
    #         if len(self.where_conditions) > 0 and self._subquery_prefix:
    #             self.where_conditions.append(self._subquery_prefix)    
            
    #         self.where_conditions.extend('(')

    #     if len(self.where_conditions) == 0 or self._is_start_subquery:
    #         self.where_conditions.extend([field, operator, value])
    #     else:
    #         self.where_conditions.extend([combinator, field, operator, value])

    #     if self._is_end_subquery:
    #         self.where_conditions.extend(')')

    #     return self
    
    # def subquery(self, *lambda_functions: Callable[[], None], combinator: Optional[str] = None):
    #     print('subquery')

    #     for index, lambda_function in enumerate(lambda_functions):
    #         print(index)
    #         if index == 0:
    #             self._is_start_subquery = True
    #             self._subquery_prefix = combinator
    #         else:
    #             self._is_start_subquery = False
    #             self._subquery_prefix = None

    #         if (len(lambda_functions) == 1 or index == (len(lambda_functions) - 1)):
    #             self._is_end_subquery = True
    #         else:
    #             self._is_end_subquery = False

    #         self.index = index
    #         if callable(lambda_function):
    #             lambda_function()

    #     return self

    def call_subquery(func):
        def wrapper(self, *args, **kwargs):
            print('antes')
            result = func(self, *args, **kwargs)
            print('depois')

            return result
        return wrapper

    def before_and_after(func):
        def wrapper(self, *args, **kwargs):
            
            # print("Antes da execução de", func.__name__) # nome da função
            result = func(self, *args, **kwargs)  # Executa a função original com seus argumentos
            # self.where_conditions.extend(')')
            # print("Depois da execução de", func.__name__)
            return result
        return wrapper
    

    def to_postgres_select_field(self, fields):
        new_fileds = []
        for index, field in enumerate(fields):
            if index == (len(fields) - 1):
                new_fileds.append(field)
            else:
                new_fileds.extend([field, ','])
        return new_fileds
    
    def to_postgres_field(self, field):  
        return f'{field}'
    
    def to_postgres_value(self, value):
        if isinstance(value, str):
            return f"'{value}'"
        else:
            return value
        
    def to_postgres_values(self, values: List):
        new_values = []

        for value in values:
            new_values.append(self.to_postgres_value(value))

        string = '('
        string += ", ".join(map(str, new_values))
        string += ')'

        return string        

    # @before_and_after
    def where(self, field: str, operator: str, value: Any, combinator: str = 'AND'):       
        field = self.to_postgres_field(field)

        if isinstance(value, list):
            value = self.to_postgres_values(value)
        else:
            value = self.to_postgres_value(value)
        
        if self._is_in_subquery:
            if self._is_start_subquery:
                self.subquery_args.extend([field, operator, value])  
            else:
                if "WHERE" in self.subquery_args:
                    self.subquery_args.extend([combinator, field, operator, value])
                else:
                    self.subquery_args.extend(['WHERE', field, operator, value])

        else:
            if len(self.where_conditions) == 0:
                self.where_conditions.extend(['WHERE', field, operator, value])  
            else:
                self.where_conditions.extend([combinator, field, operator, value])
        return self
    
    def where_or(self, field: str, operator: str, value: Any):
        self.where(field, operator, value, 'OR')
        return self
    
    def where_in(self, field: str, values: List, combinator: str = 'AND'):
        if len(values) > 0:
            self.where(field, 'IN', values, combinator)

        return self
    
    def select(self, fields: List[str] = '*') -> 'Postgres':
        selected_fields = []

        if len(fields) == 0 or (len(fields) == 1 and fields[0] == '*'):
            selected_fields = ['*']

        if len(fields) == 1:
            selected_fields = fields
        else:
            for index, field in enumerate(fields):
                if index == (len(fields) - 1):
                    selected_fields.append(field)
                else:
                    selected_fields.extend([field, ','])

        if self._is_in_subquery:
            self.subquery_args.append('SELECT')
            self.subquery_args.extend(selected_fields)
        else:
            self.selected_fields.append('SELECT')
            self.selected_fields.extend(selected_fields)
        return self
    
    def from_table(self, table_name: str) -> 'Postgres':
        if not self.from_schema_name:
            self.from_schema_name = 'public'

        if '.' in table_name:
            full_table_name = table_name
        else:
            full_table_name = f'{self.from_schema_name}.{table_name}'

        if self._is_in_subquery:
            self.subquery_args.extend(['FROM', full_table_name])
        else:
            self.from_table_name.extend(['FROM', full_table_name])
        return self
    
    def where_subquery(self, field: str, operator: Optional[str], *lambda_functions: Callable[[], None], combinator: Optional[str] = 'AND'):
        self._current_attribute = self.where_conditions
    
        if len(self.where_conditions) == 0:
            self.where_conditions.extend(['WHERE', field, operator])
        else:
            self.where_conditions.extend([combinator, field, operator])       

        self.subquery(*lambda_functions);
        return self
    

    
    # @call_subquery
    def subquery(self, *lambda_functions, combinator: Optional[str] = None):
        if combinator and (combinator in self._attributes_by_function):
            self._current_attribute = getattr(self, self._attributes_by_function[combinator])
        # print(combinator)
        # else:
        #     self._current_attribute = getattr(self, self._attributes_by_function['SUBQUERY'])

        has_previous_argument = True if self.subquery_args and self.subquery_args[-1] != '(' else False
        has_arguments = True if len(lambda_functions) > 0 else False
        is_first_argument = True if self.subquery_depth == 0 else False
        
        if combinator and has_arguments and (is_first_argument or has_previous_argument):
            self.subquery_args.append(combinator)

        self.subquery_depth += 1

        if has_arguments:
            self.subquery_args.extend('(')
        
            for index, lambda_function in enumerate(lambda_functions):
                if index == 0:
                    self._is_start_subquery = True
                else:
                    self._is_start_subquery = False

                self._is_in_subquery = True

                if callable(lambda_function):
                    lambda_function()
                else:
                    self.subquery_args.append(lambda_function)

            self.subquery_args.extend(')')

        self.subquery_depth -= 1
        self._is_in_subquery = False

        # if self._current_attribute and self.subquery_depth == 0:
        # self.where_conditions.extend(self.subquery_args)
        # print(self.where_conditions)
        # print(self._current_attribute)
        # print(self.subquery_args)

        if self.subquery_depth == 0:
            self._current_attribute.extend(self.subquery_args)
            self.subquery_args = []

        return self
    
    def __builder_query_select(self):
        query_select = ", ".join(map(str, self.selected_fields))
        return query_select


    def get(self) -> 'Postgres':
        query_parts = []

        # print(self.__builder_query_select())
        # if self.with_query:
        #     query_parts.append("WITH " + ", ".join(self.with_query))

        if self.selected_fields:
            query_parts.extend(self.selected_fields)
        else:
            query_parts.append("SELECT *")

        if self.from_table_name:

            query_parts.extend(self.from_table_name)

        # if self.joins_conditions:
        #     query_parts.extend(self.joins_conditions)

        if self.where_conditions:
            query_parts.extend(self.where_conditions)

        # if self.group_by_conditions:
        #     query_parts.append("GROUP BY " + ", ".join(self.group_by_conditions))

        # if self.having_conditions:
        #     query_parts.append("HAVING " + " AND ".join(self.having_conditions))

        # if self.order_by_conditions:
        #     query_parts.append("ORDER BY " + ", ".join(self.order_by_conditions))

        # if self.limit_value is not None:
        #     query_parts.append("LIMIT " + str(self.limit_value))

        # if self.offset_value is not None:
        #     query_parts.append("OFFSET " + str(self.offset_value))
        query = " ".join(map(str, query_parts))
        query += ';'

        # self.query = " ".join(map(str, query_parts))
        # self.query += ';'

        if self._return_query == True:
            self.__reset_query_parameters()
            return query
        
        if self._show_query == True:
            print(query)

        if self._rows_only:
            return self.results_only_rows(query)
        elif self._rows_with_headers:
            return self.results_with_headers(query)
        else:
            return self.results_with_dictionary(query)


    # RESULT METHODOS
    def results_only_rows(self, query):
        self.open_connection()

        with self.conn.cursor() as cursor:
            cursor.execute(query)
            results = cursor.fetchall()

        self.close_connection()

        self.__reset_query_parameters()

        return results
    
    def results_with_headers(self, query):
        self.open_connection()

        results = []

        with self.conn.cursor() as cursor:
            cursor.execute(query)
            column_names = tuple(desc[0] for desc in cursor.description)
            results.append(column_names)
            rows = cursor.fetchall()
            results.extend(rows)

        self.close_connection()

        self.__reset_query_parameters()

        return results

    def results_with_dictionary(self, query):
        self.open_connection()

        results = []

        with self.conn.cursor() as cursor:
            cursor.execute(query)
            column_names = [desc[0] for desc in cursor.description]
            rows = cursor.fetchall()
            for row in rows:
                results.append(dict(zip(column_names, row)))

        self.close_connection()
        self.__reset_query_parameters()

        return results

    def rows_are_dictionary(self):
        self._rows_are_dictionary = True
        self._rows_have_headers = False
        self._rows_only = False
        return self

    def rows_with_headers(self):
        self._rows_with_headers = True
        self._rows_are_dictionary = False
        self._rows_only = False
        return self
    
    def rows_only(self):
        self._rows_only = True
        self._rows_with_headers = False
        self._rows_are_dictionary = False
        return self
