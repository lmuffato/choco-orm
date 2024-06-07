import os
from abc import ABC, abstractmethod
from dotenv import load_dotenv

# Carrega as variáveis de ambiente do arquivo .env na raiz do projeto
load_dotenv()

class DatabaseConfig(ABC):
    @abstractmethod
    def get_config(self):
        pass

class MongoDBConfig(DatabaseConfig):
    def get_config(self):
        return {
            'database': os.getenv('DB_MONGO_DATABASE'),
            'host': os.getenv('DB_MONGO_HOST'),
            'port': os.getenv('DB_MONGO_PORT'),
            'user_name': os.getenv('DB_MONGO_USERNAME'),
            'password': os.getenv('DB_MONGO_PASSWORD')
        }

class MySQLConfig(DatabaseConfig):
    def get_config(self):
        return {
            'database': os.getenv('DB_MYSQL_DATABASE'),
            'host': os.getenv('DB_MYSQL_HOST'),
            'port': os.getenv('DB_MYSQL_PORT'),
            'user_name': os.getenv('DB_MYSQL_ROOT_USER'),
            'password': os.getenv('DB_MYSQL_ROOT_PASSWORD')
        }

class PostgresConfig(DatabaseConfig):
    def get_config(self):
        return {
            'database': os.getenv('DB_POSTGRES_DATABASE'),
            'host': os.getenv('DB_POSTGRES_HOST'),
            'port': os.getenv('DB_POSTGRES_PORT'),
            'user_name': os.getenv('DB_POSTGRES_USERNAME'),
            'password': os.getenv('DB_POSTGRES_PASSWORD')
        }