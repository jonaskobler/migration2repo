import re
import os
from typing import List, Dict, Any


def parse_sql(sql_file: str) -> List[Dict[str, Any]]:
    with open(sql_file, 'r') as file:
        sql = file.read()

    tables = []
    table_regex = re.compile(r'CREATE TABLE (\w+) \((.*?)\);', re.S)
    for match in table_regex.finditer(sql):
        table_name = match.group(1)
        columns = match.group(2).strip().split(',')
        columns = [col.strip().split()[0] for col in columns if 'constraint' not in col.lower()]
        tables.append({'table_name': table_name, 'columns': columns})

    return tables


def generate_abstract_repository(tables: List[Dict[str, Any]]) -> str:
    methods = []
    for table in tables:
        class_name = table['table_name'].capitalize()
        methods.append(
            f'    async def add_{table["table_name"]}(self, {table["table_name"]}: {class_name}) -> str | None:')
        methods.append('        raise NotImplementedError')
        methods.append('')
        methods.append(
            f'    async def get_{table["table_name"]}(self, {table["columns"][0]}: str) -> {class_name} | None:')
        methods.append('        raise NotImplementedError')
        methods.append('')
        methods.append(f'    async def get_{table["table_name"]}s(self) -> list[{class_name}]:')
        methods.append('        raise NotImplementedError')
        methods.append('')

    return f"""from abc import ABC, abstractmethod

class AbstractRepository(ABC):
{os.linesep.join(methods)}
"""


def generate_postgres_adapter(tables: List[Dict[str, Any]]) -> str:
    imports = [
        'import psycopg2',
        'from psycopg2.extensions import connection as Connection',
        'from app.database_modules.abstract_repository import AbstractRepository'
    ]
    class_def = 'class PostgresAdapter(AbstractRepository):\n    def __init__(self, connection: Connection):\n        self.connection = connection\n'

    methods = []
    for table in tables:
        class_name = table['table_name'].capitalize()
        column_list = ', '.join(table['columns'])
        placeholders = ', '.join(['%s' for _ in table['columns']])
        column_assignments = ', '.join([f'{col}=row[{i}]' for i, col in enumerate(table['columns'])])

        table_name = table["table_name"]
        table_columns = table["columns"]

        # Add method
        add_method = (
            f'    async def add_{table_name}(self, {table_name}: {class_name}) -> str | None:\n'
            '        sql_query = (\n'
            f'            "INSERT INTO {table_name} ({column_list}) "\n'
            f'            "VALUES ({placeholders}) RETURNING {table_columns[0]}"\n'
            '        )\n'
            f'        return await self.__execute_write_query(sql_query, ({", ".join([f"{table_name}." + col for col in table_columns])}))\n'
        )
        methods.append(add_method)

        # Get single method
        get_single_method = (
            f'    async def get_{table_name}(self, {table_columns[0]}: str) -> {class_name} | None:\n'
            '        sql_query = (\n'
            f'            "SELECT {column_list} FROM {table_name} "\n'
            f'            "WHERE {table_columns[0]} = %s"\n'
            '        )\n'
            f'        res = await self.__execute_retrieval_query(sql_query, ({table_columns[0]},))\n'
            '        if len(res) == 0:\n'
            '            return None\n'
            f'        return {class_name}(\n'
        )
        for col in table_columns:
            get_single_method += f'            {col}=res[0][{table_columns.index(col)}],\n'
        get_single_method += '        )\n'
        methods.append(get_single_method)

        # Get multiple method
        get_multiple_method = (
            f'    async def get_{table_name}s(self) -> list[{class_name}]:\n'
            '        sql_query = (\n'
            f'            "SELECT {column_list} FROM {table_name}"\n'
            '        )\n'
            '        res = await self.__execute_retrieval_query(sql_query)\n'
            '        return [\n'
            f'            {class_name}(\n'
        )
        for col in table_columns:
            get_multiple_method += f'                {col}=row[{table_columns.index(col)}],\n'
        get_multiple_method += '            ) for row in res\n'
        get_multiple_method += '        ]\n'
        methods.append(get_multiple_method)

    utility_methods = [
        '    async def __execute_retrieval_query(self, query: str, params: tuple | None = None) -> list:',
        '        with self.connection:',
        '            with self.connection.cursor() as cursor:',
        '                cursor.execute(query, params)',
        '                return cursor.fetchall()',
        '',
        '    async def __execute_write_query(self, query: str, params: tuple | None = None) -> str | None:',
        '        with self.connection:',
        '            with self.connection.cursor() as cursor:',
        '                cursor.execute(query, params)',
        '                result = cursor.fetchone()',
        '                return result[0] if result else None',
    ]

    # Create the different parts of the final string
    imports_str = os.linesep.join(imports)
    class_def_str = class_def
    methods_str = os.linesep.join(methods)
    utility_methods_str = os.linesep.join(utility_methods)

    # Combine them into the final result
    final_result = f"""{imports_str}

{class_def_str}
{methods_str}
{utility_methods_str}
"""

    return final_result


def generate_data_class(table: Dict[str, Any]) -> str:
    class_name = table['table_name'].capitalize()
    fields = [f'    {col}: str' for col in table['columns'] if col != 'id']
    fields.append('    id: str = uuid.uuid4()')

    return f"""import uuid
from dataclasses import dataclass

@dataclass
class {class_name}:
{os.linesep.join(fields)}
"""


def write_file(filename: str, content: str):
    with open(filename, 'w') as file:
        file.write(content)


def main(sql_file: str):
    tables = parse_sql(sql_file)

    abstract_repository_content = generate_abstract_repository(tables)
    postgres_adapter_content = generate_postgres_adapter(tables)

    write_file('abstract_repository.py', abstract_repository_content)
    write_file('postgres_adapter.py', postgres_adapter_content)

    for table in tables:
        class_content = generate_data_class(table)
        write_file(f'{table["table_name"]}.py', class_content)


if __name__ == "__main__":
    sql_file = 'test.sql'
    main(sql_file)
