import psycopg2
from psycopg2.extensions import connection as Connection
from app.database_modules.abstract_repository import AbstractRepository

class PostgresAdapter(AbstractRepository):
    def __init__(self, connection: Connection):
        self.connection = connection

    async def add_document(self, document: Document) -> str | None:
        sql_query = (
            "INSERT INTO document (id, name, url, status) "
            "VALUES (%s, %s, %s, %s) RETURNING id"
        )
        return await self.__execute_write_query(sql_query, (document.id, document.name, document.url, document.status))

    async def get_document(self, id: str) -> Document | None:
        sql_query = (
            "SELECT id, name, url, status FROM document "
            "WHERE id = %s"
        )
        res = await self.__execute_retrieval_query(sql_query, (id,))
        if len(res) == 0:
            return None
        return Document(
            id=res[0][0],
            name=res[0][1],
            url=res[0][2],
            status=res[0][3],
        )

    async def get_documents(self) -> list[Document]:
        sql_query = (
            "SELECT id, name, url, status FROM document"
        )
        res = await self.__execute_retrieval_query(sql_query)
        return [
            Document(
                id=row[0],
                name=row[1],
                url=row[2],
                status=row[3],
            ) for row in res
        ]

    async def add_clause(self, clause: Clause) -> str | None:
        sql_query = (
            "INSERT INTO clause (id, documentId, section, subsection, content) "
            "VALUES (%s, %s, %s, %s, %s) RETURNING id"
        )
        return await self.__execute_write_query(sql_query, (clause.id, clause.documentId, clause.section, clause.subsection, clause.content))

    async def get_clause(self, id: str) -> Clause | None:
        sql_query = (
            "SELECT id, documentId, section, subsection, content FROM clause "
            "WHERE id = %s"
        )
        res = await self.__execute_retrieval_query(sql_query, (id,))
        if len(res) == 0:
            return None
        return Clause(
            id=res[0][0],
            documentId=res[0][1],
            section=res[0][2],
            subsection=res[0][3],
            content=res[0][4],
        )

    async def get_clauses(self) -> list[Clause]:
        sql_query = (
            "SELECT id, documentId, section, subsection, content FROM clause"
        )
        res = await self.__execute_retrieval_query(sql_query)
        return [
            Clause(
                id=row[0],
                documentId=row[1],
                section=row[2],
                subsection=row[3],
                content=row[4],
            ) for row in res
        ]

    async def __execute_retrieval_query(self, query: str, params: tuple | None = None) -> list:
        with self.connection:
            with self.connection.cursor() as cursor:
                cursor.execute(query, params)
                return cursor.fetchall()

    async def __execute_write_query(self, query: str, params: tuple | None = None) -> str | None:
        with self.connection:
            with self.connection.cursor() as cursor:
                cursor.execute(query, params)
                result = cursor.fetchone()
                return result[0] if result else None
