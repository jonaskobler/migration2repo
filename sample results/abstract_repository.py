from abc import ABC, abstractmethod

class AbstractRepository(ABC):
    async def add_document(self, document: Document) -> str | None:
        raise NotImplementedError

    async def get_document(self, id: str) -> Document | None:
        raise NotImplementedError

    async def get_documents(self) -> list[Document]:
        raise NotImplementedError

    async def add_clause(self, clause: Clause) -> str | None:
        raise NotImplementedError

    async def get_clause(self, id: str) -> Clause | None:
        raise NotImplementedError

    async def get_clauses(self) -> list[Clause]:
        raise NotImplementedError

