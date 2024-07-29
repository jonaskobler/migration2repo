import uuid
from dataclasses import dataclass

@dataclass
class Clause:
    documentId: str
    section: str
    subsection: str
    content: str
    id: str = uuid.uuid4()
