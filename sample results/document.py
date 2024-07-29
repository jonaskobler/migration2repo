import uuid
from dataclasses import dataclass

@dataclass
class Document:
    name: str
    url: str
    status: str
    id: str = uuid.uuid4()
