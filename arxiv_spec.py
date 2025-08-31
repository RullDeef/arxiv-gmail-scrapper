from datetime import datetime
from json import dumps

# ---------------------------------------------------------------------------
# Domain model
# ---------------------------------------------------------------------------
class ArxivSpec:
    def __init__(self) -> None:
        self.DOI: str = ""
        self.Date: datetime = datetime.now()
        self.Title: str = ""
        self.Authors: str = ""
        self.Categories: str = ""
        self.Comments: str = ""
        self.MSCClass: str = ""
        self.ACMClass: str = ""
        self.Abstract: str = ""

    def to_json(self) -> str:
        return dumps({
            'doi': self.DOI,
            'date': self.Date.strftime("%Y/%m/%d"),
            'title': self.Title,
            'authors': self.Authors,
            'categories': self.Categories,
            'comments': self.Comments,
            'abstract': self.Abstract,
        })
