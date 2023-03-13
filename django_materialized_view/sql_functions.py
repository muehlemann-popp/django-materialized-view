from typing import List

from django.db import connection


def dictfetchall(cursor: connection.cursor) -> List[dict]:
    """Return all rows from a cursor as a dict"""
    columns = [col[0] for col in cursor.description]
    return [dict(zip(columns, row)) for row in cursor.fetchall()]
