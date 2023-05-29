import pypika
from pypika import Table, Query, Schema


def t():

    sch = Schema('etl')
    causeCode = Table('CauseCode')
    query = Query.from_(sch.causeCode).select('id', 'CauseCode')
    return query
