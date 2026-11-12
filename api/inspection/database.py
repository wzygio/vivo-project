"""Apply finite timeouts and server-enforced READ ONLY to inspection DB sessions."""
from contextlib import contextmanager


@contextmanager
def read_only_sessions():
    from sqlalchemy import event
    from sqlalchemy.engine import Engine

    def protect(connection, record, proxy):
        # This project uses PostgreSQL DBAPI connections. Non-PostgreSQL fixture
        # adapters never receive PostgreSQL session statements.
        if type(connection).__module__.startswith('psycopg2'):
            connection.set_session(readonly=True)
            with connection.cursor() as cursor:
                cursor.execute('SET statement_timeout = 120000')
            # Commit session configuration only, before application transactions.
            # psycopg2 now starts every subsequent transaction READ ONLY.
            connection.commit()
    event.listen(Engine, 'checkout', protect)
    try:
        yield
    finally:
        event.remove(Engine, 'checkout', protect)
