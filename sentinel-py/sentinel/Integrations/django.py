try:
    import django
except ImportError:
    raise ImportError(
        "DjangoSentinel requires Django. "
        "Install it with: pip install sentinel-coordination[django]"
    )

from ..sentinel import Sentinel

def _get_django_conn():
    from django.db import connections
    conn = connections["default"]
    conn.ensure_connection()
    return conn.connection

class DjangoSentinel(Sentinel):
    def __init__(self, default_ttl_ms=3000, namespace=None):
        super().__init__(
            get_conn=_get_django_conn,
            default_ttl_ms=default_ttl_ms,
            namespace=namespace,
            owns_connection=False
        )