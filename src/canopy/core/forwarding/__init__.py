"""Forward mode: a history instance that passes selected writes to its primary.

A ``forward`` instance (``CANOPY_ROLE=forward``) is a mirror in every other
respect. ``routes`` lists the writes it may forward; ``middleware`` sends them to
the primary over HTTPS with the instance's service key and the caller's name in
``X-Canopy-On-Behalf-Of``, then waits briefly for replication to bring the change
back. The primary trusts that header only from service keys, and only with
``CANOPY_ACCEPT_FORWARDED_WRITES`` (``core.security``); it records the name in
``created_by`` and returns ``X-Canopy-Commit-LSN`` for a request that wrote.

The primary stays the only writer and the only machine controller.
"""
