"""Writes on a history head: each goes to the instance that owns its row.

A head (``CANOPY_ROLE=mirror``) holds every arm's rows beside its own, and an
instance writes only rows it owns (``canopy.core.ownership``). ``routes`` names, for
every write route, how to find the owner of what it changes; ``middleware`` looks
that owner up in the head's copy (``owners``) and, for an arm with a forward URL and
a service key (``canopy.core.arms``), sends the write there over HTTPS with the
caller's name in ``X-Canopy-On-Behalf-Of``, then waits briefly for that arm's
subscription to bring the change back. Everything else runs here.

The arm trusts that header only from service keys, and only with
``CANOPY_ACCEPT_FORWARDED_WRITES`` (``core.security``); it records the name in
``created_by`` and returns ``X-Canopy-Commit-LSN`` for a request that wrote.
"""
