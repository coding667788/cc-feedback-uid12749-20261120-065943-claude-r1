"""Offline service-identity retirement coordinator.

Coordinates the retirement of service identities: services bind multiple
credentials and declare, per action, the minimum number of usable identities.
Identities carry a tenant, a generation, authorized actions and a retirement
phase.  All state is persisted in SQLite; every finalize / migration batch is
validated inside one transaction against the in-transaction clock and state,
and the change plus its receipt are stored in that same transaction.
"""

__all__ = ["store", "core"]
