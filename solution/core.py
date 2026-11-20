"""Domain logic: request processing, eligibility, coverage, idempotency.

Time is a logical integer clock supplied by the caller on every step ("now"
for mutations, "at" for queries).  The value used for validation is read
inside the transaction, so a finalize / migration batch is always checked
against the in-transaction clock and state, and the change plus its receipt
are written in that same transaction.
"""
from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import time

# --------------------------------------------------------------------------
# Error codes
# --------------------------------------------------------------------------
MALFORMED_REQUEST = "MALFORMED_REQUEST"        # missing / ill-typed fields (not persisted)
IDEMPOTENCY_CONFLICT = "IDEMPOTENCY_CONFLICT"  # same request_id, different params (not persisted)
SYNC_TIMEOUT = "SYNC_TIMEOUT"                  # test sync-point not released in time (not persisted)

TENANT_EXISTS = "TENANT_EXISTS"
UNKNOWN_TENANT = "UNKNOWN_TENANT"
IDENTITY_EXISTS = "IDENTITY_EXISTS"
UNKNOWN_IDENTITY = "UNKNOWN_IDENTITY"
CREDENTIAL_EXISTS = "CREDENTIAL_EXISTS"
UNKNOWN_CREDENTIAL = "UNKNOWN_CREDENTIAL"
INVALID_WINDOW = "INVALID_WINDOW"
ALREADY_REVOKED = "ALREADY_REVOKED"
SERVICE_EXISTS = "SERVICE_EXISTS"
UNKNOWN_SERVICE = "UNKNOWN_SERVICE"
ALREADY_BOUND = "ALREADY_BOUND"
NOT_BOUND = "NOT_BOUND"
PLAN_EXISTS = "PLAN_EXISTS"
NO_OPEN_PLAN = "NO_OPEN_PLAN"
INVALID_CANDIDATE = "INVALID_CANDIDATE"
INVALID_DEADLINE = "INVALID_DEADLINE"
SOURCE_NOT_RETIRING = "SOURCE_NOT_RETIRING"
TARGET_NOT_ACTIVE = "TARGET_NOT_ACTIVE"
TARGET_NOT_ELIGIBLE = "TARGET_NOT_ELIGIBLE"
TENANT_MISMATCH = "TENANT_MISMATCH"
COVERAGE_INSUFFICIENT = "COVERAGE_INSUFFICIENT"

PHASE_ACTIVE = "ACTIVE"
PHASE_PREPARED = "PREPARED"
PHASE_RETIRED = "RETIRED"

MUTATING_OPS = {
    "create_tenant", "create_identity", "create_credential", "revoke_credential",
    "create_service", "bind", "prepare", "migrate", "finalize", "cancel",
}
READONLY_OPS = {"query_coverage", "query_identity", "query_credential", "query_service"}


class InputError(Exception):
    """Request rejected before any persistence; nothing is stored."""

    def __init__(self, code: str, message: str):
        super().__init__(message)
        self.code = code


class BusinessFailure(Exception):
    """Deterministic business outcome; persisted as a FAILED receipt."""

    def __init__(self, code: str, detail: dict | None = None):
        super().__init__(code)
        self.code = code
        self.detail = detail or {}


class SyncTimeout(Exception):
    """A test sync point was not released before its timeout."""


# --------------------------------------------------------------------------
# Test sync points (used only by the crash / concurrency tests)
# --------------------------------------------------------------------------
def _sync_point(phase: str, request_id: str) -> None:
    directory = os.environ.get("SIRC_SYNC_" + phase.upper())
    if not directory:
        return
    os.makedirs(directory, exist_ok=True)
    ready = os.path.join(directory, f"{phase}.{request_id}.ready")
    go = os.path.join(directory, f"{phase}.{request_id}.go")
    with open(ready, "w", encoding="utf-8") as fh:
        fh.write("ready")
    timeout = float(os.environ.get("SIRC_SYNC_TIMEOUT", "60"))
    deadline = time.monotonic() + timeout
    while not os.path.exists(go):
        if time.monotonic() > deadline:
            raise SyncTimeout(f"sync point {phase} for {request_id} not released")
        time.sleep(0.05)


# --------------------------------------------------------------------------
# Field validation
# --------------------------------------------------------------------------
def _req_str(step: dict, name: str) -> str:
    value = step.get(name)
    if not isinstance(value, str) or not value:
        raise InputError(MALFORMED_REQUEST, f"{name!r} must be a non-empty string")
    return value


def _req_int(step: dict, name: str) -> int:
    value = step.get(name)
    if not isinstance(value, int) or isinstance(value, bool):
        raise InputError(MALFORMED_REQUEST, f"{name!r} must be an integer")
    return value


def _req_str_list(step: dict, name: str) -> list:
    value = step.get(name)
    if (not isinstance(value, list) or not value
            or any(not isinstance(v, str) or not v for v in value)):
        raise InputError(MALFORMED_REQUEST, f"{name!r} must be a non-empty list of strings")
    return value


def _params_hash(step: dict) -> str:
    payload = {k: v for k, v in step.items() if k != "request_id"}
    blob = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()


# --------------------------------------------------------------------------
# Row access helpers
# --------------------------------------------------------------------------
def _tenant_row(conn, tenant):
    return conn.execute("SELECT tenant_id FROM tenants WHERE tenant_id=?", (tenant,)).fetchone()


def _require_tenant(conn, tenant):
    if _tenant_row(conn, tenant) is None:
        raise BusinessFailure(UNKNOWN_TENANT, {"tenant": tenant})


def _identity_row(conn, tenant, identity_id):
    row = conn.execute(
        "SELECT * FROM identities WHERE tenant=? AND identity_id=?",
        (tenant, identity_id)).fetchone()
    if row is None:
        raise BusinessFailure(UNKNOWN_IDENTITY, {"identity": identity_id})
    return row


def _credential_row(conn, tenant, credential_id):
    row = conn.execute(
        "SELECT * FROM credentials WHERE tenant=? AND credential_id=?",
        (tenant, credential_id)).fetchone()
    if row is not None:
        return row
    other = conn.execute(
        "SELECT tenant FROM credentials WHERE credential_id=?", (credential_id,)).fetchone()
    if other is not None:
        raise BusinessFailure(TENANT_MISMATCH, {
            "credential": credential_id, "actual_tenant": other["tenant"]})
    raise BusinessFailure(UNKNOWN_CREDENTIAL, {"credential": credential_id})


def _service_row(conn, tenant, service_id):
    row = conn.execute(
        "SELECT * FROM services WHERE tenant=? AND service_id=?",
        (tenant, service_id)).fetchone()
    if row is not None:
        return row
    other = conn.execute(
        "SELECT tenant FROM services WHERE service_id=?", (service_id,)).fetchone()
    if other is not None:
        raise BusinessFailure(TENANT_MISMATCH, {
            "service": service_id, "actual_tenant": other["tenant"]})
    raise BusinessFailure(UNKNOWN_SERVICE, {"service": service_id})


def _plan_row(conn, tenant):
    return conn.execute("SELECT * FROM plans WHERE tenant=?", (tenant,)).fetchone()


# --------------------------------------------------------------------------
# Eligibility and coverage
# --------------------------------------------------------------------------
def _eligible(cred, ident, plan, at: int) -> bool:
    """A credential is eligible at `at` iff every condition holds.

    - the bound identity is not RETIRED;
    - the credential is not revoked;
    - the credential's generation equals the identity's *current* generation
      (a revoked generation never becomes eligible again);
    - `valid_from <= at < valid_until` (exactly-at-expiry is ineligible);
    - while its identity is PREPARED, the plan deadline caps survival:
      `at < deadline`.
    """
    if ident["phase"] == PHASE_RETIRED:
        return False
    if cred["revoked"]:
        return False
    if cred["generation"] != ident["generation"]:
        return False
    if not (cred["valid_from"] <= at < cred["valid_until"]):
        return False
    if ident["phase"] == PHASE_PREPARED and plan is not None and at >= plan["deadline"]:
        return False
    return True


def compute_coverage(conn, tenant: str, service_id: str, at: int) -> dict:
    """Coverage of one service at `at`, deduplicated by qualified identity.

    Two credentials of the same identity contribute a single seat.
    """
    svc = _service_row(conn, tenant, service_id)
    requirements = json.loads(svc["requirements"])
    plan = _plan_row(conn, tenant)
    bindings = conn.execute(
        "SELECT credential_id FROM bindings WHERE tenant=? AND service_id=?",
        (tenant, service_id)).fetchall()
    qualified = {action: set() for action in requirements}
    for binding in bindings:
        cred = _credential_row(conn, tenant, binding["credential_id"])
        ident = _identity_row(conn, tenant, cred["identity_id"])
        if not _eligible(cred, ident, plan, at):
            continue
        authorized = set(json.loads(ident["actions"]))
        for action in requirements:
            if action in authorized:
                qualified[action].add(cred["identity_id"])
    actions = {
        action: {
            "required": requirements[action],
            "qualified": sorted(ids),
            "count": len(ids),
            "ok": len(ids) >= requirements[action],
        }
        for action, ids in qualified.items()
    }
    return {
        "service": service_id,
        "at": at,
        "actions": actions,
        "ok": all(a["ok"] for a in actions.values()),
    }


def _check_services(conn, tenant: str, service_ids, now: int) -> None:
    """Verify coverage of every affected service; raise on any shortfall."""
    failures = {}
    for service_id in sorted(service_ids):
        cov = compute_coverage(conn, tenant, service_id, now)
        if not cov["ok"]:
            failures[service_id] = {
                action: {"required": d["required"], "count": d["count"],
                         "qualified": d["qualified"]}
                for action, d in cov["actions"].items() if not d["ok"]
            }
    if failures:
        raise BusinessFailure(COVERAGE_INSUFFICIENT,
                              {"checked_at": now, "services": failures})


def _next_boundary(conn, tenant: str, service_id: str, at: int):
    """Next known credential boundary after `at` for the bound credentials.

    The coverage evidence computed at `at` stays valid over [at, boundary).
    """
    plan = _plan_row(conn, tenant)
    boundaries = []
    rows = conn.execute(
        "SELECT c.valid_until, c.identity_id FROM bindings b "
        "JOIN credentials c ON c.tenant=b.tenant AND c.credential_id=b.credential_id "
        "WHERE b.tenant=? AND b.service_id=?", (tenant, service_id)).fetchall()
    for row in rows:
        if row["valid_until"] > at:
            boundaries.append(row["valid_until"])
        if plan is not None and plan["deadline"] > at:
            ident = _identity_row(conn, tenant, row["identity_id"])
            if ident["phase"] == PHASE_PREPARED:
                boundaries.append(plan["deadline"])
    return min(boundaries) if boundaries else None


# --------------------------------------------------------------------------
# Mutating operations (run inside the request transaction / savepoint)
# --------------------------------------------------------------------------
def _op_create_tenant(conn, step, now):
    tenant = _req_str(step, "tenant")
    if _tenant_row(conn, tenant) is not None:
        raise BusinessFailure(TENANT_EXISTS, {"tenant": tenant})
    conn.execute("INSERT INTO tenants (tenant_id) VALUES (?)", (tenant,))
    return {"tenant": tenant}


def _op_create_identity(conn, step, now):
    tenant = _req_str(step, "tenant")
    _require_tenant(conn, tenant)
    identity = _req_str(step, "identity")
    actions = _req_str_list(step, "actions")
    exists = conn.execute(
        "SELECT 1 FROM identities WHERE tenant=? AND identity_id=?",
        (tenant, identity)).fetchone()
    if exists:
        raise BusinessFailure(IDENTITY_EXISTS, {"identity": identity})
    conn.execute(
        "INSERT INTO identities (tenant, identity_id, generation, actions, phase) "
        "VALUES (?,?,?,?,?)",
        (tenant, identity, 1, json.dumps(sorted(set(actions))), PHASE_ACTIVE))
    return {"identity": identity, "generation": 1, "phase": PHASE_ACTIVE}


def _op_create_credential(conn, step, now):
    tenant = _req_str(step, "tenant")
    _require_tenant(conn, tenant)
    credential = _req_str(step, "credential")
    identity = _req_str(step, "identity")
    valid_from = _req_int(step, "valid_from")
    valid_until = _req_int(step, "valid_until")
    if valid_from >= valid_until:
        raise BusinessFailure(INVALID_WINDOW,
                              {"valid_from": valid_from, "valid_until": valid_until})
    ident = _identity_row(conn, tenant, identity)
    generation = step.get("generation", ident["generation"])
    if not isinstance(generation, int) or isinstance(generation, bool) or generation < 1:
        raise InputError(MALFORMED_REQUEST, "'generation' must be a positive integer")
    exists = conn.execute(
        "SELECT 1 FROM credentials WHERE tenant=? AND credential_id=?",
        (tenant, credential)).fetchone()
    if exists:
        raise BusinessFailure(CREDENTIAL_EXISTS, {"credential": credential})
    conn.execute(
        "INSERT INTO credentials (tenant, credential_id, identity_id, generation, "
        "valid_from, valid_until, revoked) VALUES (?,?,?,?,?,?,0)",
        (tenant, credential, identity, generation, valid_from, valid_until))
    return {"credential": credential, "identity": identity, "generation": generation}


def _op_revoke_credential(conn, step, now):
    tenant = _req_str(step, "tenant")
    _require_tenant(conn, tenant)
    credential = _req_str(step, "credential")
    cred = _credential_row(conn, tenant, credential)
    if cred["revoked"]:
        raise BusinessFailure(ALREADY_REVOKED, {"credential": credential})
    conn.execute(
        "UPDATE credentials SET revoked=1 WHERE tenant=? AND credential_id=?",
        (tenant, credential))
    return {"credential": credential, "revoked": True}


def _op_create_service(conn, step, now):
    tenant = _req_str(step, "tenant")
    _require_tenant(conn, tenant)
    service = _req_str(step, "service")
    requirements = step.get("requirements")
    if (not isinstance(requirements, dict) or not requirements
            or any(not isinstance(k, str) or not k or not isinstance(v, int)
                   or isinstance(v, bool) or v < 1 for k, v in requirements.items())):
        raise InputError(MALFORMED_REQUEST,
                         "'requirements' must map action names to positive integers")
    exists = conn.execute(
        "SELECT 1 FROM services WHERE tenant=? AND service_id=?",
        (tenant, service)).fetchone()
    if exists:
        raise BusinessFailure(SERVICE_EXISTS, {"service": service})
    conn.execute(
        "INSERT INTO services (tenant, service_id, requirements) VALUES (?,?,?)",
        (tenant, service, json.dumps(requirements, sort_keys=True)))
    return {"service": service, "requirements": requirements}


def _op_bind(conn, step, now):
    tenant = _req_str(step, "tenant")
    _require_tenant(conn, tenant)
    service = _req_str(step, "service")
    credential = _req_str(step, "credential")
    _service_row(conn, tenant, service)
    _credential_row(conn, tenant, credential)
    exists = conn.execute(
        "SELECT 1 FROM bindings WHERE tenant=? AND service_id=? AND credential_id=?",
        (tenant, service, credential)).fetchone()
    if exists:
        raise BusinessFailure(ALREADY_BOUND, {"service": service, "credential": credential})
    conn.execute(
        "INSERT INTO bindings (tenant, service_id, credential_id) VALUES (?,?,?)",
        (tenant, service, credential))
    return {"service": service, "credential": credential}


def _op_prepare(conn, step, now):
    """Fix the candidate set and the old-credential survival deadline."""
    tenant = _req_str(step, "tenant")
    _require_tenant(conn, tenant)
    if _plan_row(conn, tenant) is not None:
        raise BusinessFailure(PLAN_EXISTS, {"tenant": tenant})
    candidates = _req_str_list(step, "candidates")
    deadline = _req_int(step, "deadline")
    if deadline <= now:
        raise BusinessFailure(INVALID_DEADLINE, {"deadline": deadline, "now": now})
    for identity in candidates:
        ident = _identity_row(conn, tenant, identity)
        if ident["phase"] != PHASE_ACTIVE:
            raise BusinessFailure(INVALID_CANDIDATE,
                                  {"identity": identity, "phase": ident["phase"]})
    plan_id = step.get("plan_id") or step["request_id"]
    if not isinstance(plan_id, str) or not plan_id:
        raise InputError(MALFORMED_REQUEST, "'plan_id' must be a non-empty string")
    conn.execute(
        "INSERT INTO plans (tenant, plan_id, candidates, deadline, created_at) "
        "VALUES (?,?,?,?,?)",
        (tenant, plan_id, json.dumps(candidates), deadline, now))
    for identity in candidates:
        conn.execute(
            "UPDATE identities SET phase=? WHERE tenant=? AND identity_id=?",
            (PHASE_PREPARED, tenant, identity))
    return {"plan_id": plan_id, "candidates": candidates, "deadline": deadline}


def _op_migrate(conn, step, now):
    """Move bindings between credentials in one batch; same tenant only.

    The batch is atomic: coverage of the union of services affected before
    and after the change is verified at the in-transaction clock, and any
    shortfall rolls the whole batch back.
    """
    tenant = _req_str(step, "tenant")
    _require_tenant(conn, tenant)
    moves = step.get("moves")
    if not isinstance(moves, list) or not moves or not all(isinstance(m, dict) for m in moves):
        raise InputError(MALFORMED_REQUEST, "'moves' must be a non-empty list of objects")
    plan = _plan_row(conn, tenant)
    affected = set()
    validated = []
    for move in moves:
        service = _req_str(move, "service")
        from_id = _req_str(move, "from")
        to_id = _req_str(move, "to")
        _service_row(conn, tenant, service)
        from_cred = _credential_row(conn, tenant, from_id)
        to_cred = _credential_row(conn, tenant, to_id)
        bound = conn.execute(
            "SELECT 1 FROM bindings WHERE tenant=? AND service_id=? AND credential_id=?",
            (tenant, service, from_id)).fetchone()
        if not bound:
            raise BusinessFailure(NOT_BOUND, {"service": service, "credential": from_id})
        already = conn.execute(
            "SELECT 1 FROM bindings WHERE tenant=? AND service_id=? AND credential_id=?",
            (tenant, service, to_id)).fetchone()
        if already:
            raise BusinessFailure(ALREADY_BOUND, {"service": service, "credential": to_id})
        from_ident = _identity_row(conn, tenant, from_cred["identity_id"])
        to_ident = _identity_row(conn, tenant, to_cred["identity_id"])
        if from_ident["phase"] != PHASE_PREPARED:
            raise BusinessFailure(SOURCE_NOT_RETIRING, {
                "credential": from_id, "identity": from_cred["identity_id"],
                "phase": from_ident["phase"]})
        if to_ident["phase"] != PHASE_ACTIVE:
            raise BusinessFailure(TARGET_NOT_ACTIVE, {
                "credential": to_id, "identity": to_cred["identity_id"],
                "phase": to_ident["phase"]})
        if not _eligible(to_cred, to_ident, plan, now):
            raise BusinessFailure(TARGET_NOT_ELIGIBLE, {
                "credential": to_id, "checked_at": now})
        validated.append((service, from_id, to_id))
        affected.add(service)
    for service, from_id, to_id in validated:
        conn.execute(
            "DELETE FROM bindings WHERE tenant=? AND service_id=? AND credential_id=?",
            (tenant, service, from_id))
        conn.execute(
            "INSERT INTO bindings (tenant, service_id, credential_id) VALUES (?,?,?)",
            (tenant, service, to_id))
    _check_services(conn, tenant, affected, now)
    return {"moves": [{"service": s, "from": f, "to": t} for s, f, t in validated],
            "checked_services": sorted(affected)}


def _op_finalize(conn, step, now):
    """Retire the prepared candidates; revoke their current generation.

    Coverage of every service bound to a candidate's credentials is checked
    at the in-transaction clock after applying the change; a shortfall rolls
    the change back (the FAILED receipt is still persisted).
    """
    tenant = _req_str(step, "tenant")
    _require_tenant(conn, tenant)
    plan = _plan_row(conn, tenant)
    if plan is None:
        raise BusinessFailure(NO_OPEN_PLAN, {"tenant": tenant})
    candidates = json.loads(plan["candidates"])
    marks = ",".join("?" for _ in candidates)
    affected = {row["service_id"] for row in conn.execute(
        "SELECT DISTINCT b.service_id FROM bindings b "
        "JOIN credentials c ON c.tenant=b.tenant AND c.credential_id=b.credential_id "
        f"WHERE b.tenant=? AND c.identity_id IN ({marks})",
        (tenant, *candidates)).fetchall()}
    for identity in candidates:
        conn.execute(
            "UPDATE identities SET phase=?, generation=generation+1 "
            "WHERE tenant=? AND identity_id=?",
            (PHASE_RETIRED, tenant, identity))
    conn.execute("DELETE FROM plans WHERE tenant=?", (tenant,))
    _check_services(conn, tenant, affected, now)
    return {"plan_id": plan["plan_id"], "retired": candidates,
            "checked_services": sorted(affected)}


def _op_cancel(conn, step, now):
    """Cancel the open plan.  Committed migrations are NOT rolled back."""
    tenant = _req_str(step, "tenant")
    _require_tenant(conn, tenant)
    plan = _plan_row(conn, tenant)
    if plan is None:
        raise BusinessFailure(NO_OPEN_PLAN, {"tenant": tenant})
    candidates = json.loads(plan["candidates"])
    for identity in candidates:
        conn.execute(
            "UPDATE identities SET phase=? WHERE tenant=? AND identity_id=? AND phase=?",
            (PHASE_ACTIVE, tenant, identity, PHASE_PREPARED))
    conn.execute("DELETE FROM plans WHERE tenant=?", (tenant,))
    return {"plan_id": plan["plan_id"], "reactivated": candidates,
            "note": "committed migrations are kept"}


_OPS = {
    "create_tenant": _op_create_tenant,
    "create_identity": _op_create_identity,
    "create_credential": _op_create_credential,
    "revoke_credential": _op_revoke_credential,
    "create_service": _op_create_service,
    "bind": _op_bind,
    "prepare": _op_prepare,
    "migrate": _op_migrate,
    "finalize": _op_finalize,
    "cancel": _op_cancel,
}


# --------------------------------------------------------------------------
# Read-only queries
# --------------------------------------------------------------------------
def _execute_readonly(conn, step) -> dict:
    op = step["op"]
    tenant = _req_str(step, "tenant")
    if op == "query_coverage":
        at = _req_int(step, "at")
        service = _req_str(step, "service")
        _require_tenant(conn, tenant)
        cov = compute_coverage(conn, tenant, service, at)
        cov["valid_from"] = at
        cov["valid_until"] = _next_boundary(conn, tenant, service, at)
        return {"status": "OK", "error": None, "result": cov}
    if op == "query_identity":
        identity = _req_str(step, "identity")
        _require_tenant(conn, tenant)
        ident = _identity_row(conn, tenant, identity)
        return {"status": "OK", "error": None, "result": {
            "identity": identity,
            "generation": ident["generation"],
            "phase": ident["phase"],
            "actions": json.loads(ident["actions"]),
        }}
    if op == "query_credential":
        credential = _req_str(step, "credential")
        _require_tenant(conn, tenant)
        cred = _credential_row(conn, tenant, credential)
        return {"status": "OK", "error": None, "result": {
            "credential": credential,
            "identity": cred["identity_id"],
            "generation": cred["generation"],
            "valid_from": cred["valid_from"],
            "valid_until": cred["valid_until"],
            "revoked": bool(cred["revoked"]),
        }}
    if op == "query_service":
        service = _req_str(step, "service")
        _require_tenant(conn, tenant)
        svc = _service_row(conn, tenant, service)
        bindings = [r["credential_id"] for r in conn.execute(
            "SELECT credential_id FROM bindings WHERE tenant=? AND service_id=? "
            "ORDER BY credential_id", (tenant, service)).fetchall()]
        return {"status": "OK", "error": None, "result": {
            "service": service,
            "requirements": json.loads(svc["requirements"]),
            "bindings": bindings,
        }}
    raise InputError(MALFORMED_REQUEST, f"unknown op {op!r}")


# --------------------------------------------------------------------------
# Request entry point
# --------------------------------------------------------------------------
def execute_step(store, step: dict) -> dict:
    """Execute one case step and return its result record."""
    op = step.get("op")
    if not isinstance(op, str):
        raise InputError(MALFORMED_REQUEST, "'op' must be a string")
    if op in READONLY_OPS:
        return _execute_readonly(store.conn, step)
    if op not in MUTATING_OPS:
        raise InputError(MALFORMED_REQUEST, f"unknown op {op!r}")
    return _execute_mutating(store, step)


def _execute_mutating(store, step: dict) -> dict:
    tenant = _req_str(step, "tenant")
    request_id = _req_str(step, "request_id")
    now = _req_int(step, "now")
    params_hash = _params_hash(step)
    conn = store.conn
    conn.execute("BEGIN IMMEDIATE")
    try:
        row = conn.execute(
            "SELECT params_hash, receipt FROM requests WHERE tenant=? AND request_id=?",
            (tenant, request_id)).fetchone()
        if row is not None:
            stored = json.loads(row["receipt"])
            if row["params_hash"] == params_hash:
                conn.execute("COMMIT")
                return {"status": stored["status"], "error": stored["error"],
                        "receipt": stored, "replay": True}
            conn.execute("ROLLBACK")
            raise InputError(
                IDEMPOTENCY_CONFLICT,
                f"request_id {request_id!r} already used with different parameters "
                f"in tenant {tenant!r}")
        conn.execute("SAVEPOINT req")
        receipt = None
        try:
            detail = _OPS[step["op"]](conn, step, now)
            receipt = {"status": "OK", "error": None, "detail": detail}
        except BusinessFailure as failure:
            # Roll back the state change; the FAILED receipt is still
            # persisted below, in the same transaction, so a business
            # failure is distinguishable from the (unchanged) state.
            conn.execute("ROLLBACK TO req")
            receipt = {"status": "FAILED", "error": failure.code,
                       "detail": failure.detail}
        finally:
            conn.execute("RELEASE req")
        receipt.update({"op": step["op"], "request_id": request_id,
                        "tenant": tenant, "now": now})
        conn.execute(
            "INSERT INTO requests (tenant, request_id, op, params_hash, receipt) "
            "VALUES (?,?,?,?,?)",
            (tenant, request_id, step["op"], params_hash,
             json.dumps(receipt, sort_keys=True, ensure_ascii=False)))
        _sync_point("before_commit", request_id)
        conn.execute("COMMIT")
        _sync_point("after_commit", request_id)
        return {"status": receipt["status"], "error": receipt["error"],
                "receipt": receipt, "replay": False}
    except (InputError, SyncTimeout):
        _rollback_quietly(conn)
        raise
    except sqlite3.Error:
        _rollback_quietly(conn)
        raise


def _rollback_quietly(conn) -> None:
    try:
        conn.execute("ROLLBACK")
    except sqlite3.Error:
        pass
