"""
Security layer with API key authentication, RBAC, and audit logging.

Features:
- API key-based authentication
- Role-Based Access Control (admin, writer, reader)
- Inter-node authentication via shared secret
- Comprehensive audit logging
"""

import time
import logging
from typing import Dict, List, Optional
from enum import Enum
from dataclasses import dataclass, field
from collections import deque
from functools import wraps

from fastapi import Request, HTTPException
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import JSONResponse

logger = logging.getLogger(__name__)


class Role(str, Enum):
    """User roles for RBAC."""
    ADMIN = "admin"
    WRITER = "writer"
    READER = "reader"


# Role permissions: what each role can access
ROLE_PERMISSIONS = {
    Role.ADMIN: {"GET", "POST", "PUT", "DELETE"},
    Role.WRITER: {"GET", "POST", "PUT"},
    Role.READER: {"GET"},
}

# Paths that don't require authentication
PUBLIC_PATHS = {
    "/health",
    "/docs",
    "/openapi.json",
    "/redoc",
}

# Internal paths (require node secret, not API key)
INTERNAL_PATHS = {
    "/raft/request-vote",
    "/raft/append-entries",
    "/cache/snoop/read",
    "/cache/snoop/invalidate",
}


@dataclass
class AuditEntry:
    """An audit log entry."""
    timestamp: float = field(default_factory=time.time)
    client_ip: str = ""
    method: str = ""
    path: str = ""
    role: str = ""
    status_code: int = 0
    latency_ms: float = 0
    user_agent: str = ""

    def to_dict(self) -> Dict:
        return {
            "timestamp": self.timestamp,
            "datetime": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(self.timestamp)),
            "client_ip": self.client_ip,
            "method": self.method,
            "path": self.path,
            "role": self.role,
            "status_code": self.status_code,
            "latency_ms": round(self.latency_ms, 2),
        }


class SecurityManager:
    """Manages authentication, authorization, and audit logging."""

    def __init__(
        self,
        api_key: str = "",
        admin_key: str = "",
        node_secret: str = "",
        max_audit_entries: int = 1000,
    ):
        self.api_keys: Dict[str, Role] = {}
        self.node_secret = node_secret
        self.audit_log: deque = deque(maxlen=max_audit_entries)

        # Register keys
        if api_key:
            self.api_keys[api_key] = Role.WRITER
        if admin_key:
            self.api_keys[admin_key] = Role.ADMIN

    def authenticate(self, request: Request) -> Optional[Role]:
        """
        Authenticate a request and return the role.
        Returns None if authentication fails.
        """
        path = request.url.path

        # Public paths don't need auth
        if path in PUBLIC_PATHS:
            return Role.READER

        # Internal paths use node secret
        if any(path.startswith(p) for p in INTERNAL_PATHS):
            secret = request.headers.get("X-Node-Secret", "")
            if secret == self.node_secret or not self.node_secret:
                return Role.ADMIN
            return None

        # Standard API key auth
        api_key = request.headers.get("X-API-Key", "")
        if not api_key:
            api_key = request.query_params.get("api_key", "")

        if api_key in self.api_keys:
            return self.api_keys[api_key]

        # If no keys configured, allow all (dev mode)
        if not self.api_keys:
            return Role.ADMIN

        return None

    def authorize(self, role: Role, method: str) -> bool:
        """Check if a role is authorized for a given HTTP method."""
        allowed_methods = ROLE_PERMISSIONS.get(role, set())
        return method in allowed_methods

    def add_audit_entry(self, entry: AuditEntry):
        """Add an entry to the audit log."""
        self.audit_log.append(entry)

    def get_audit_log(self, limit: int = 50) -> List[Dict]:
        """Get recent audit entries."""
        entries = list(self.audit_log)[-limit:]
        return [e.to_dict() for e in entries]


class SecurityMiddleware(BaseHTTPMiddleware):
    """FastAPI middleware for authentication, authorization, and audit logging."""

    def __init__(self, app, security_manager: SecurityManager):
        super().__init__(app)
        self.security = security_manager

    async def dispatch(self, request: Request, call_next):
        start_time = time.time()
        path = request.url.path

        # Authenticate
        role = self.security.authenticate(request)

        if role is None:
            # Log failed auth
            self.security.add_audit_entry(AuditEntry(
                client_ip=request.client.host if request.client else "unknown",
                method=request.method,
                path=path,
                role="unauthenticated",
                status_code=401,
            ))
            return JSONResponse(
                status_code=401,
                content={"detail": "Unauthorized. Provide X-API-Key header."},
            )

        # Authorize
        if not self.security.authorize(role, request.method):
            self.security.add_audit_entry(AuditEntry(
                client_ip=request.client.host if request.client else "unknown",
                method=request.method,
                path=path,
                role=role.value,
                status_code=403,
            ))
            return JSONResponse(
                status_code=403,
                content={
                    "detail": f"Forbidden. Role '{role.value}' cannot perform {request.method}."
                },
            )

        # Process request
        response = await call_next(request)

        # Audit log
        latency_ms = (time.time() - start_time) * 1000
        self.security.add_audit_entry(AuditEntry(
            client_ip=request.client.host if request.client else "unknown",
            method=request.method,
            path=path,
            role=role.value,
            status_code=response.status_code,
            latency_ms=latency_ms,
        ))

        return response
