from __future__ import annotations

import ipaddress
import socket
from contextlib import suppress
from urllib.parse import urljoin, urlsplit

import httpx

BLOCKED_HOSTNAMES = {
    "localhost",
    "localhost.localdomain",
    "host.docker.internal",
    "gateway.docker.internal",
    "kubernetes.docker.internal",
}
BLOCKED_HOST_SUFFIXES = (
    ".internal",
    ".local",
    ".localhost",
    ".home",
    ".arpa",
)
SAFE_REDIRECT_LIMIT = 5


class UnsafeOutboundUrlError(ValueError):
    pass


def _resolve_host_addresses(hostname: str, port: int) -> set[ipaddress.IPv4Address | ipaddress.IPv6Address]:
    try:
        records = socket.getaddrinfo(hostname, port, type=socket.SOCK_STREAM)
    except socket.gaierror:
        return set()

    addresses: set[ipaddress.IPv4Address | ipaddress.IPv6Address] = set()
    for _, _, _, _, sockaddr in records:
        if not sockaddr:
            continue
        raw_address = str(sockaddr[0]).split("%", 1)[0]
        with suppress(ValueError):
            addresses.add(ipaddress.ip_address(raw_address))
    return addresses


def validate_outbound_url(url: str) -> str:
    candidate = str(url or "").strip()
    parsed = urlsplit(candidate)
    scheme = parsed.scheme.lower()
    if scheme not in {"http", "https"}:
        raise UnsafeOutboundUrlError("Outbound URLs must use http or https.")
    if parsed.username or parsed.password:
        raise UnsafeOutboundUrlError("Outbound URLs must not include embedded credentials.")
    hostname = (parsed.hostname or "").rstrip(".").lower()
    if not hostname:
        raise UnsafeOutboundUrlError("Outbound URL is missing a hostname.")

    try:
        port = parsed.port or (443 if scheme == "https" else 80)
    except ValueError as exc:
        raise UnsafeOutboundUrlError("Outbound URL includes an invalid port.") from exc

    try:
        address = ipaddress.ip_address(hostname)
    except ValueError:
        address = None
    if address is not None:
        if not address.is_global:
            raise UnsafeOutboundUrlError(
                "Outbound URLs must not target a private or local network address."
            )
        return candidate

    if hostname in BLOCKED_HOSTNAMES or hostname.endswith(BLOCKED_HOST_SUFFIXES):
        raise UnsafeOutboundUrlError(
            "Outbound URLs must not target a private or local network hostname."
        )
    if "." not in hostname:
        raise UnsafeOutboundUrlError("Outbound URLs must use a fully qualified public hostname.")

    resolved_addresses = _resolve_host_addresses(hostname, port)
    if any(not address.is_global for address in resolved_addresses):
        raise UnsafeOutboundUrlError(
            "Outbound URLs must not resolve to a private or local network address."
        )

    return candidate


def fetch_safe_response(
    url: str,
    *,
    timeout: float,
    headers: dict[str, str] | None = None,
    max_redirects: int = SAFE_REDIRECT_LIMIT,
    allow_insecure_tls: bool = False,
) -> httpx.Response:
    current_url = validate_outbound_url(url)
    request_headers = headers or {}
    with httpx.Client(
        follow_redirects=False,
        timeout=timeout,
        verify=not allow_insecure_tls,
    ) as client:
        for redirect_count in range(max_redirects + 1):
            response = client.get(current_url, headers=request_headers)
            response.read()

            if not response.is_redirect:
                validate_outbound_url(str(response.url))
                return response

            if redirect_count >= max_redirects:
                raise UnsafeOutboundUrlError("Outbound fetch exceeded the redirect limit.")

            redirect_target = response.headers.get("location")
            if not redirect_target:
                return response
            current_url = validate_outbound_url(urljoin(str(response.url), redirect_target))

    raise UnsafeOutboundUrlError("Outbound fetch could not be completed.")
