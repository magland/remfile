"""Identify remfile's requests in object-store access logs.

Outside the browser, every request carries a remfile User-Agent header. S3
server access logs record the User-Agent, so bucket owners can attribute the
traffic to remfile without the url being touched (which also keeps presigned
urls and redirects working as-is).

Under pyodide the requests are made by the browser, which does not let a page
set the User-Agent. There, requests to the DANDI, EMBER and OpenNeuro archives
instead carry a `source=remfile` query parameter, which their access logs also
record and their object stores ignore. Other hosts are left alone, since an
arbitrary server may not tolerate an unknown query parameter.
"""
import importlib.metadata
import sys
from urllib.parse import unquote, urlsplit

try:
    _version = importlib.metadata.version("remfile")
except importlib.metadata.PackageNotFoundError:  # pragma: no cover
    _version = "unknown"

USER_AGENT = f"remfile/{_version}"

REQUEST_WATERMARK_PARAM = "source"
REQUEST_WATERMARK_VALUE = "remfile"
REQUEST_WATERMARK = f"{REQUEST_WATERMARK_PARAM}={REQUEST_WATERMARK_VALUE}"

# Archive domains; a host matches the domain itself or any subdomain of it.
_WATERMARK_DOMAINS = ("dandiarchive.org", "emberarchive.org", "openneuro.org")
# S3 buckets behind those archives, matched by name prefix
# (dandiarchive, dandiarchive-embargo, openneuro.org, ember-open-data).
_WATERMARK_BUCKET_PREFIXES = ("dandiarchive", "openneuro", "ember-open-data")


def _in_pyodide() -> bool:
    return sys.platform == "emscripten"


def _s3_bucket(host: str, path: str):
    """The bucket an S3 url addresses, or None for a non-S3 url."""
    if not host.endswith(".amazonaws.com"):
        return None
    if host.startswith(("s3.", "s3-")):
        # path-style: https://s3.amazonaws.com/<bucket>/<key>
        return path.lstrip("/").split("/")[0] or None
    # virtual-hosted style: https://<bucket>.s3[.<region>].amazonaws.com/<key>
    i = host.find(".s3")
    return host[:i] if i > 0 else None


def is_watermark_host(url: str) -> bool:
    """Whether the url points at the DANDI, EMBER or OpenNeuro archives."""
    try:
        parts = urlsplit(url)
    except ValueError:
        return False
    host = (parts.hostname or "").lower()
    if any(host == d or host.endswith("." + d) for d in _WATERMARK_DOMAINS):
        return True
    bucket = _s3_bucket(host, parts.path)
    return bucket is not None and bucket.startswith(_WATERMARK_BUCKET_PREFIXES)


def _split_url(url: str):
    base, sep, hash_ = url.partition("#")
    hash_ = sep + hash_
    base, _, query = base.partition("?")
    return base, query, hash_


def _query_param_names(query: str):
    return [unquote(kv.split("=")[0]) for kv in query.split("&") if kv != ""]


def is_presigned_url(url: str) -> bool:
    """Whether the url is a presigned S3 url.

    Its signature covers the whole query string, so a parameter cannot be added
    to it without invalidating it.
    """
    _, query, _ = _split_url(url)
    return any(
        n.lower() in ("x-amz-signature", "signature")
        for n in _query_param_names(query)
    )


def add_request_watermark(url: str) -> str:
    """The url with the remfile watermark appended to its query string.

    Returns the url unchanged when it is not an http(s) url, already carries
    the watermark parameter, or is presigned (see is_presigned_url).
    Idempotent, so it is safe to apply at more than one layer.
    """
    if not url.lower().startswith(("http://", "https://")):
        return url
    if is_presigned_url(url):
        return url
    base, query, hash_ = _split_url(url)
    if REQUEST_WATERMARK_PARAM in _query_param_names(query):
        return url
    params = [kv for kv in query.split("&") if kv != ""]
    params.append(REQUEST_WATERMARK)
    return f"{base}?{'&'.join(params)}{hash_}"


def request_url(url: str) -> str:
    """The url to request: watermarked only under pyodide, for archive hosts."""
    if _in_pyodide() and is_watermark_host(url):
        return add_request_watermark(url)
    return url


def request_headers() -> dict:
    """Headers identifying remfile. Empty under pyodide, where the browser
    refuses to let a page set the User-Agent."""
    if _in_pyodide():
        return {}
    return {"User-Agent": USER_AGENT}
