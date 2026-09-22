"""Query-string watermark for the requests remfile makes to object stores
(DANDI's S3 buckets, OpenNeuro's S3 bucket, ...).

Bucket access logs record the request URI including its query string, so
tagging every object request with a fixed parameter lets a bucket owner
attribute that traffic to remfile. Object stores ignore query parameters they
do not recognize, so the tag does not change what is served.
"""
from urllib.parse import unquote

REQUEST_WATERMARK_PARAM = "source"
REQUEST_WATERMARK_VALUE = "remfile"
REQUEST_WATERMARK = f"{REQUEST_WATERMARK_PARAM}={REQUEST_WATERMARK_VALUE}"


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
