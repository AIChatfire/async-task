"""上游适配：请求发出、响应解析、上游方言差异。"""

from .client import UpstreamCallResult, UpstreamClient, UpstreamResponse, pin_request

__all__ = ["UpstreamCallResult", "UpstreamClient", "UpstreamResponse", "pin_request"]
