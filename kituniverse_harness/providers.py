from __future__ import annotations

import json
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from typing import Any, Dict, List, Optional


DEFAULT_BASE_URL = "http://10.0.0.38:1234/v1"
DEFAULT_MODEL = "lfm2.5-350m-heretic-high-reasoning"


@dataclass
class ProviderResponse:
    ok: bool
    content: str
    raw: Dict[str, Any]
    model: str
    usage: Dict[str, Any]
    error: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "ok": self.ok,
            "content": self.content,
            "raw": self.raw,
            "model": self.model,
            "usage": self.usage,
            "error": self.error,
        }


class LMStudioProvider:
    def __init__(
        self,
        base_url: str = DEFAULT_BASE_URL,
        model: str = DEFAULT_MODEL,
        timeout_seconds: int = 90,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.server_url = self.base_url[:-3] if self.base_url.endswith("/v1") else self.base_url
        self.model = model
        self.timeout_seconds = timeout_seconds

    def models(self) -> Dict[str, Any]:
        return self._request("GET", "/models")

    def health(self) -> Dict[str, Any]:
        started = time.time()
        try:
            models_raw = self.models()
        except Exception as exc:  # noqa: BLE001 - normalize provider failures.
            return {
                "ok": False,
                "endpoint_reachable": False,
                "model_listed": False,
                "model": self.model,
                "error": str(exc),
                "elapsed_seconds": round(time.time() - started, 3),
            }

        model_ids = [item.get("id") for item in models_raw.get("data", [])]
        return {
            "ok": self.model in model_ids,
            "endpoint_reachable": True,
            "model_listed": self.model in model_ids,
            "model": self.model,
            "model_ids": model_ids,
            "elapsed_seconds": round(time.time() - started, 3),
        }

    def native_models(self) -> Dict[str, Any]:
        return self._request_url("GET", f"{self.server_url}/api/v1/models")

    def ensure_loaded(
        self,
        context_length: int = 8192,
        parallel: Optional[int] = None,
        flash_attention: bool = True,
        offload_kv_cache_to_gpu: bool = True,
    ) -> Dict[str, Any]:
        native = self.native_models()
        model = next(
            (item for item in native.get("models", []) if item.get("key") == self.model),
            None,
        )
        if model is None:
            return {
                "ok": False,
                "model": self.model,
                "status": "missing",
                "error": "model not found in native model list",
            }
        instances = model.get("loaded_instances") or []
        if instances:
            load_config = instances[0].get("config") or {}
            requested = {
                "context_length": context_length,
                **({"parallel": parallel} if parallel is not None else {}),
            }
            return {
                "ok": True,
                "model": self.model,
                "status": "already-loaded",
                "instance_id": instances[0].get("id"),
                "load_config": load_config,
                "config_matches": all(load_config.get(key) == value for key, value in requested.items()),
            }
        payload = {
            "model": self.model,
            "context_length": context_length,
            "flash_attention": flash_attention,
            "offload_kv_cache_to_gpu": offload_kv_cache_to_gpu,
            "echo_load_config": True,
        }
        if parallel is not None:
            payload["parallel"] = parallel
        try:
            loaded = self._request_url(
                "POST",
                f"{self.server_url}/api/v1/models/load",
                payload,
            )
        except Exception as exc:  # noqa: BLE001 - normalize native load failures.
            return {
                "ok": False,
                "model": self.model,
                "status": "load-failed",
                "error": str(exc),
            }
        return {
            "ok": loaded.get("status") == "loaded",
            "model": self.model,
            **loaded,
        }

    def chat(
        self,
        messages: List[Dict[str, str]],
        temperature: float = 0.1,
        max_tokens: int = 1600,
        response_format: Optional[Dict[str, Any]] = None,
    ) -> ProviderResponse:
        payload = {
            "model": self.model,
            "messages": messages,
            "temperature": temperature,
            "max_tokens": max_tokens,
        }
        if response_format is not None:
            payload["response_format"] = response_format
        try:
            raw = self._request("POST", "/chat/completions", payload)
        except Exception as exc:  # noqa: BLE001 - normalize provider failures.
            return ProviderResponse(
                ok=False,
                content="",
                raw={},
                model=self.model,
                usage={},
                error=str(exc),
            )

        choice = (raw.get("choices") or [{}])[0]
        message = choice.get("message") or {}
        content = message.get("content") or ""
        return ProviderResponse(
            ok=bool(content.strip()),
            content=content,
            raw=raw,
            model=raw.get("model") or self.model,
            usage=raw.get("usage") or {},
            error=None if content.strip() else "empty provider content",
        )

    def _request(
        self,
        method: str,
        path: str,
        payload: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        return self._request_url(method, f"{self.base_url}{path}", payload)

    def _request_url(
        self,
        method: str,
        url: str,
        payload: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        body = None
        headers = {"Accept": "application/json"}
        if payload is not None:
            body = json.dumps(payload).encode("utf-8")
            headers["Content-Type"] = "application/json"

        request = urllib.request.Request(
            url,
            data=body,
            headers=headers,
            method=method,
        )
        try:
            with urllib.request.urlopen(request, timeout=self.timeout_seconds) as response:
                return json.loads(response.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="replace")
            raise RuntimeError(f"HTTP {exc.code} from {url}: {detail}") from exc


def ask_provider(
    prompt: str,
    system: str = "Return concise, valid JSON for the requested task.",
    base_url: str = DEFAULT_BASE_URL,
    model: str = DEFAULT_MODEL,
    temperature: float = 0.1,
    max_tokens: int = 1600,
    timeout_seconds: int = 90,
) -> ProviderResponse:
    provider = LMStudioProvider(
        base_url=base_url,
        model=model,
        timeout_seconds=timeout_seconds,
    )
    return provider.chat(
        messages=[
            {"role": "system", "content": system},
            {"role": "user", "content": prompt},
        ],
        temperature=temperature,
        max_tokens=max_tokens,
    )
