from __future__ import annotations

import json
import os
import stat
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


PROFILE_SCHEMA = "kituniverse.openrouter-profile.v1"
REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_PROFILE_PATH = REPO_ROOT / "profiles" / "ask_openrouter.json"
DEFAULT_ENV_PATH = REPO_ROOT / ".env.openrouter.local"


class OpenRouterProfileError(RuntimeError):
    pass


class OpenRouterRequestError(RuntimeError):
    def __init__(
        self,
        message: str,
        *,
        status: Optional[int] = None,
        retry_after: Optional[float] = None,
    ) -> None:
        super().__init__(message)
        self.status = status
        self.retry_after = retry_after


def load_local_environment(path: Path = DEFAULT_ENV_PATH) -> Dict[str, bool]:
    if not path.is_file():
        raise OpenRouterProfileError(f"OpenRouter environment file not found: {path}")
    mode = stat.S_IMODE(path.stat().st_mode)
    if mode & 0o077:
        raise OpenRouterProfileError(
            f"OpenRouter environment file must use mode 0600 or stricter: {path}"
        )
    loaded: Dict[str, bool] = {}
    for line_number, original in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        line = original.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[7:].lstrip()
        if "=" not in line:
            raise OpenRouterProfileError(f"invalid environment entry at {path}:{line_number}")
        name, value = line.split("=", 1)
        name = name.strip()
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in {'"', "'"}:
            value = value[1:-1]
        if not name or not name.replace("_", "").isalnum():
            raise OpenRouterProfileError(f"invalid environment name at {path}:{line_number}")
        if name not in os.environ:
            os.environ[name] = value
        loaded[name] = bool(value)
    return loaded


def load_profile(path: Path = DEFAULT_PROFILE_PATH) -> Dict[str, Any]:
    try:
        profile = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise OpenRouterProfileError(f"OpenRouter profile not found: {path}") from exc
    except json.JSONDecodeError as exc:
        raise OpenRouterProfileError(f"invalid OpenRouter profile JSON: {exc}") from exc
    if profile.get("schema_version") != PROFILE_SCHEMA:
        raise OpenRouterProfileError("unsupported OpenRouter profile schema")
    if profile.get("provider") != "openrouter":
        raise OpenRouterProfileError("profile provider must be openrouter")
    defaults = profile.get("defaults") or {}
    reliability = profile.get("reliability") or {}
    if not defaults.get("model") or not defaults.get("base_url"):
        raise OpenRouterProfileError("profile needs default model and base URL")
    attempts = int(reliability.get("attempts_per_model") or 0)
    if not 1 <= attempts <= 10:
        raise OpenRouterProfileError("attempts_per_model must be between 1 and 10")
    ceiling = int(reliability.get("max_tokens_ceiling") or 0)
    if ceiling < 1:
        raise OpenRouterProfileError("max_tokens_ceiling must be positive")
    reasoning = defaults.get("reasoning") or {}
    if reasoning.get("effort") not in {"none", "low", "high"}:
        raise OpenRouterProfileError("reasoning effort must be none, low, or high")
    return profile


class OpenRouterClient:
    def __init__(
        self,
        base_url: str,
        api_key: str,
        timeout_seconds: int,
        request_headers: Optional[Dict[str, str]] = None,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self.timeout_seconds = timeout_seconds
        self.request_headers = request_headers or {}

    def models(self) -> Dict[str, Any]:
        return self._request("GET", "/models")

    def chat(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        return self._request("POST", "/chat/completions", payload)

    def _request(
        self,
        method: str,
        path: str,
        payload: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        body = None
        headers = {
            "Accept": "application/json",
            "Authorization": f"Bearer {self.api_key}",
            "User-Agent": "NexusEngine-KitUniverse/0.1",
            **self.request_headers,
        }
        if payload is not None:
            body = json.dumps(payload).encode("utf-8")
            headers["Content-Type"] = "application/json"
        request = urllib.request.Request(
            f"{self.base_url}{path}", data=body, headers=headers, method=method
        )
        try:
            with urllib.request.urlopen(request, timeout=self.timeout_seconds) as response:
                return json.loads(response.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="replace")
            message = _safe_error_message(detail) or f"OpenRouter returned HTTP {exc.code}"
            retry_after = _retry_after_seconds(exc.headers.get("Retry-After"))
            raise OpenRouterRequestError(
                message, status=exc.code, retry_after=retry_after
            ) from exc
        except urllib.error.URLError as exc:
            raise OpenRouterRequestError(
                f"OpenRouter connection failed: {exc.reason}"
            ) from exc
        except TimeoutError as exc:
            raise OpenRouterRequestError("OpenRouter request timed out") from exc


def profile_health(
    profile_path: Path = DEFAULT_PROFILE_PATH,
    env_path: Path = DEFAULT_ENV_PATH,
) -> Dict[str, Any]:
    profile = load_profile(profile_path)
    loaded = load_local_environment(env_path)
    resolved = _resolve(profile)
    client = OpenRouterClient(
        resolved["base_url"],
        resolved["api_key"],
        resolved["timeout_seconds"],
        profile.get("request_headers"),
    )
    started = time.monotonic()
    try:
        raw = client.models()
        available = {str(item.get("id")) for item in raw.get("data", [])}
        candidates = resolved["models"]
        return {
            "ok": any(model in available for model in candidates),
            "profile_id": profile["profile_id"],
            "endpoint_reachable": True,
            "credential_configured": loaded.get(resolved["api_key_env"], False),
            "requested_model": candidates[0],
            "candidate_models": [
                {"model": model, "available": model in available} for model in candidates
            ],
            "reasoning": resolved["reasoning"],
            "elapsed_seconds": round(time.monotonic() - started, 3),
        }
    except OpenRouterRequestError as exc:
        return {
            "ok": False,
            "profile_id": profile["profile_id"],
            "endpoint_reachable": exc.status is not None,
            "credential_configured": loaded.get(resolved["api_key_env"], False),
            "error": str(exc),
            "http_status": exc.status,
            "elapsed_seconds": round(time.monotonic() - started, 3),
        }


def ask_openrouter(
    prompt: str,
    *,
    profile_path: Path = DEFAULT_PROFILE_PATH,
    env_path: Path = DEFAULT_ENV_PATH,
    system: Optional[str] = None,
    temperature: Optional[float] = None,
    max_tokens: Optional[int] = None,
) -> Dict[str, Any]:
    profile = load_profile(profile_path)
    load_local_environment(env_path)
    resolved = _resolve(profile)
    defaults = profile["defaults"]
    reliability = profile["reliability"]
    token_limit = int(max_tokens if max_tokens is not None else defaults["max_tokens"])
    ceiling = int(reliability["max_tokens_ceiling"])
    if not 1 <= token_limit <= ceiling:
        raise OpenRouterProfileError(f"max_tokens must be between 1 and {ceiling}")
    chosen_temperature = float(
        temperature if temperature is not None else defaults["temperature"]
    )
    if not 0 <= chosen_temperature <= 2:
        raise OpenRouterProfileError("temperature must be between 0 and 2")
    client = OpenRouterClient(
        resolved["base_url"],
        resolved["api_key"],
        resolved["timeout_seconds"],
        profile.get("request_headers"),
    )
    failures: List[Dict[str, Any]] = []
    attempts = 0
    requested_model = resolved["models"][0]
    started = time.monotonic()
    for model_index, model in enumerate(resolved["models"]):
        last_kind = "provider_unavailable"
        for attempt_index in range(int(reliability["attempts_per_model"])):
            attempts += 1
            request_error: Optional[OpenRouterRequestError] = None
            payload = {
                "model": model,
                "messages": [
                    {"role": "system", "content": system or defaults["system"]},
                    {"role": "user", "content": prompt},
                ],
                "reasoning": resolved["reasoning"],
                "temperature": chosen_temperature,
                "max_tokens": token_limit,
            }
            try:
                raw = client.chat(payload)
                message = ((raw.get("choices") or [{}])[0].get("message") or {})
                content = str(message.get("content") or "").strip()
                if content:
                    return {
                        "ok": True,
                        "profile_id": profile["profile_id"],
                        "requested_model": requested_model,
                        "effective_model": raw.get("model") or model,
                        "fallback_used": model_index > 0,
                        "content": content,
                        "usage": raw.get("usage") or {},
                        "attempts": attempts,
                        "failures": failures,
                        "elapsed_seconds": round(time.monotonic() - started, 3),
                    }
                last_kind = "empty_response"
                failure = {
                    "model": model,
                    "attempt": attempt_index + 1,
                    "kind": last_kind,
                    "error": "OpenRouter returned empty content",
                }
            except OpenRouterRequestError as exc:
                request_error = exc
                last_kind = _failure_kind(exc.status, str(exc))
                failure = {
                    "model": model,
                    "attempt": attempt_index + 1,
                    "kind": last_kind,
                    "error": str(exc),
                    "http_status": exc.status,
                }
            failures.append(failure)
            if (
                attempt_index + 1 >= int(reliability["attempts_per_model"])
                or not _should_retry(failure, reliability)
            ):
                break
            backoff = _backoff_seconds(reliability, attempt_index, request_error)
            time.sleep(backoff)
        if last_kind not in set(reliability.get("fallback_on") or []):
            break
    return {
        "ok": False,
        "profile_id": profile["profile_id"],
        "requested_model": requested_model,
        "effective_model": None,
        "fallback_used": len(resolved["models"]) > 1 and attempts > 1,
        "content": "",
        "usage": {},
        "attempts": attempts,
        "failures": failures,
        "elapsed_seconds": round(time.monotonic() - started, 3),
    }


def _resolve(profile: Dict[str, Any]) -> Dict[str, Any]:
    environment = profile["environment"]
    defaults = profile["defaults"]
    api_key_env = str(environment["api_key"])
    api_key = os.environ.get(api_key_env, "").strip()
    if not api_key:
        raise OpenRouterProfileError(f"missing required environment variable: {api_key_env}")
    base_url = os.environ.get(str(environment["base_url"]), defaults["base_url"]).strip()
    model = os.environ.get(str(environment["model"]), defaults["model"]).strip()
    models = [model]
    for fallback in defaults.get("fallback_models") or []:
        if fallback and fallback not in models:
            models.append(str(fallback))
    return {
        "api_key": api_key,
        "api_key_env": api_key_env,
        "base_url": base_url,
        "models": models,
        "reasoning": defaults["reasoning"],
        "timeout_seconds": int(profile["reliability"]["timeout_seconds"]),
    }


def _safe_error_message(detail: str) -> str:
    try:
        payload = json.loads(detail)
    except json.JSONDecodeError:
        return detail[:500]
    error = payload.get("error") or {}
    return str(error.get("message") or error.get("code") or detail[:500])


def _failure_kind(status: Optional[int], message: str) -> str:
    lowered = message.lower()
    if status == 403 and "key limit exceeded" in lowered:
        return "key_limit_exceeded"
    if status == 402:
        return "payment_required"
    if status == 429:
        return "rate_limited"
    if status is None or (status is not None and status >= 500):
        return "provider_unavailable"
    return "request_rejected"


def _should_retry(failure: Dict[str, Any], reliability: Dict[str, Any]) -> bool:
    if failure["kind"] == "empty_response":
        return True
    status = failure.get("http_status")
    return status in set(reliability.get("retry_http_statuses") or [])


def _backoff_seconds(
    reliability: Dict[str, Any], attempt_index: int, error: Optional[BaseException]
) -> float:
    if isinstance(error, OpenRouterRequestError) and error.retry_after is not None:
        return min(max(error.retry_after, 0.0), 30.0)
    values = reliability.get("backoff_seconds") or [0.5]
    return float(values[min(attempt_index, len(values) - 1)])


def _retry_after_seconds(value: Optional[str]) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(value)
    except ValueError:
        return None
