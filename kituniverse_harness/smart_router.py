from __future__ import annotations

import asyncio
import concurrent.futures
from typing import Any, Dict, List, Optional

from .providers import LMStudioProvider, ProviderResponse


class SmartRoutingService:
    def __init__(
        self,
        base_url: str,
        model: str,
        timeout_seconds: int,
        max_predictions: int = 128,
        max_context_tokens: int = 100,
    ) -> None:
        if max_predictions < 1:
            raise ValueError("max_predictions must be at least 1")
        if max_context_tokens < 16:
            raise ValueError("max_context_tokens must be at least 16")
        self.base_url = base_url
        self.model = model
        self.timeout_seconds = timeout_seconds
        self.max_predictions = max_predictions
        self.max_context_tokens = max_context_tokens
        self._semaphore = asyncio.Semaphore(max_predictions)
        self._executor = concurrent.futures.ThreadPoolExecutor(max_workers=max_predictions)
        self._calls_started = 0
        self._calls_completed = 0
        self._active_predictions = 0
        self._peak_active_predictions = 0

    async def chat(
        self,
        messages: List[Dict[str, str]],
        temperature: float,
        max_tokens: int,
        retries: int = 0,
        response_format: Optional[Dict[str, Any]] = None,
    ) -> tuple[ProviderResponse, int]:
        routed_messages = trim_messages(messages, self.max_context_tokens)
        response: Optional[ProviderResponse] = None
        attempts = 0
        for attempt in range(retries + 1):
            attempts = attempt + 1
            async with self._semaphore:
                self._calls_started += 1
                self._active_predictions += 1
                self._peak_active_predictions = max(
                    self._peak_active_predictions,
                    self._active_predictions,
                )
                loop = asyncio.get_running_loop()
                provider = LMStudioProvider(
                    base_url=self.base_url,
                    model=self.model,
                    timeout_seconds=self.timeout_seconds,
                )
                try:
                    response = await loop.run_in_executor(
                        self._executor,
                        lambda: provider.chat(
                            messages=routed_messages,
                            temperature=temperature,
                            max_tokens=max_tokens,
                            response_format=response_format,
                        ),
                    )
                    self._calls_completed += 1
                finally:
                    self._active_predictions -= 1
            if response.ok and response.content.strip():
                return response, attempts
            await asyncio.sleep(min(0.05 * (attempt + 1), 0.5))
        if response is None:
            raise RuntimeError("router completed without provider response")
        return response, attempts

    def stats(self) -> Dict[str, Any]:
        return {
            "max_predictions": self.max_predictions,
            "max_context_tokens": self.max_context_tokens,
            "calls_started": self._calls_started,
            "calls_completed": self._calls_completed,
            "active_predictions": self._active_predictions,
            "peak_active_predictions": self._peak_active_predictions,
        }

    def shutdown(self) -> None:
        self._executor.shutdown(wait=True)


def trim_messages(
    messages: List[Dict[str, str]],
    max_context_tokens: int,
) -> List[Dict[str, str]]:
    remaining = max_context_tokens
    routed: List[Dict[str, str]] = []
    for message in reversed(messages):
        content = str(message.get("content", ""))
        words = content.split()
        if remaining <= 0:
            trimmed = ""
        elif len(words) > remaining:
            trimmed = " ".join(words[-remaining:])
            remaining = 0
        else:
            trimmed = content
            remaining -= len(words)
        routed.append({**message, "content": trimmed})
    return list(reversed(routed))
