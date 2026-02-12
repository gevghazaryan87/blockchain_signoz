"""Provider pool for managing multiple blockchain data sources."""
import time
import threading
import itertools
from extraction.base import BlockchainProvider


class ProviderPool:
    """Manages a pool of providers with round-robin selection and rate-limit handling."""

    def __init__(self, providers: list[BlockchainProvider]):
        self.providers = providers
        self._cycle = itertools.cycle(providers)
        self._lock = threading.Lock()
        
        # Track when a provider can be used again (unix timestamp)
        # Dictionary mapping provider.name -> cooldown_until_timestamp
        self._cooldowns = {p.name: 0 for p in providers}

    def get_next_provider(self) -> BlockchainProvider:
        """Get the next available provider in round-robin fashion."""
        with self._lock:
            # Try to find a provider that isn't in cooldown
            # We iterate through the cycle up to len(providers) times
            for _ in range(len(self.providers)):
                provider = next(self._cycle)
                if time.time() >= self._cooldowns[provider.name]:
                    return provider
            
            # If all providers are in cooldown, just return the next one
            # and let the caller handle the potential 429
            return next(self._cycle)

    def report_rate_limit(self, provider_name: str, retry_after: int = 60):
        """Report that a provider hit a rate limit."""
        with self._lock:
            self._cooldowns[provider_name] = time.time() + retry_after
            print(f"\n   ⚠️ Provider '{provider_name}' rate-limited or failed. Pausing for {retry_after}s.")

    def get_all_providers(self) -> list[BlockchainProvider]:
        return self.providers
