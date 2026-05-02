import asyncio
import logging

logger = logging.getLogger(__name__)

# Latency matrix in seconds
LATENCY_MATRIX = {
    "ap-jakarta": {
        "ap-jakarta": 0.002,
        "ap-singapore": 0.020,
        "ap-tokyo": 0.100,
    },
    "ap-singapore": {
        "ap-jakarta": 0.020,
        "ap-singapore": 0.002,
        "ap-tokyo": 0.070,
    },
    "ap-tokyo": {
        "ap-jakarta": 0.100,
        "ap-singapore": 0.070,
        "ap-tokyo": 0.002,
    }
}

DEFAULT_LATENCY = 0.050  # Default 50ms for unknown regions

def get_latency(source_region: str, dest_region: str) -> float:
    """Get the simulated latency in seconds between two regions."""
    if not source_region or not dest_region:
        return 0.0
        
    try:
        return LATENCY_MATRIX[source_region][dest_region]
    except KeyError:
        return DEFAULT_LATENCY

async def simulate_latency(source_region: str, dest_region: str):
    """Asynchronously sleep to simulate network latency."""
    latency = get_latency(source_region, dest_region)
    if latency > 0:
        logger.debug(f"Simulating network latency: {latency}s from {source_region} to {dest_region}")
        await asyncio.sleep(latency)
