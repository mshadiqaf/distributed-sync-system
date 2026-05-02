import pytest
from src.geo.latency import get_latency, LATENCY_MATRIX, DEFAULT_LATENCY

def test_get_latency_known_regions():
    """Test latency between known regions."""
    assert get_latency("ap-jakarta", "ap-singapore") == 0.020
    assert get_latency("ap-tokyo", "ap-jakarta") == 0.100
    assert get_latency("ap-singapore", "ap-singapore") == 0.002

def test_get_latency_unknown_regions():
    """Test fallback to default latency for unknown regions."""
    assert get_latency("unknown-1", "ap-jakarta") == DEFAULT_LATENCY
    assert get_latency("ap-tokyo", "unknown-2") == DEFAULT_LATENCY
    assert get_latency("foo", "bar") == DEFAULT_LATENCY

def test_get_latency_empty_regions():
    """Test handling of empty region strings."""
    assert get_latency("", "ap-jakarta") == 0.0
    assert get_latency("ap-jakarta", None) == 0.0
    assert get_latency(None, None) == 0.0
