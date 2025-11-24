import logging
import time
from typing import Dict, Optional
from collections import defaultdict
from threading import Lock
import sys


class MetricsCollector:    
    def __init__(self):
        self._metrics: Dict[str, int] = defaultdict(int)
        self._timings: Dict[str, list] = defaultdict(list)
        self._lock = Lock()
    
    def increment(self, metric_name: str, value: int = 1):
        with self._lock:
            self._metrics[metric_name] += value
    
    def record_timing(self, metric_name: str, duration_ms: float):
        with self._lock:
            self._timings[metric_name].append(duration_ms)
            # Keep only last 1000 timings to prevent memory bloat
            if len(self._timings[metric_name]) > 1000:
                self._timings[metric_name] = self._timings[metric_name][-1000:]
    
    def get_metrics(self) -> dict:
        with self._lock:
            metrics = dict(self._metrics)
            timings = {}
            for key, values in self._timings.items():
                if values:
                    timings[key] = {
                        'count': len(values),
                        'avg_ms': sum(values) / len(values),
                        'min_ms': min(values),
                        'max_ms': max(values),
                    }
            return {'counters': metrics, 'timings': timings}
    
    def reset(self):
        with self._lock:
            self._metrics.clear()
            self._timings.clear()
    
    def print_summary(self):
        metrics = self.get_metrics()
        print("\n" + "="*50)
        print("METRICS SUMMARY")
        print("="*50)
        
        if metrics['counters']:
            print("\nCounters:")
            for name, value in sorted(metrics['counters'].items()):
                print(f"  {name}: {value}")
        
        if metrics['timings']:
            print("\nTimings:")
            for name, stats in sorted(metrics['timings'].items()):
                print(f"  {name}:")
                print(f"    count: {stats['count']}")
                print(f"    avg: {stats['avg_ms']:.2f}ms")
                print(f"    min: {stats['min_ms']:.2f}ms")
                print(f"    max: {stats['max_ms']:.2f}ms")
        print("="*50 + "\n")


class Timer:
    def __init__(self, metrics: Optional[MetricsCollector] = None, metric_name: str = "operation"):
        self.metrics = metrics
        self.metric_name = metric_name
        self.start_time = None
        self.duration_ms = None
    
    def __enter__(self):
        self.start_time = time.time()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.duration_ms = (time.time() - self.start_time) * 1000
        if self.metrics:
            self.metrics.record_timing(self.metric_name, self.duration_ms)


def setup_logger(name: str, level: str = "INFO") -> logging.Logger:
    """
    Args:
        name: Logger name
        level: Logging level
    
    Returns:
        Configured logger instance
    """
    logger = logging.getLogger(name)
    logger.setLevel(getattr(logging, level))
    
    # Remove existing handlers to avoid duplicates
    logger.handlers.clear()
    
    # Console handler with formatting
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(getattr(logging, level))
    
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    handler.setFormatter(formatter)
    
    logger.addHandler(handler)
    logger.propagate = False
    
    return logger


def exponential_backoff(retry_count: int, base_ms: int = 1000, max_ms: int = 30000) -> float:
    # Calculate exponential backoff delay
    delay_ms = min(base_ms * (2 ** retry_count), max_ms)
    return delay_ms / 1000.0
