from functools import lru_cache
import time

import sys
sys.setrecursionlimit(150000)

# Fibonacci with caching
@lru_cache(maxsize=3)
def fibonacci_cached(n):
    if n < 2:
        return n
    return fibonacci_cached(n-1) + fibonacci_cached(n-2)

# n = 5
# Fibonacci without caching
def fibonacci_plain(n):
    if n < 2:
        return n
    return fibonacci_plain(n-1) + fibonacci_plain(n-2)

# Timing function
def time_fibonacci(fib_func, n):
    start_time = time.time()
    result = fib_func(n)
    end_time = time.time()
    return result, end_time - start_time

# Number to compute
n = 1000

# Timing Fibonacci with and without cache
result_cached, time_cached = time_fibonacci(fibonacci_cached, n)
# result_plain, time_plain = time_fibonacci(fibonacci_plain, n)

print(f"Fibonacci with lru_cache: Result = {result_cached}, Time = {time_cached:.6f} seconds")
# print(f"Fibonacci without cache: Result = {result_plain}, Time = {time_plain:.6f} seconds")



