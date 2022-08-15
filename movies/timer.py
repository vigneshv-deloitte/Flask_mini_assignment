from functools import wraps
import time


def timer(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        start_timer = time.time()
        response = func(*args, **kwargs)
        end_timer = time.time()
        elapsed_time = end_timer - start_timer
        print(elapsed_time)
        return response

    return wrapper
