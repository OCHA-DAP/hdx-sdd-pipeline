import logging
from functools import wraps

logger = logging.getLogger(__name__)


def handle_exception(func):
    """
    Decorator for handling exceptions in functions or class methods.
    """

    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            logger.error('Exception in %s: %s', func.__name__, e)
            raise e

    return wrapper
