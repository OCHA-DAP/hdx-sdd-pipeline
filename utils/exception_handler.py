import logging

logger = logging.getLogger(__name__)

def handle_exception(func):
    """
    Decorator for handling exceptions in functions or class methods.
    """
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            logger.error(f'Exception in {func.__name__}: {e}')
            raise e
    return wrapper
