import logging
from functools import wraps

logger = logging.getLogger(__name__)


class ContextualError(Exception):
    """Raised to add context to an underlying exception."""

    def __init__(self, message, original_exc=None):
        super().__init__(message)
        self.original_exc = original_exc


def handle_exception_wrap(message_template='Error in {func_name}'):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                msg = message_template.format(func_name=func.__name__)
                # Optional: lightweight log with context (no full traceback)
                logger.debug('%s: %s', msg, e)
                # Wrap exception to add context and preserve traceback
                raise ContextualError(msg, original_exc=e) from e

        return wrapper

    return decorator
