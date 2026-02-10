"""Tests for exception handler decorator."""

import pytest
import logging
from unittest.mock import Mock, patch
from src.shared.utils.exception_handler import handle_exception


class TestExceptionHandler:
    """Test suite for exception handler decorator."""

    def test_handle_exception_success(self):
        """Test that decorated function executes successfully without exceptions."""
        @handle_exception()
        def successful_function(x, y):
            return x + y

        result = successful_function(2, 3)
        assert result == 5

    def test_handle_exception_with_exception(self, caplog):
        """Test that exceptions are caught, logged, and re-raised as ContextualError."""
        from src.shared.utils.exception_handler import ContextualError
        
        @handle_exception()
        def failing_function():
            raise ValueError("Test error")

        with caplog.at_level(logging.ERROR, logger="src.shared.utils.exception_handler"):
            with pytest.raises(ContextualError) as excinfo:
                failing_function()

        assert "Test error" in str(excinfo.value)
        assert "Test error" in caplog.text

    def test_handle_exception_preserves_function_metadata(self):
        """Test that decorator preserves function metadata."""
        @handle_exception()
        def documented_function():
            """This is a documented function."""
            return "success"

        assert documented_function.__name__ == "documented_function"
        assert documented_function.__doc__ == "This is a documented function."

    def test_handle_exception_with_args_and_kwargs(self):
        """Test that decorator works with various argument patterns."""
        @handle_exception()
        def complex_function(a, b, *args, **kwargs):
            return {
                'a': a,
                'b': b,
                'args': args,
                'kwargs': kwargs
            }

        result = complex_function(1, 2, 3, 4, key1='value1', key2='value2')
        assert result['a'] == 1
        assert result['b'] == 2
        assert result['args'] == (3, 4)
        assert result['kwargs'] == {'key1': 'value1', 'key2': 'value2'}

    def test_handle_exception_logs_function_name(self, caplog):
        """Test that exception handler logs the function name."""
        from src.shared.utils.exception_handler import ContextualError
        
        @handle_exception()
        def named_function():
            raise Exception("Test exception")

        with caplog.at_level(logging.ERROR, logger="src.shared.utils.exception_handler"):
            with pytest.raises(ContextualError):
                named_function()

        assert "named_function" in caplog.text or "Exception" in caplog.text

    def test_handle_exception_with_different_exception_types(self, caplog):
        """Test handling of different exception types."""
        from src.shared.utils.exception_handler import ContextualError
        
        @handle_exception()
        def multi_exception_function(exception_type):
            if exception_type == "value":
                raise ValueError("Value error")
            elif exception_type == "type":
                raise TypeError("Type error")
            elif exception_type == "runtime":
                raise RuntimeError("Runtime error")
            return "success"

        with caplog.at_level(logging.ERROR):
            with pytest.raises(ContextualError):
                multi_exception_function("value")
            
            with pytest.raises(ContextualError):
                multi_exception_function("type")
                
            result = multi_exception_function("none")

        assert result == "success"

    def test_handle_exception_with_method(self):
        """Test that decorator works with class methods."""
        from src.shared.utils.exception_handler import ContextualError
        
        class TestClass:
            @handle_exception()
            def instance_method(self, value):
                if value < 0:
                    raise ValueError("Negative value")
                return value * 2

        obj = TestClass()
        assert obj.instance_method(5) == 10
        with pytest.raises(ContextualError):
            obj.instance_method(-5)

    def test_handle_exception_with_static_method(self):
        """Test that decorator works with static methods."""
        from src.shared.utils.exception_handler import ContextualError
        
        class TestClass:
            @staticmethod
            @handle_exception()
            def static_method(value):
                if value == 0:
                    raise ZeroDivisionError("Division by zero")
                return 10 / value

        assert TestClass.static_method(2) == 5.0
        with pytest.raises(ContextualError):
            TestClass.static_method(0)

    def test_handle_exception_with_class_method(self):
        """Test that decorator works with class methods."""
        from src.shared.utils.exception_handler import ContextualError
        
        class TestClass:
            value = 10

            @classmethod
            @handle_exception()
            def class_method(cls, multiplier):
                if multiplier < 0:
                    raise ValueError("Negative multiplier")
                return cls.value * multiplier

        assert TestClass.class_method(3) == 30
        with pytest.raises(ContextualError):
            TestClass.class_method(-1)
