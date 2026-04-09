import datetime


def get_current_utc_time():
    """Returns the current date and time in UTC."""
    return datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')


def add_numbers(a, b):
    """Returns the sum of two numbers."""
    return a + b


def subtract_numbers(a, b):
    """Returns the difference of two numbers."""
    return a - b


def multiply_numbers(a, b):
    """Returns the product of two numbers."""
    return a * b


def divide_numbers(a, b):
    """Returns the quotient of two numbers. Raises ValueError if dividing by zero."""
    if b == 0:
        raise ValueError('Cannot divide by zero')
    return a / b