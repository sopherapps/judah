"""Module With utilities for testing"""

import signal


class TestingTimeoutError(Exception):
    """Exception to be raised when a test has gone past its timeout"""
    pass


class TimeoutManager:
    """
    This provides a context manager to wrap around code
    that is to timeout if it takes longer than a given amount of time
    """

    def __init__(self, seconds, error_message=None):
        if error_message is None:
            error_message = f'Timeout after {seconds}s.'
        self.seconds = seconds
        self.error_message = error_message

    def handle_timeout(self, signum, frame):
        raise TestingTimeoutError(self.error_message)

    def __enter__(self):
        signal.signal(signal.SIGALRM, self.handle_timeout)
        signal.alarm(self.seconds)

    def __exit__(self, exc_type, exc_val, exc_tb):
        signal.alarm(0)
