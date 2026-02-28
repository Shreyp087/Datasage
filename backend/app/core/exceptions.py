class LLMError(Exception):
    def __init__(self, message: str, provider: str, retries: int):
        self.provider = provider
        self.retries = retries
        super().__init__(message)

class LLMParseError(Exception):
    def __init__(self, message: str, raw_response: str):
        self.raw_response = raw_response
        super().__init__(message)
