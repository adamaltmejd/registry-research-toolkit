from __future__ import annotations

from dataclasses import dataclass

EXIT_SUCCESS = 0
EXIT_USAGE = 2
EXIT_CONFIG = 10
EXIT_NOT_FOUND = 16
EXIT_NO_MATCH = 17
EXIT_OUTPUT = 20
EXIT_INTERNAL = 30


@dataclass
class RegmetaError(Exception):
    exit_code: int
    code: str
    error_class: str
    message: str
    remediation: str

    def to_dict(self) -> dict[str, str]:
        return {
            "code": self.code,
            "class": self.error_class,
            "message": self.message,
            "remediation": self.remediation,
        }
