from __future__ import annotations

from dataclasses import dataclass

EXIT_SUCCESS = 0
EXIT_USAGE = 2
EXIT_CONFIG = 10
EXIT_NOT_FOUND = 16
EXIT_NO_MATCH = 17
EXIT_OUTPUT = 20
EXIT_NETWORK = 25
EXIT_INTERNAL = 30


@dataclass
class RegMetaError(Exception):
    exit_code: int
    code: str
    error_class: str
    message: str
    remediation: str

    # The dataclass `__init__` never calls `Exception.__init__`, so `args` stays
    # empty; print the message (also after `_curation.located` prefixes it).
    def __str__(self) -> str:
        return self.message

    def to_dict(self) -> dict[str, str]:
        return {
            "code": self.code,
            "class": self.error_class,
            "message": self.message,
            "remediation": self.remediation,
        }


class SourceFormatError(RegMetaError, ValueError):
    """A selected delivery breaks one rule of its reader's format contract.

    The code names the rule and the message names the file and the place in it. It
    stays a ``ValueError``, so callers that catch a reader's failures as one still
    do, while `prepare-sources`, `prepare-input-bundle` and `parse-sos` report its
    own code instead of their generic wrapper code.
    """

    def __init__(self, message: str, *, code: str) -> None:
        super().__init__(
            exit_code=EXIT_CONFIG,
            code=code,
            error_class="configuration",
            message=message,
            remediation=(
                "Inspect the delivery at the place the message names. Restore the "
                "reviewed file if it changed by mistake; if the source format "
                "changed, repair its reader, then prepare a new candidate."
            ),
        )
