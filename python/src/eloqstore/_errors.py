from __future__ import annotations


STATUS_NAMES = {
    0: "Ok",
    1: "InvalidArgs",
    2: "NotFound",
    3: "NotRunning",
    4: "Corrupted",
    5: "EndOfFile",
    6: "OutOfSpace",
    7: "OutOfMem",
    8: "OpenFileLimit",
    9: "TryAgain",
    10: "Busy",
    11: "Timeout",
    12: "NoPermission",
    13: "CloudErr",
    14: "IoFail",
    15: "ExpiredTerm",
    16: "OssInsufficientStorage",
    17: "AlreadyExists",
}


class EloqStoreError(RuntimeError):
    def __init__(self, status: int, message: str):
        self.status = status
        self.status_name = STATUS_NAMES.get(status, f"Unknown({status})")
        super().__init__(f"{self.status_name}: {message}")

