from enum import IntEnum

class Score(IntEnum):
    """Enum for Scoring types

    Args:
        Enum (_type_): int
    """
    MATCH = 1
    MISMATCH = -1
    GAP = -1
