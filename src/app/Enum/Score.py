from enum import IntEnum

class Score(IntEnum):
    MATCH = 1
    MISMATCH = -1
    GAP = -1
