from enum import IntEnum

class Trace(IntEnum):
    """Enum for Trace types

    Args:
        Enum (_type_): int
    """
    STOP = 0  ## STOP  implies that we have reached the end of the match sequences.
    LEFT = 1  ## LEFT implies that we stay in the same row and move one column left for trace back
    UP = 2  ## UP implies that we stay in the same column and move one row up for trace back
    DIAGONAL = 3  ## Diagnol implies that the value in seq 2 can be substituted by the one in seq1 i.e. it is a match and we can move move to the diagnol element or next for analysis
