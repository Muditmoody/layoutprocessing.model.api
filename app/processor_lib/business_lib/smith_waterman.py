import numpy as np
from app.Enum import Score, Trace

class SmithWaterman:

    @staticmethod
    def perform_alignment(seq1, seq2):
        # Generating the empty matrices for storing scores and tracing
        row = len(seq1) + 1
        col = len(seq2) + 1
        matrix = np.zeros(shape=(row, col), dtype=int)
        tracing_matrix = np.zeros(shape=(row, col), dtype=int)

        # Initialising the variables to find the highest scoring cell
        max_score = -1  ## Records the max level/ number of common elements in the sequences
        max_index = (-1,
                     -1)  ## Recorsd the index where the max value is found. So we could us that to trace back the matches following the trace back matrix

        # Calculating the scores for all cells in the matrix
        for i in range(1, row):
            for j in range(1, col):
                # Calculating the diagonal score (match score)
                match_value = Score.Score.MATCH if seq1[i - 1] == seq2[j - 1] else Score.Score.MISMATCH
                diagonal_score = matrix[i - 1, j - 1] + match_value
                ## Tracks the value of the cell if it is a match and if we can move to the next element of the sequences (and start tracking for it in the next row and colu,m)

                # Calculating the vertical gap score
                vertical_score = matrix[i - 1, j] + Score.Score.GAP
                ## Vertical score implies at the value from the previous row and column above

                # Calculating the horizontal gap score
                horizontal_score = matrix[i, j - 1] + Score.Score.GAP
                ## Horizontal score imploes for the value from previous column and same row

                # Taking the highest score
                matrix[i, j] = max(0, diagonal_score, vertical_score, horizontal_score)
                ## We take the max of all scores and if we have a match, we would essentially be increasing the match score and moving towars success.
                ## if we have a penalty (or either of the vertical and horizontal score, we basically replace it with zeros)

                # Tracking where the cell's value is coming from
                if matrix[i, j] == 0:
                    tracing_matrix[i, j] = Trace.Trace.STOP

                elif matrix[i, j] == horizontal_score:
                    tracing_matrix[i, j] = Trace.Trace.LEFT

                elif matrix[i, j] == vertical_score:
                    tracing_matrix[i, j] = Trace.Trace.UP

                elif matrix[i, j] == diagonal_score:
                    tracing_matrix[i, j] = Trace.Trace.DIAGONAL

                    # Tracking the cell with the maximum score
                if matrix[i, j] >= max_score:
                    max_index = (i, j)
                    max_score = matrix[i, j]

        # Initialising the variables for tracing
        aligned_seq1 = []
        aligned_seq2 = []
        current_aligned_seq1 = []
        current_aligned_seq2 = []
        (max_i, max_j) = max_index

        # Tracing and computing the pathway with the local alignment
        while tracing_matrix[max_i, max_j] != Trace.Trace.STOP:
            if tracing_matrix[max_i, max_j] == Trace.Trace.DIAGONAL:
                current_aligned_seq1 = seq1[max_i - 1]
                current_aligned_seq2 = seq2[max_j - 1]
                max_i = max_i - 1
                max_j = max_j - 1

            elif tracing_matrix[max_i, max_j] == Trace.Trace.UP:
                current_aligned_seq1 = seq1[max_i - 1]
                current_aligned_seq2 = '-'
                max_i = max_i - 1

            elif tracing_matrix[max_i, max_j] == Trace.Trace.LEFT:
                current_aligned_seq1 = '-'
                current_aligned_seq2 = seq2[max_j - 1]
                max_j = max_j - 1

            if len(current_aligned_seq1) > 0:
                aligned_seq1.append(current_aligned_seq1)
            if len(current_aligned_seq2) > 0:
                aligned_seq2.append(current_aligned_seq2)

        # Reversing the order of the sequences
        aligned_seq1 = aligned_seq1[::-1]
        aligned_seq2 = aligned_seq2[::-1]
        if len(aligned_seq2) < 2:
            score = 0
        else:
            # score = (len(aligned_seq2)) / len(seq2)
            score = (len(aligned_seq2) - aligned_seq2.count('-')) / len(seq2)

        return aligned_seq1, aligned_seq2, score
