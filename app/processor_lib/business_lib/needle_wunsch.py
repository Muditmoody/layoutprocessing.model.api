import numpy as np
from app.Enum import Score, Trace
import numpy as np


class NeedleWunsch:



    @staticmethod
    def perform_alignment(seq1, seq2, penalty=-1, reward=1):

        def get_score(n1, n2, penalty=-1, reward=1):
            if n1 == n2:
                return reward
            else:
                return penalty


        # initialize score matrix
        score_matrix = np.ndarray((len(seq1) + 1, len(seq2) + 1))

        for i in range(len(seq1) + 1):
            score_matrix[i, 0] = penalty * i

        for j in range(len(seq2) + 1):
            score_matrix[0, j] = penalty * j

        # define each cell in the matrix by as the max score possible in that stage
        for i in range(1, len(seq1) + 1):
            for j in range(1, len(seq2) + 1):
                match = score_matrix[i - 1, j - 1] + get_score(seq1[i - 1], seq2[j - 1], penalty, reward)
                delete = score_matrix[i - 1, j] + penalty
                insert = score_matrix[i, j - 1] + penalty

                score_matrix[i, j] = max([match, delete, insert])

        i = len(seq1)
        j = len(seq2)

        align_seq1 = []
        align_seq2 = []

        while i > 0 or j > 0:

            current_score = score_matrix[i, j]
            left_score = score_matrix[i - 1, j]

            if i > 0 and j > 0 and seq1[i - 1] == seq2[j - 1]:
                align_seq1.insert(0, seq1[i - 1])
                align_seq2.insert(0, seq2[j - 1])
                i = i - 1
                j = j - 1

            elif i > 0 and current_score == left_score + penalty:
                align_seq1.insert(0, seq1[i - 1])
                align_seq2.insert(0, "-")
                i = i - 1

            else:
                align_seq1.insert(0, "-")
                align_seq2.insert(0, seq2[j - 1])
                j = j - 1

        return align_seq1, align_seq2
