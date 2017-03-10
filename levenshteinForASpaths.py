# -*- coding: utf-8 -*-
"""
Created on Fri Mar 10 15:49:29 2017

From https://en.wikibooks.org/wiki/Algorithm_Implementation/Strings/Levenshtein_distance#Python

"""
import numpy as np

def levenshtein(ASpath1, ASpath2):
    if len(ASpath1) < len(ASpath2):
        return levenshtein(ASpath2, ASpath1)

    # So now we have len(ASpath1) >= len(ASpath2).
    if len(ASpath2) == 0:
        return len(ASpath1)

    # We call tuple() to force strings to be used as sequences
    # ('c', 'a', 't', 's') - numpy uses them as values by default.
    ASpath1 = np.array(tuple(ASpath1))
    ASpath2 = np.array(tuple(ASpath2))

    # We use a dynamic programming algorithm, but with the
    # added optimization that we only need the last two rows
    # of the matrix.
    previous_row = np.arange(ASpath2.size + 1)
    for s in ASpath1:
        # Insertion (ASpath2 grows longer than ASpath1):
        current_row = previous_row + 1

        # Substitution or matching:
        # ASpath2 and ASpath1 items are aligned, and either
        # are different (cost of 1), or are the same (cost of 0).
        current_row[1:] = np.minimum(
                current_row[1:],
                np.add(previous_row[:-1], ASpath2 != s))

        # Deletion (target grows shorter than source):
        current_row[1:] = np.minimum(
                current_row[1:],
                current_row[0:-1] + 1)

        previous_row = current_row

    return previous_row[-1]
    
    
ASpath1 = ['2800', '174', '345']
ASpath2 = ['665', '2800', '174', '345']
levenshtein(ASpath1, ASpath2)