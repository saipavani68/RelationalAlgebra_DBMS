#!/usr/bin/env python3

import pprint
import collections

from Chinook_Python import *


def project(relation, columns):
    finalResult = []
    for t in relation:
        result = []
        for c in columns:
            result.append(getattr(t,c))
        finalResult.append(result)
    return finalResult

def select(relation, predicate):
    pass


def rename(relation, new_columns=None, new_relation=None):
    pass


def cross(relation1, relation2):
    pass


def theta_join(relation1, relation2, predicate):
    pass


def natural_join(relation1, relation2):
    pass


pprint.pprint(
    project(Album,
        ['AlbumId','Title']
    )
)
