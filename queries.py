#!/usr/bin/env python3

import pprint
import collections

from Chinook_Python import *


def project(relation, columns):
    Result = collections.namedtuple('Result', columns)
    finalResult = []
    projectedColumns= set()
    for t in relation:
        result = []
        for c in columns:
            result.append(getattr(t,c))
        finalResult.append(result)
    for f in finalResult:
        projectedColumns.add(Result(*f))
    print('Total number of rows returned from project:', len(projectedColumns))
    if len(projectedColumns) ==0:
        return 'No rows returned'
    else:
        return projectedColumns

def select(relation, predicate):
    Result = collections.namedtuple('Result', next(iter(relation))._fields)
    SelectResult = set()
    Select = set()
    for t in relation:
        if predicate(t):
            SelectResult.add(t)
    for s in SelectResult:
        Select.add(Result(*s))
    print('Total number of rows returned from select:', len(Select))
    return Select

def rename(relation, new_columns=None, new_relation=None):
    finalResult = set()
    projectedColumns= set()
    Result = collections.namedtuple('relation', new_columns, rename=True)
    for t in relation:
        finalResult.add(t)
    for f in finalResult:
        projectedColumns.add(Result(*f))
    return projectedColumns


def cross(relation1, relation2):
    Result = collections.namedtuple('Result', next(iter(relation1))._fields + next(iter(relation2))._fields, rename = True)
    CrossResult = set()
    CrossJoin = set()
    for t1 in relation1:
        for t2 in relation2:
              CrossResult.add(t1+t2)
    for c in CrossResult:
        CrossJoin.add(Result(*c))
    print('Total number of rows returned from cross join:', len(CrossResult))
    return CrossJoin

def theta_join(relation1, relation2, predicate):
    Result = collections.namedtuple('Result', next(iter(relation1))._fields + next(iter(relation2))._fields, rename = True)
    Thetajoin = set()
    ThetaJoinResult = set()
    for t1 in relation1:
        for t2 in relation2:
            if predicate(t1,t2):
                Thetajoin.add(t1+t2)
    for t in Thetajoin:
        ThetaJoinResult.add(Result(*t))
    print('Total number of rows returned from theta join:', len(ThetaJoinResult))
    return ThetaJoinResult

def natural_join(relation1, relation2):
    NaturaljoinResult = []
    NaturalJoin = []
    cols1 = set(next(iter(relation1))._fields)
    cols2 = set(next(iter(relation2))._fields)
    merge_cols = cols1 & cols2
    diff = set(cols2 - merge_cols)
    finalcols=cols1|cols2

    Result = collections.namedtuple('Result',finalcols)

    t1map = {k: i for i, k in enumerate(next(iter(relation1))._fields)}
    t2map = {k: i for i, k in enumerate(next(iter(relation2))._fields)}
    for t1 in relation1:
        for t2 in relation2:
            row=list()
            if (all(t1[t1map[m]] == t2[t2map[m]] for m in merge_cols)):
                for col in finalcols:
                    if col in cols1:
                        row.append(t1[t1map[col]])
                    if col in diff:
                        row.append(t2[t2map[col]])
                NaturalJoin.append(row)
    for n in NaturalJoin:
        NaturaljoinResult.append(Result(*n))
    print('Total number of rows returned from natural join:', len(NaturaljoinResult))
    return NaturaljoinResult


pprint.pprint(
    project(
        select(
            select(
                cross(
                    Album,
                    rename(Artist, ['Id', 'Name'])
                ),
                lambda t: t.ArtistId == t.Id
            ),
            lambda t: t.Name == 'Red Hot Chili Peppers'
        ),
        ['Title']
    ),

)

pprint.pprint(
    project(
        select(
            theta_join(
                Album,
                rename(Artist, ['Id', 'Name']),
                lambda t1, t2: t1.ArtistId == t2.Id
            ),
            lambda t: t.Name == 'Red Hot Chili Peppers'
        ),
        ['Title']
    )
)

pprint.pprint(
    project(
        theta_join(
            Album,
            rename(
                select(Artist, lambda t: t.Name == 'Red Hot Chili Peppers'),
                ['Id', 'Name']
            ),
            lambda t1, t2: t1.ArtistId == t2.Id
        ),
        ['Title']
    )
)

pprint.pprint(
    project(
        natural_join(
            Album,
            select(Artist, lambda t: t.Name == 'Red Hot Chili Peppers')
        ),
        ['Title']
    )
)