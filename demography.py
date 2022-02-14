#!/usr/bin/env python3
import pickle

NUM_STATES = 100
NUM_SEX = 2
NUM_RACE = 7

with open('demography.pkl', 'rb') as f:
    lookup = pickle.load(f)
    pass

DEFAULT = [0] * 9
DEFAULT[0] = 99

def get (pid, onehot = True):
    ft = lookup.get(pid, [0, DEFAULT])[1]
    if onehot:
        state = [0] * NUM_STATES
        sex = [0] * NUM_SEX
        race = [0] * NUM_RACE
        state[ft[0]] = 1
        sex[ft[1]] = 1
        race[ft[2]] = 1
        return sex + race + state + ft[3:]
    return ft
    pass


if __name__ == '__main__':
    print(len(lookup))
    print(get(100024915, onehot=False))
    print(get(100024915, onehot=True))

