import pickle

import numpy as np

from pathlib import Path
from collections import deque

def compute_stats(baseline, compared):

    count = len(compared)
    
    bff = np.empty(count)
    cff = np.empty(count)
    baf = np.empty(count)
    caf = np.empty(count)

    pdf = np.zeros(count)

    for i, request in enumerate(compared):
        
        bff[i] = baseline[request]['time_to_first_fail']
        cff[i] = compared[request]['time_to_first_fail']
        
        baf[i] = baseline[request]['time_to_last_fail']
        caf[i] = compared[request]['time_to_last_fail']

        brt = baseline[request]['response_times']
        crt = compared[request]['response_times']

        assert len(brt) == len(crt)

        pdf[i] = len([None for test in crt if brt[test] < crt[test]]) / len(crt)

    first = np.median(bff - cff) / np.median(bff)

    last = np.median(baf - caf) / np.median(baf)

    delayed = np.median(pdf)

    return first, last, delayed


def load(path):
    with open(path, 'rb') as file:
        return pickle.load(file)


def main():

    cwd = Path()

    baseline_out = 'test.out.16.fifo.pkl'
    compared_out = 'test.out.16.single.pkl'

    baseline = load(cwd / 'out' / baseline_out)

    compared = load(cwd / 'out' / compared_out)

    ff, lf, df = compute_stats(baseline, compared)

    print('time_to_first_fail', ff)
    print('time_to_last_fail', lf)
    print('percent_fail_delay', df)

if __name__ == '__main__':
    main()
