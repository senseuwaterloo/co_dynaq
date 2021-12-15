import math


def co_fail(s2, s1):

    for t2, f2, p2 in s2:

        a2 = f2 | p2
        
        for t1, f1, p1 in s1:

            f1a2 = len(f1 & a2)
            ff = len(f2 & f1) / f1a2 if f1a2 > 0 else 0.5

            p1a2 = len(p1 & a2)
            fp = len(f2 & p1) / p1a2 if p1a2 > 0 else 0.5

            yield t2, t1, ff, fp
