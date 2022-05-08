#!/bin/python

import json
import sys


def parseRow(row: list) -> list:
    def strToBool(value: str) -> str:
        if(value == 'Y'):
            return 'true'
        return 'false'

    out = row.copy()
    out[10] = strToBool(out[10])
    out[11] = strToBool(out[11])

    return out 
    
def main(filein, fileout):
    batch = []
    count = 1

    with open(filein, 'r', encoding='UTF-8') as inp , open(fileout, 'w') as out:
        while (row := inp.readline().strip().split('\t')):
            if (not len(row) - 1):
                return
            parsed = parseRow(row)
            out.write('\t'.join(parsed)+'\n')


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2])
