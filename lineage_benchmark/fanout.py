import duckdb
import pandas as pd
import argparse
import csv
import numpy as np

from utils import getStats, MicroDataZipfan, MicroDataSelective, DropLineageTables, MicroDataMcopies,  Run

def PersistResults(results, filename, append):
    print("Writing results to ", filename, " Append: ", append)
    header = ["query", "runtime", "cardinality", "groups", "output", "stats", "lineage_type", "notes"]
    control = 'w'
    if append:
        control = 'a'
    with open(filename, control) as csvfile:
        csvwriter = csv.writer(csvfile)
        csvwriter.writerow(header)
        csvwriter.writerows(results)

