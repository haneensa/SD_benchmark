# Benchmark lineage systems with increasingly complex queries

import duckdb
import pandas as pd
import numpy as np
import argparse
import csv
import os.path

from utils import execute, ZipfanGenerator, DropLineageTables, Run, getStats, MicroDataZipfan

parser = argparse.ArgumentParser(description='Micro Benchmark: Physical Join Operators')
parser.add_argument('notes', type=str,  help="run notes")
parser.add_argument('--save_csv', action='store_true',  help="save result in csv")
parser.add_argument('--csv_append', action='store_true',  help="Append results to old csv")
parser.add_argument('--repeat', type=int, help="Repeat time for each query", default=5)
parser.add_argument('--show_output', action='store_true',  help="query output")
parser.add_argument('--profile', action='store_true',  help="Enable profiling")
parser.add_argument('--enable_lineage', action='store_true',  help="Enable trace_lineage")
parser.add_argument('--stats', action='store_true',  help="get lineage size, nchunks and postprocess time")
parser.add_argument('--persist', action='store_true',  help="Lineage persist")
parser.add_argument('--perm', action='store_true',  help="run perm lineage queries")
args = parser.parse_args()
if args.enable_lineage and args.perm:
    args.perm = False

old_profile_val = args.profile
results = []

if args.enable_lineage and args.persist:
    lineage_type = "SD_Persist"
elif args.enable_lineage:
    lineage_type = "SD_Capture"
elif args.perm:
    lineage_type = "Logical_RID"
else:
    lineage_type = "Baseline"

con = duckdb.connect(database=':memory:', read_only=False)

def SmokedDuck(q, q_lineage, level):
    args.enable_lineage = True
    args.profile=old_profile_val
    avg, output_size = Run(q, args, con)
    stats = ""
    if args.stats:
        lineage_size, nchunks, postprocess_time= getStats(con, q)
        stats = "{},{},{}".format(lineage_size, nchunks, postprocess_time*1000)
    if args.persist:
        args.enable_lineage = False
        args.profile=False
        results.append(["SD_Persist", "level2", avg, output_size, card, N[0], N[1], N[2], N[3], args.notes])
        query_id = con.execute("select max(query_id) as qid from queries_list").fetchdf().loc[0, 'qid'] - 1
        avg, output_size = Run(q_lineage.format(query_id), args, con, "lineage")
        output_size = con.execute("select count(*) as c from lineage").fetchdf().loc[0, 'c']
        results.append(["SD_Query", level, avg, output_size, card, N[0], N[1], N[2], N[3], stats, args.notes])
        DropLineageTables(con)
        con.execute("DROP TABLE lineage");
    else:
        results.append(["SD_Capture", level, avg, -1, card, N[0], N[1], N[2], N[3], stats, args.notes])
################### Check data exists if not, then generate data
folder = "benchmark_data/"
cardinality = [100000]
N_list = [(1000, 100, 10, 1)]
a = 1

for N in N_list:
    for card in cardinality:
        card = [100000]
        a = [1]
        groups = [1]
        MicroDataZipfan(folder, groups, card, 100, a)
        filename = "zipfan_g"+str(groups[0])+"_card"+str(card[0])+"_a1.csv"
        zipf1 = pd.read_csv(folder+filename)

        # initialize table
        con.register('zipf1_view', zipf1)
        con.execute("create table zipf1 as select * from zipf1_view")

        level1 = "select z, sum(v) from zipf1 group by z"
        if args.perm:
            ################## level 1 #####################
            level1_lineage = """create table lineage as (
                select zipf1.rowid as zipf1_rowid from zipf1 join (
                select z, sum(v) from zipf1 group by z) using (z)
            )"""
            avg, output_size = Run(level1_lineage, args, con, "lineage")
            output_size = con.execute("select count(*) as c from lineage").fetchdf().loc[0, 'c']
            results.append(["Logical_RID", "level1", avg, output_size, card, N[0], N[1], N[2], N[3], args.notes])
            con.execute("DROP TABLE lineage");
        else:
            baseline_avg, df = Run(level1, args, con)
            results.append(["Baseline", "level1", baseline_avg, len(df), card, N[0], N[1], N[2], N[3], args.notes])

        con.execute("drop table t1")


########### Write results to CSV
if args.save_csv:
    filename="nested_agg_notes_"+args.notes+"_lineage_type_"+lineage_type+".csv"
    print(filename)
    header = ["lineage_type", "level", "runtime", "output_size", "cardinality", "n1", "n2", "n3", "n4", "stats", "notes"]
    control = 'w'
    if args.csv_append:
        control = 'a'
    with open(filename, control) as csvfile:
        csvwriter = csv.writer(csvfile)
        csvwriter.writerow(header)
        csvwriter.writerows(results)
