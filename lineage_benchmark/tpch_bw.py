### Benchmark Perm backward queries by rerunning the original query with predicate
### for Q11, use for queries/q11.sql and perm_bw/q11.sql
### sf10: sum(ps_supplycost * ps_availqty) * 0.0000100000
### sf1: sum(ps_supplycost * ps_availqty) * 0.0001000000
import duckdb
import datetime
import pandas as pd
import argparse
import csv
import random

from utils import Run

parser = argparse.ArgumentParser(description='TPCH single script')
# results management
parser.add_argument('notes', type=str,  help="run notes")
parser.add_argument('--save_csv', action='store_true',  help="save result in csv")
parser.add_argument('--csv_append', action='store_true',  help="Append results to old csv")
parser.add_argument('--show_tables', action='store_true',  help="list tables")
parser.add_argument('--show_output', action='store_true',  help="query output")
parser.add_argument('--profile', action='store_true',  help="enable profiling")
# lineage system
parser.add_argument('--enable_lineage', action='store_true',  help="Enable trace_lineage")
# benchmark setting
parser.add_argument('--sf', type=float, help="sf scale", default=1)
parser.add_argument('--threads', type=int, help="number of threads", default=1)
parser.add_argument('--repeat', type=int, help="Repeat time for each query", default=5)
args = parser.parse_args()
print("script args: ", args)

def PersistResults(results, args):
    filename="tpch_bw_notes_"+args.notes+"_lineage_type_Logical-RID.csv"
    print(filename)
    header = ["query", "runtime", "min", "max", "sf", "repeat", "lineage_type", "n_threads", "lineage_size", "base_size"]
    control = 'w'
    if args.csv_append:
        control = 'a'
    with open(filename, control) as csvfile:
        csvwriter = csv.writer(csvfile)
        csvwriter.writerow(header)
        csvwriter.writerows(results)

results = []
base = "extension/tpch/dbgen/queries"
con = duckdb.connect(database=':memory:', read_only=False)

# generate TPCH workload
con.execute("CALL dbgen(sf="+str(args.sf)+");")
if args.threads > 1:
    con.execute("PRAGMA threads="+str(args.threads))
    con.execute("PRAGMA force_parallelism")

for qid in range(1, 23):
    print("=======" + str(qid) + "========")
    
    # collect query attributes to reference when running
    # BW queries by applying predicates
    qkeys = base+"/perm_keys/q"+str(qid).zfill(2)+".sql"
    qkeys_text_file = open(qkeys, "r")
    qkeys = qkeys_text_file.read()
    qkeys = " ".join(qkeys.split())
    qkeys = qkeys.split(',')
    
    # read base query = tpch query qid
    q = base+"/q"+str(qid).zfill(2)+".sql"
    text_file = open(q, "r")
    base_q = text_file.read()
    base_q = " ".join(base_q.split())
    text_file.close()

    # run base query
    df = con.execute(base_q).fetchdf()
    perm_prefix = base + "/perm_bw/q"
    t = 0
    tmin = 1000000
    tmax = 0

    max_ntuples = 10
    # cap number of bw queries to max_ntuples
    sample_size = min(len(df), max_ntuples)
    print("Running ", sample_size, "queries for qid:", qid)
    # pick random tuples ID to run backward queries on
    test_out = random.sample(range(0, len(df)), sample_size)
    print("Tuples ID for BW: ", test_out)
    if sample_size == 0:
        continue
    for i in test_out:
        # construct the predicate for the perm query
        predicate=''
        for k in qkeys:
            if len(k) == 0:
                continue
            if len(predicate)>0:
                predicate+=" AND "
            val = df.loc[i, k]
            if isinstance(val, int) or isinstance(val, float):
                predicate+=k+"="+str(val)
            elif isinstance(val, str):
                predicate+=k+ "='"+str(val)+"'"
            elif isinstance(val,datetime.date):
                predicate+=k+"='"+str(val)+"'"
            else:
                predicate+=k+"="+str(val)
        # Add the where predicate to the lineage query
        if len(predicate) > 0:
            predicate = " where " + predicate
        # read perm query
        q = perm_prefix+str(qid).zfill(2)+".sql"
        text_file = open(q, "r")
        tpch = text_file.read()
        tpch = " ".join(tpch.split()) + " " + predicate
        text_file.close()
        # run perm query
        avg, _ = Run(tpch, args, con)
        # return how many tuples returned by the BW query
        output_size = con.execute(tpch).fetchdf().loc[0, 'c']
        if avg < tmin: tmin = avg
        if avg > tmax: tmax = avg
        t += avg
    results.append([qid, t/sample_size, tmin, tmax, args.sf, args.repeat, "Logical-RID", args.threads, output_size, len(df)])

if args.save_csv:
    PersistResults(results, args)
