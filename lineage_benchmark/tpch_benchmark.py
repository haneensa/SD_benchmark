# python3.7 scripts/lineage_benchmark/tpch_benchmark.py feb11  --repeat 3  --save_csv  --perm --csv_append
# python3.7 scripts/lineage_benchmark/tpch_benchmark.py feb11  --repeat 3  --save_csv  --csv_append
import duckdb
import pandas as pd
import argparse
import csv

from utils import Run, DropLineageTables, getStats

parser = argparse.ArgumentParser(description='TPCH benchmarking script')
parser.add_argument('notes', type=str,  help="run notes")
parser.add_argument('--enable_lineage', action='store_true',  help="Enable trace_lineage")
parser.add_argument('--show_tables', action='store_true',  help="list tables")
parser.add_argument('--show_output', action='store_true',  help="query output")
parser.add_argument('--stats', action='store_true',  help="get lineage size, nchunks and postprocess time")
parser.add_argument('--query_lineage', action='store_true',  help="query lineage")
parser.add_argument('--persist', action='store_true',  help="Persist lineage captured")
parser.add_argument('--perm', action='store_true',  help="use perm queries")
parser.add_argument('--save_csv', action='store_true',  help="save result in csv")
parser.add_argument('--csv_append', action='store_true',  help="Append results to old csv")
parser.add_argument('--sf', type=float, help="sf scale", default=1)
parser.add_argument('--repeat', type=int, help="Repeat time for each query", default=1)
parser.add_argument('--profile', action='store_true',  help="Enable profiling")
parser.add_argument('--threads', type=int, help="number of threads", default=1)
args = parser.parse_args()
print(args.profile)

con = duckdb.connect(database=':memory:', read_only=False)
prefix = "queries/q"
table_name=None
if args.perm:
    prefix = "queries/perm/q"
    args.lineage_query = False
    lineage_type = "Logical-RID"
    table_name='lineage'
elif not args.enable_lineage:
    lineage_type = "Baseline"
elif args.persist:
    lineage_type = "SD_Persist"
else:
    lineage_type = "SD_Capture"

# sf: 1, 5, 10, 20
# threads: 1, 4, 8, 12, 16
sf_list = [1]
threads_list = [1]#, 4, 8, 12, 16]
results = []
for sf in sf_list:
    con.execute("CALL dbgen(sf="+str(sf)+");")
    for th_id in threads_list:
        con.execute("PRAGMA threads="+str(th_id))
        con.execute("PRAGMA force_parallelism")
    
        for i in range(9, 10):
            qfile = prefix+str(i).zfill(2)+".sql"
            text_file = open(qfile, "r")
            query = text_file.read()
            text_file.close()
            print("%%%%%%%%%%%%%%%% Running Query # ", i, " threads: ", th_id)
            avg, df = Run(query, args, con, table_name)
            output_size = len(df)
            if table_name:
                df = con.execute("select count(*) as c from {}".format(table_name)).fetchdf()
                output_size = df.loc[0,'c']
                con.execute("DROP TABLE "+table_name)
            if args.show_tables:
                print(con.execute("PRAGMA show_tables").fetchdf())
            stats = ""
            if args.enable_lineage and args.stats:
                lineage_size, nchunks, postprocess_time= getStats(con, query)
                stats = "{},{},{}".format(lineage_size, nchunks, postprocess_time*1000)
            results.append([i, avg, sf, args.repeat, lineage_type, th_id, output_size, stats, args.notes])

if args.save_csv:
    filename="tpch_benchmark_capture_{}.csv".format(args.notes)
    print(filename)
    header = ["query", "runtime", "sf", "repeat", "lineage_type", "n_threads", "output", "stats", "notes"]
    control = 'w'
    if args.csv_append:
        control = 'a'
    with open(filename, control) as csvfile:
        csvwriter = csv.writer(csvfile)
        csvwriter.writerow(header)
        csvwriter.writerows(results)
