# TODO: incremantally add: SD_Capture, SD_Capture_LSN, SD_Capture_Offsets, SD_Capture+Index, SD_Capture+PostPorcess, SD_Capture+Index+PostProcess
##      add querying single operator logic
##      add visualization graph and summarization / compute relative overhead and compare between different systems
# Benchmark DuckDB's physical operators
# Collect runtime, lineage size, output cardinality
# %%% SD_Capture:
# $ python3.7 scripts/lineage_benchmark/micro_physical_op.py draft2 --save_csv --repeat 1 --enable_lineage  --show_output --csv_append --base /home/haneen/
# %%% SD_Persist and SD_Query:
# $ python3.7 scripts/lineage_benchmark/micro_physical_op.py draft2 --save_csv --repeat 1 --enable_lineage --persist --show_output --csv_append
# %%% Perm:
# $ python3.7 scripts/lineage_benchmark/micro_physical_op.py draft2 --save_csv --repeat 1  --show_output --csv_append --base /home/haneen/ --perm
# %%% Baseline:
# $ python3.7 scripts/lineage_benchmark/micro_physical_op.py draft2 --save_csv --repeat 1  --show_output --csv_append
import duckdb
import pandas as pd
import argparse
import csv
import numpy as np

from utils import MicroDataZipfan, MicroDataSelective, DropLineageTables, Run
from micro_physical_op import *

parser = argparse.ArgumentParser(description='Micro Benchmark: Physical Operators')
# results management
parser.add_argument('notes', type=str,  help="run notes")
parser.add_argument('--save_csv', action='store_true',  help="save result in csv")
parser.add_argument('--csv_append', action='store_true',  help="Append results to old csv")
parser.add_argument('--show_output', action='store_true',  help="query output")
parser.add_argument('--profile', action='store_true',  help="Enable profiling")
# lineage system
parser.add_argument('--enable_lineage', action='store_true',  help="Enable trace_lineage")
parser.add_argument('--persist', action='store_true',  help="Persist lineage captured")
parser.add_argument('--stats', action='store_true',  help="get lineage size, nchunks and postprocess time")
parser.add_argument('--perm', action='store_true',  help="Use Perm Approach with join")
parser.add_argument('--group_concat', action='store_true',  help="Use Perm Apprach with group concat")
parser.add_argument('--list', action='store_true',  help="Use Perm Apprach with list")
# benchmark setting
parser.add_argument('--repeat', type=int, help="Repeat time for each query", default=1)
parser.add_argument('--base', type=str, help="Base directory for benchmark_data", default="")
args = parser.parse_args()

# append log results for each query instance
# schema:  ["query", "runtime", "cardinality", "groups", "output", "lineage_size", "lineage_type"]
results = []

if args.enable_lineage and args.persist:
    lineage_type = "SD_Persist"
elif args.enable_lineage:
    lineage_type = "SD_Capture"
elif args.perm:
    lineage_type = "Perm"
else:
    lineage_type = "Baseline"

con = duckdb.connect(database=':memory:', read_only=False)

if args.perm and args.enable_lineage:
    args.enable_lineage=False


################### TODO: 
# 1. Check data exists if not, then generate data
folder = args.base + "benchmark_data/"
groups = [10, 100, 1000]
cardinality = [1000, 10000, 100000, 1000000, 5000000, 10000000]
max_val = 100
a = 1
MicroDataZipfan(folder, groups, cardinality, max_val, a)
selectivity = [0.0, 0.02, 0.2, 0.5, 0.8, 1.0]
cardinality = [1000000, 5000000, 10000000]
MicroDataSelective(folder, selectivity, cardinality)

################### Order By ###########################
##  order on 'z' with 'g' unique values and table size
#   of 'card' cardinality. Goal: see the effect of
#   large table size on lineage capture overhead
#   zipfan a=1 -> has duplicates
########################################################
groups = [100]
cardinality = [1000000, 5000000, 10000000]
ScanMicro(con, args, folder, lineage_type, groups, cardinality, results)
OrderByMicro(con, args, folder, lineage_type, groups, cardinality, results)

################### Filter ###########################
##  filter on 'z' with 'g' unique values and table size
#   of 'card' cardinality. Test on values on z with
#   different selectivity
#   TODO: specify data cardinality: [nothing, 50%, 100%]
########################################################
selectivity = [0.0, 0.2, 0.5, 1.0]
cardinality = [1000000, 5000000, 10000000]
pushdown = "filter"
FilterMicro(con, args, folder, lineage_type, selectivity, cardinality, results, pushdown)
pushdown = "clear"
FilterMicro(con, args, folder, lineage_type, selectivity, cardinality, results, pushdown)

################### Hash Aggregate  ############
##  Group by on 'z' with 'g' unique values and table size
#   of 'card'. Test on various 'g' values.
########################################################
groups = [10, 100, 1000]
cardinality = [1000000, 5000000, 10000000]
agg_type = "perfect"
int_hashAgg(con, args, folder, lineage_type, groups, cardinality, results, agg_type)
agg_type = "reg"
int_hashAgg(con, args, folder, lineage_type, groups, cardinality, results, agg_type)
groups = [10, 100, 1000]
cardinality = [1000000, 5000000, 10000000]
hashAgg(con, args, folder, lineage_type, groups, cardinality, results)

################### Joins  ############
########################################################
print("------------ Test Joins-----------")

cardinality = [(100, 10000), (4000, 4000), (10000, 10000)]
jointype = "merge"
join_lessthan(con, args, folder, lineage_type, cardinality, results, jointype)
jointype = "nl"
join_lessthan(con, args, folder, lineage_type, cardinality, results, jointype)

#NLJ(con, args, folder, lineage_type, cardinality, results)
BNLJ(con, args, folder, lineage_type, cardinality, results)

crossProduct(con, args, folder, lineage_type, cardinality, results)

############## Hash Join PKFK ##########
groups = [10, 100, 1000]
cardinality = [1000000, 5000000, 10000000]
HashJoinFKPK(con, args, folder, lineage_type, groups, cardinality, results)

############## Hash Join many-to-many ##########
# zipf1.z is within [1,10] or [1,100]
# zipf2.z is [1,100]
# left size=1000, right size: 1000 .. 100000
groups = [10, 100]
cardinality = [1000, 10000, 100000]
HashJoinMtM(con, args, folder, lineage_type, groups, cardinality, results)

############## Index Join (predicate: join on index attribute)
groups = [10, 100, 1000]
cardinality = [1000000, 5000000, 10000000]
IndexJoinFKPK(con, args, folder, lineage_type, groups, cardinality, results)

############ Index Join (M:M)
groups = [10, 100]
cardinality = [1000, 10000, 100000]
IndexJoinMtM(con, args, folder, lineage_type, groups, cardinality, results)

########### write results to csv
if args.save_csv:
    filename="micro_benchmark_notes_"+args.notes+".csv"
    PersistResults(results, filename, args.csv_append)
