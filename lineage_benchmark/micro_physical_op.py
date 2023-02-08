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

def PersistResults(results, args):
    filename="micro_benchmark_notes_"+args.notes+".csv"
    print(filename)
    header = ["query", "runtime", "cardinality", "groups", "output", "lineage_size", "lineage_type"]
    control = 'w'
    if args.csv_append:
        control = 'a'
    with open(filename, control) as csvfile:
        csvwriter = csv.writer(csvfile)
        csvwriter.writerow(header)
        csvwriter.writerows(results)

################### Order By ###########################
##  order on 'z' with 'g' unique values and table size
#   of 'card' cardinality. Goal: see the effect of
#   large table size on lineage capture overhead
########################################################
def OrderByMicro(groups, cardinality):
    for g in groups:
        for card in cardinality:
            filename = "zipfan_g"+str(g)+"_card"+str(card)+"_a1.csv"
            print(filename, g, card)
            zipf1 = pd.read_csv(folder+filename)
            con.register('zipf1_view', zipf1)
            con.execute("create table zipf1 as select * from zipf1_view")
            q = "SELECT z FROM zipf1 Order By z"
            table_name = None
            if args.perm:
                q = "SELECT rowid, z FROM zipf1 Order By z"
                q = "create table zipf1_perm_lineage as "+ q
                table_name='zipf1_perm_lineage'
            avg, output_size = Run(q, args, con, table_name)
            if args.perm:
                df = con.execute("select count(*) as c from zipf1_perm_lineage").fetchdf()
                output_size = df.loc[0,'c']
                con.execute("drop table zipf1_perm_lineage")
            results.append(["orderby", avg, card, g, output_size, -1, lineage_type])
            if args.persist:
                q_list = "select * from queries_list where query='{}'".format(q)
                query_info = con.execute(q_list).fetchdf()
                query_id = query_info.loc[0, 'query_id']
                lineage_size = query_info.loc[0, 'lineage_size']
                lineage_q = """select count(*) as c from (SELECT LINEAGE_{0}_ORDER_BY_1_0.in_index as zipf1_rowid_0,
                            LINEAGE_{0}_ORDER_BY_1_0.out_index FROM LINEAGE_{0}_ORDER_BY_1_0)""".format(query_id)
                args.enable_lineage=False
                avg, _ = Run(lineage_q, args, con)
                args.enable_lineage=True
                df = con.execute(lineage_q).fetchdf()
                output_size = df.loc[0,'c']
                results.append(["orderby", avg, card, g, output_size, lineage_size, "SD_Query"])
            if args.enable_lineage:
                DropLineageTables(con)
            con.execute("drop table zipf1")

################### Filter ###########################
##  filter on 'z' with 'g' unique values and table size
#   of 'card' cardinality. Test on values on z with
#   different selectivity
#   TODO: specify data cardinality: [nothing, 50%, 100%]
########################################################
def FilterMicro(selectivity, cardinality):
    for sel in selectivity:
        for card in cardinality:
            filename = "filter_sel"+str(sel)+"_card"+str(card)+".csv"
            print(filename, sel, card)
            t1 = pd.read_csv(folder+filename)
            con.register('t1_view', t1)
            con.execute("create table t1 as select * from t1_view")
            q = "SELECT v FROM t1 where z=0"
            table_name = None
            if args.perm:
                q = "SELECT rowid, v FROM t1 WHERE z=0"
                q = "create table t1_perm_lineage as "+ q
                table_name='t1_perm_lineage'
            avg, output_size = Run(q, args, con, table_name)
            if args.perm:
                df = con.execute("select count(*) as c from t1_perm_lineage").fetchdf()
                output_size = df.loc[0,'c']
                con.execute("drop table t1_perm_lineage")
            results.append(["filter", avg, card, sel, output_size, -1, lineage_type])
            if args.persist:
                if output_size == 0:
                    results.append(["filter", 0, card, sel, 0, 0, "SD_Query"])
                else:
                    print(con.execute("PRAGMA show_tables;").fetchdf())
                    query_info = con.execute("select * from queries_list where query='{}'".format(q)).fetchdf()
                    query_id = query_info.loc[0, 'query_id']
                    lineage_size = query_info.loc[0, 'lineage_size']
                    lineage_q = """select count(*) as c from (SELECT * FROM LINEAGE_{0}_SEQ_SCAN_0_0)""".format(query_id)
                    args.enable_lineage=False
                    avg, _ = Run(lineage_q, args, con)
                    args.enable_lineage=True
                    df = con.execute(lineage_q).fetchdf()
                    output_size = df.loc[0,'c']
                    results.append(["filter", avg, card, sel, output_size, lineage_size, "SD_Query"])
            if args.enable_lineage:
                DropLineageTables(con)
            con.execute("drop table t1")

################### Perfect Hash Aggregate  ############
##  Group by on 'z' with 'g' unique values and table size
#   of 'card'. Test on various 'g' values.
########################################################
def perfect_hashAgg(groups, cardinality):
    for g in groups:
        for card in cardinality:
            filename = "zipfan_g"+str(g)+"_card"+str(card)+"_a1.csv"
            print(filename, g, card)
            zipf1 = pd.read_csv(folder+filename)
            con.register('zipf1_view', zipf1)
            con.execute("create table zipf1 as select * from zipf1_view")
            q = "SELECT z, count(*) FROM zipf1 GROUP BY z"
            table_name, method = None, ''
            if args.perm and args.group_concat:
                q = "SELECT z, count(*), group_concat(rowid,',') FROM zipf1 GROUP BY z"
                method="_group_concat"
            elif args.perm and args.list:
                q = "SELECT z, count(*), list(rowid) FROM zipf1 GROUP BY z"
                method="_list"
            elif args.perm:
                q = "SELECT zipf1.rowid, z FROM (SELECT z, count(*) FROM zipf1 GROUP BY z) join zipf1 using (z)"
            if args.perm:
                q = "create table zipf1_perm_lineage as "+ q
                table_name='zipf1_perm_lineage'
            avg, output_size = Run(q, args, con, table_name)
            if args.perm:
                df = con.execute("select count(*) as c from zipf1_perm_lineage").fetchdf()
                output_size = df.loc[0,'c']
                con.execute("drop table zipf1_perm_lineage")
            results.append(["perfect_groupby", avg, card, g, output_size, -1, lineage_type+method])
            if args.persist:
                query_info = con.execute("select * from queries_list where query='{}'".format(q)).fetchdf()
                query_id = query_info.loc[0, 'query_id']
                lineage_size = query_info.loc[0, 'lineage_size']
                lineage_q = """select count(*) as c from (SELECT LINEAGE_{0}_PERFECT_HASH_GROUP_BY_2_0.in_index as zipf1_rowid_0,
                                LINEAGE_{0}_PERFECT_HASH_GROUP_BY_2_1.out_index
                            FROM LINEAGE_{0}_PERFECT_HASH_GROUP_BY_2_1, LINEAGE_{0}_PERFECT_HASH_GROUP_BY_2_0
                            WHERE LINEAGE_{0}_PERFECT_HASH_GROUP_BY_2_1.in_index=LINEAGE_{0}_PERFECT_HASH_GROUP_BY_2_0.out_index)""".format(query_id)
                args.enable_lineage=False
                avg, _ = Run(lineage_q, args, con)
                args.enable_lineage=True
                df = con.execute(lineage_q).fetchdf()
                output_size = df.loc[0,'c']
                results.append(["perfect_groupby", avg, card, g, output_size, lineage_size, "SD_Query"])
            if args.enable_lineage:
                DropLineageTables(con)
            con.execute("drop table zipf1")

################### Hash Aggregate  ############
##  Group by on 'z' with 'g' unique values and table size
#   of 'card'. Test on various 'g' values.
########################################################
def hashAgg(groups, cardinality):
    for g in groups:
        for card in cardinality:
            filename = "zipfan_g"+str(g)+"_card"+str(card)+"_a1.csv"
            print(filename, g, card)
            zipf1 = pd.read_csv(folder+filename)
            zipf1 = zipf1.astype({"z": str})

            con.register('zipf1_view', zipf1)
            con.execute("create table zipf1 as select * from zipf1_view")
            q = "SELECT z, count(*) FROM zipf1 GROUP BY z"
            table_name, method = None, ''
            if args.perm and args.group_concat:
                q = "SELECT z, count(*), group_concat(rowid,',') FROM zipf1 GROUP BY z"
                method="_group_concat"
            elif args.perm and args.list:
                q = "SELECT z, count(*), list(rowid) FROM zipf1 GROUP BY z"
                method="_list"
            elif args.perm:
                q = "SELECT zipf1.rowid, z FROM (SELECT z, count(*) FROM zipf1 GROUP BY z) join zipf1 using (z)"
            if args.perm:
                q = "create table zipf1_perm_lineage as "+ q
                table_name='zipf1_perm_lineage'
            avg, output_size = Run(q, args, con, table_name)
            if args.perm:
                df = con.execute("select count(*) as c from zipf1_perm_lineage").fetchdf()
                output_size = df.loc[0,'c']
                con.execute("drop table zipf1_perm_lineage")
            results.append(["groupby", avg, card, g, output_size, -1, lineage_type+method])
            if args.persist:
                query_info = con.execute("select * from queries_list where query='{}'".format(q)).fetchdf()
                query_id = query_info.loc[0, 'query_id']
                lineage_size = query_info.loc[0, 'lineage_size']
                lineage_q = """select count(*) as c from (SELECT LINEAGE_{0}_HASH_GROUP_BY_2_0.in_index as zipf1_rowid_0,
                                LINEAGE_{0}_HASH_GROUP_BY_2_1.out_index
                            FROM LINEAGE_{0}_HASH_GROUP_BY_2_1, LINEAGE_{0}_HASH_GROUP_BY_2_0
                            WHERE LINEAGE_{0}_HASH_GROUP_BY_2_1.in_index=LINEAGE_{0}_HASH_GROUP_BY_2_0.out_index)""".format(query_id)
                args.enable_lineage=False
                avg, _ = Run(lineage_q, args, con)
                args.enable_lineage=True
                df = con.execute(lineage_q).fetchdf()
                output_size = df.loc[0,'c']
                results.append(["groupby", avg, card, g, output_size, lineage_size, "SD_Query"])
            if args.enable_lineage:
                DropLineageTables(con)
            con.execute("drop table zipf1")

################### Joins ###########################
###### Cross Product
def crossProduct(cardinality):
    for card in cardinality:
        # create tables & insert values
        con.execute("create table t1 as SELECT i FROM range(0,"+str(card[0])+") tbl(i)")
        con.execute("create table t2 as SELECT i FROM range(0,"+str(card[1])+") tbl(i)")
        # Run query
        q = "select * from t1, t2"
        table_name=None
        if args.perm:
            q = "SELECT t1.rowid as t1_rowid, t2.rowid as t2_rowid, *  FROM t1, t2"
            q = "create table zipf1_perm_lineage as "+ q
            table_name='zipf1_perm_lineage'
        avg, output_size = Run(q, args, con, table_name)
        if args.perm:
            df = con.execute("select count(*) as c from zipf1_perm_lineage").fetchdf()
            output_size = df.loc[0,'c']
            con.execute("drop table zipf1_perm_lineage")
        results.append(["cross_product", avg, card[0], card[1], output_size, -1, lineage_type])
        if args.persist:
            query_info = con.execute("select * from queries_list where query='{}'".format(q)).fetchdf()
            query_id = query_info.loc[0, 'query_id']
            lineage_size = query_info.loc[0, 'lineage_size']
            lineage_q = """select count(*) as c from (SELECT * FROM LINEAGE_{0}_CROSS_PRODUCT_2_1)""".format(query_id)
            args.enable_lineage=False
            avg, _ = Run(lineage_q, args, con)
            args.enable_lineage=True
            df = con.execute(lineage_q).fetchdf()
            output_size = df.loc[0,'c']
            results.append(["cross_product", avg, card, g, output_size, lineage_size,  "SD_Query"])
        if args.enable_lineage:
            DropLineageTables(con)
        con.execute("drop table t1")
        con.execute("drop table t2")

###### Picewise Merge Join (predicate: less/greater than)
def mergeJoin(cardinality):
    for card in cardinality:
        # create tables & insert values
        con.execute("create table t1 as SELECT i FROM range(0,"+str(card[0])+") tbl(i)")
        con.execute("create table t2 as SELECT i FROM range(0,"+str(card[1])+") tbl(i)")
        # Run query
        q = "select * from t1, t2 where t1.i < t2.i"
        table_name = None
        if args.perm:
            q = "SELECT t1.rowid as t1_rowid, t2.rowid as t2_rowid, * FROM t1, t2 WHERE t1.i<t2.i"
            q = "create table zipf1_perm_lineage as "+ q
            table_name='zipf1_perm_lineage'
        avg, output_size = Run(q, args, con, table_name)
        if args.perm:
            df = con.execute("select count(*) as c from zipf1_perm_lineage").fetchdf()
            output_size = df.loc[0,'c']
            con.execute("drop table zipf1_perm_lineage")
        results.append(["merge_join", avg, card[0], card[1], output_size, -1, lineage_type])
        if args.persist:
            query_info = con.execute("select * from queries_list where query='{}'".format(q)).fetchdf()
            query_id = query_info.loc[0, 'query_id']
            lineage_size = query_info.loc[0, 'lineage_size']
            lineage_q = """select count(*) as c from (SELECT * FROM LINEAGE_{0}_PIECEWISE_MERGE_JOIN_2_1)""".format(query_id)
            args.enable_lineage=False
            avg, _ = Run(lineage_q, args, con)
            args.enable_lineage=True
            df = con.execute(lineage_q).fetchdf()
            output_size = df.loc[0,'c']
            results.append(["merge_join", avg, card, g, output_size, lineage_size, "SD_Query"])
        if args.enable_lineage:
            DropLineageTables(con)
        con.execute("drop table t1")
        con.execute("drop table t2")

# NLJ (predicate: inequality)
def NLJ(cardinality):
    for card in cardinality:
        # create tables & insert values
        con.execute("create table t1 as SELECT i FROM range(0,"+str(card[0])+") tbl(i)")
        con.execute("create table t2 as SELECT i FROM range(0,"+str(card[1])+") tbl(i)")
        # Run query
        q = "select * from t1, t2 where t1.i <> t2.i"
        table_name = None
        if args.perm:
            q = "SELECT t1.rowid as t1_rowid, t2.rowid as t2_rowid, * FROM t1, t2 WHERE t1.i<>t2.i"
            q = "create table zipf1_perm_lineage as "+ q
            table_name='zipf1_perm_lineage'
        avg, output_size = Run(q, args, con, table_name)
        if args.perm:
            df = con.execute("select count(*) as c from zipf1_perm_lineage").fetchdf()
            output_size = df.loc[0,'c']
            con.execute("drop table zipf1_perm_lineage")
        results.append(["nl_join", avg, card[0], card[1], output_size, -1, lineage_type])
        if args.persist:
            query_info = con.execute("select * from queries_list where query='{}'".format(q)).fetchdf()
            query_id = query_info.loc[0, 'query_id']
            lineage_size = query_info.loc[0, 'lineage_size']
            lineage_q = """select count(*) as c from (SELECT * FROM LINEAGE_{0}_NESTED_LOOP_JOIN_2_1)""".format(query_id)
            args.enable_lineage=False
            avg, _ = Run(lineage_q, args, con)
            args.enable_lineage=True
            df = con.execute(lineage_q).fetchdf()
            output_size = df.loc[0,'c']
            results.append(["nl_join", avg, card, g, output_size, lineage_size,  "SD_Query"])
        if args.enable_lineage:
            DropLineageTables(con)
        con.execute("drop table t1")
        con.execute("drop table t2")

# BNLJ (predicate: or)
def BNLJ(cardinality):
    for card in cardinality:
        # create tables & insert values
        con.execute("create table t1 as SELECT i FROM range(0,"+str(card[0])+") tbl(i)")
        con.execute("create table t2 as SELECT i FROM range(0,"+str(card[1])+") tbl(i)")
        # Run query
        q = "select * from t1, t2 where t1.i=t2.i or t1.i<t2.i"
        table_name = None
        if args.perm:
            q = "SELECT t1.rowid as t1_rowid, t2.rowid as t2_rowid, * FROM t1, t2 WHERE t1.i=t2.i or t1.i<t2.i"
            q = "create table zipf1_perm_lineage as "+ q
            table_name='zipf1_perm_lineage'
        avg, output_size = Run(q, args, con, table_name)
        if args.perm:
            df = con.execute("select count(*) as c from zipf1_perm_lineage").fetchdf()
            output_size = df.loc[0,'c']
            con.execute("drop table zipf1_perm_lineage")
        results.append(["bnl_join", avg, card[0], card[1], output_size, -1, lineage_type])
        if args.persist:
            query_info = con.execute("select * from queries_list where query='{}'".format(q)).fetchdf()
            query_id = query_info.loc[0, 'query_id']
            lineage_size = query_info.loc[0, 'lineage_size']
            lineage_q = """select count(*) as c from (SELECT * FROM LINEAGE_{0}_BLOCKWISE_NL_JOIN_2_1)""".format(query_id)
            args.enable_lineage=False
            avg, _ = Run(lineage_q, args, con)
            args.enable_lineage=True
            df = con.execute(lineage_q).fetchdf()
            output_size = df.loc[0,'c']
            results.append(["bnl_join", avg, card, g, output_size, lineage_size,"SD_Query"])
        if args.enable_lineage:
            DropLineageTables(con)
        con.execute("drop table t1")
        con.execute("drop table t2")


# Hash Join
def HashJoinFKPK(groups, cardinality):
    for g in groups:
        idx = list(range(0, g))
        gid = pd.DataFrame({'id':idx})
        con.register('gids_view', gid)
        con.execute("create table gids as select * from gids_view")
        for card in cardinality:
            filename = "zipfan_g"+str(g)+"_card"+str(card)+"_a1.csv"
            print(filename, g, card)
            zipf1 = pd.read_csv(folder+filename)
            con.register('zipf1_view', zipf1)
            con.execute("create table zipf1 as select * from zipf1_view")
            q = "SELECT * FROM gids, zipf1 WHERE gids.id=zipf1.z"
            table_name = None
            if args.perm:
                q = "SELECT zipf1.rowid as zipf1_rowid, gids.rowid as gids_rowid, * FROM zipf1, gids WHERE zipf1.z=gids.id"
                q = "create table zipf1_perm_lineage as "+ q
                table_name='zipf1_perm_lineage'
            avg, output_size = Run(q, args, con, table_name)
            if args.perm:
                df = con.execute("select count(*) as c from zipf1_perm_lineage").fetchdf()
                output_size = df.loc[0,'c']
                con.execute("drop table zipf1_perm_lineage")
            results.append(["hash_join_pkfk", avg, card, g, output_size, -1, lineage_type])
            if args.persist:
                query_info = con.execute("select * from queries_list where query='{}'".format(q)).fetchdf()
                query_id = query_info.loc[0, 'query_id']
                lineage_size = query_info.loc[0, 'lineage_size']
                lineage_q = """select count(*) as c from (SELECT LINEAGE_{0}_HASH_JOIN_2_1.rhs_index as zipf1_rowid_0,
                                LINEAGE_{0}_HASH_JOIN_2_0.in_index as gids_rowid_1,
                                LINEAGE_{0}_HASH_JOIN_2_1.out_index
                            FROM LINEAGE_{0}_HASH_JOIN_2_1, LINEAGE_{0}_HASH_JOIN_2_0
                            WHERE LINEAGE_{0}_HASH_JOIN_2_0.out_address=LINEAGE_{0}_HASH_JOIN_2_1.lhs_address)""".format(query_id)
                args.enable_lineage=False
                avg, _ = Run(lineage_q, args, con)
                args.enable_lineage=True
                df = con.execute(lineage_q).fetchdf()
                output_size = df.loc[0,'c']
                results.append(["hash_join_pkfk", avg, card, g, output_size, lineage_size, "SD_Query"])
            if args.enable_lineage:
                DropLineageTables(con)
            con.execute("drop table zipf1")
        con.execute("drop table gids")

############## Hash Join many-to-many ##########
# zipf1.z is within [1,10] or [1,100]
# zipf2.z is [1,100]
# left size=1000, right size: 1000 .. 100000
def HashJoinMtM(groups, cardinality):
    filename = "zipfan_g100_card1000_a1.csv"
    zipf2 = pd.read_csv(folder+filename)
    con.register('zipf2_view', zipf2)
    con.execute("create table zipf2 as select * from zipf2_view")
    for g in groups:
        for card in cardinality:
            filename = "zipfan_g"+str(g)+"_card"+str(card)+"_a1.csv"
            print(filename, g, card)
            zipf1 = pd.read_csv(folder+filename)
            con.register('zipf1_view', zipf1)
            con.execute("create table zipf1 as select * from zipf1_view")
            q = "SELECT * FROM zipf1, zipf2 WHERE zipf1.z=zipf2.z"
            table_name = None
            if args.perm:
                q = "SELECT zipf1.rowid as zipf1_rowid, zipf2.rowid as zipf2_rowid, * FROM zipf1, zipf2 WHERE zipf1.z=zipf2.z"
                q = "create table zipf1_perm_lineage as "+ q
                table_name='zipf1_perm_lineage'
            avg, output_size = Run(q, args, con, table_name)
            if args.perm:
                df = con.execute("select count(*) as c from zipf1_perm_lineage").fetchdf()
                output_size = df.loc[0,'c']
                con.execute("drop table zipf1_perm_lineage")
            results.append(["hash_join_mtm", avg, card, g, output_size, -1, lineage_type])
            if args.persist:
                query_info = con.execute("select * from queries_list where query='{}'".format(q)).fetchdf()
                query_id = query_info.loc[0, 'query_id']
                lineage_size = query_info.loc[0, 'lineage_size']
                lineage_q = """select count(*) as c from (SELECT LINEAGE_{0}_HASH_JOIN_2_1.rhs_index as zipf1_rowid_0,
                                      LINEAGE_{0}_HASH_JOIN_2_0.in_index as gids_rowid_1,
                                      LINEAGE_{0}_HASH_JOIN_2_1.out_index
                                FROM LINEAGE_{0}_HASH_JOIN_2_1, LINEAGE_{0}_HASH_JOIN_2_0
                                WHERE LINEAGE_{0}_HASH_JOIN_2_0.out_address=LINEAGE_{0}_HASH_JOIN_2_1.lhs_address)""".format(query_id)
                args.enable_lineage=False
                avg, output_size = Run(lineage_q, args, con)
                args.enable_lineage=True
                df = con.execute(lineage_q).fetchdf()
                output_size = df.loc[0,'c']
                results.append(["hash_join_mtm", avg, card, g, output_size, lineage_size, "SD_Query"])
            if args.enable_lineage:
                DropLineageTables(con)
            con.execute("drop table zipf1")
    con.execute("drop table zipf2")

# Index Join (predicate: join on index attribute)
def IndexJoinFKPK(groups, cardinality):
    con.execute("PRAGMA explain_output = PHYSICAL_ONLY;")
    con.execute("PRAGMA force_index_join")
    for g in groups:
        idx = list(range(0, g))
        gid = pd.DataFrame({'id':idx, 'v':idx})
        con.register('gids_view', gid)
        con.execute("create table gids as select * from gids_view")
        for card in cardinality:
            filename = "zipfan_g"+str(g)+"_card"+str(card)+"_a1.csv"
            print(filename, g, card)
            zipf1 = pd.read_csv(folder+filename)
            con.register('zipf1_view', zipf1)
            con.execute("create table zipf1 as select * from zipf1_view")
            con.execute("create index i_index ON zipf1 using art(z);");
            q = "SELECT gids.* FROM gids, zipf1 WHERE gids.id=zipf1.z"
            table_name = None
            if args.perm:
                q = "SELECT zipf1.rowid as zipf1_rowid, gids.rowid as gids_rowid, gids.* FROM zipf1, gids WHERE zipf1.z=gids.id"
                q = "create table zipf1_perm_lineage as "+ q
                table_name='zipf1_perm_lineage'
            avg, output_size = Run(q, args, con, table_name)
            if args.perm:
                df = con.execute("select count(*) as c from zipf1_perm_lineage").fetchdf()
                output_size = df.loc[0,'c']
                con.execute("drop table zipf1_perm_lineage")
            results.append(["index_join_pkfk", avg, card, g, output_size, -1, lineage_type])
            if args.persist:
                query_info = con.execute("select * from queries_list where query='{}'".format(q)).fetchdf()
                query_id = query_info.loc[0, 'query_id']
                lineage_size = query_info.loc[0, 'lineage_size']
                lineage_q = """select count(*) as c from (SELECT LINEAGE_{0}_INDEX_JOIN_2_0.rhs_index as zipf1_rowid_0,
                                    LINEAGE_{0}_INDEX_JOIN_2_0.lhs_index as gids_rowid_1,
                                    LINEAGE_{0}_INDEX_JOIN_2_0.out_index
                            FROM LINEAGE_{0}_INDEX_JOIN_2_0)""".format(query_id)
                args.enable_lineage=False
                avg, output_size = Run(lineage_q, args, con)
                args.enable_lineage=True
                df = con.execute(lineage_q).fetchdf()
                output_size = df.loc[0,'c']
                results.append(["index_join_pkfk", avg, card, g, output_size, lineage_size, "SD_Query"])
            if args.enable_lineage:
                DropLineageTables(con)
            con.execute("DROP INDEX i_index")
            con.execute("drop table zipf1")
        con.execute("drop table gids")


def IndexJoinMtM(groups, cardinality):
    con.execute("PRAGMA explain_output = PHYSICAL_ONLY;")
    con.execute("PRAGMA force_index_join")
    filename = "zipfan_g100_card1000_a1.csv"
    zipf2 = pd.read_csv(folder+filename)
    con.register('zipf2_view', zipf2)
    con.execute("create table zipf2 as select * from zipf2_view")
    con.execute("create index i_index ON zipf2 using art(z);");
    for g in groups:
        for card in cardinality:
            filename = "zipfan_g"+str(g)+"_card"+str(card)+"_a1.csv"
            print(filename, g, card)
            zipf1 = pd.read_csv(folder+filename)
            con.register('zipf1_view', zipf1)
            con.execute("create table zipf1 as select * from zipf1_view")
            
            q = "SELECT zipf1.* FROM zipf1, zipf2 WHERE zipf1.z=zipf2.z"
            table_name = None
            if args.perm:
                q = "SELECT zipf1.rowid as zipf1_rowid, zipf2.rowid as zipf2_rowid, zipf1.* FROM zipf1, zipf2 WHERE zipf1.z=zipf2.z"
                q = "create table zipf1_perm_lineage as "+ q
                table_name='zipf1_perm_lineage'
            avg, output_size = Run(q, args, con, table_name)
            if args.perm:
                df = con.execute("select count(*) as c from zipf1_perm_lineage").fetchdf()
                output_size = df.loc[0,'c']
                con.execute("drop table zipf1_perm_lineage")
            results.append(["index_join_mtm", avg, card, g, output_size, -1, lineage_type])
            if args.persist:
                query_info = con.execute("select * from queries_list where query='{}'".format(q)).fetchdf()
                query_id = query_info.loc[0, 'query_id']
                lineage_size = query_info.loc[0, 'lineage_size']
                lineage_q = """select count(*) as c from (SELECT LINEAGE_{0}_INDEX_JOIN_2_0.rhs_index as zipf1_rowid_0,
                                      LINEAGE_{0}_INDEX_JOIN_2_0.lhs_index as gids_rowid_1,
                                      LINEAGE_{0}_INDEX_JOIN_2_0.out_index
                                FROM LINEAGE_{0}_INDEX_JOIN_2_0)""".format(query_id)
                args.enable_lineage=False
                avg, output_size = Run(lineage_q, args, con)
                args.enable_lineage=True
                df = con.execute(lineage_q).fetchdf()
                output_size = df.loc[0,'c']
                results.append(["index_join_mtm", avg, card, g, output_size, lineage_size, "SD_Query"])
            if args.enable_lineage:
                DropLineageTables(con)
            con.execute("drop table zipf1")
    con.execute("DROP INDEX i_index")
    con.execute("drop table zipf2")

print("------------ Test Order By zipfan 1")
groups = [100]
cardinality = [1000000, 5000000, 10000000]
OrderByMicro(groups, cardinality)
print("------------ Test Filter zipfan 1")
selectivity = [0.0, 0.02, 0.2, 0.5, 0.8, 1.0]
cardinality = [1000000, 5000000, 10000000]
FilterMicro(selectivity, cardinality)
print("------------ Test Perfect Group By zipfan 1")
groups = [10, 100, 1000]
cardinality = [1000000, 5000000, 10000000]
perfect_hashAgg(groups, cardinality)
print("------------ Test Group By zipfan 1")
groups = [10, 100, 1000]
cardinality = [1000000, 5000000, 10000000]
hashAgg(groups, cardinality)
print("------------ Test Joins")
cardinality = [(100, 10000), (4000, 4000), (10000, 10000)]
print("------------ Test Cross Product")
crossProduct(cardinality)
print("------------ Test Piecwise Merge Join")
mergeJoin(cardinality)
print("------------ Test Nested Loop Join")
NLJ(cardinality)
print("------------ Test Block Nested Loop Join")
BNLJ(cardinality)
print("------------ Test Hash Join FK-PK")
groups = [10, 100, 1000]
cardinality = [1000000, 5000000, 10000000]
HashJoinFKPK(groups, cardinality)
print("------------ Test Many to Many Join zipfan 1")
groups = [10, 100]
cardinality = [1000, 10000, 100000]
HashJoinMtM(groups, cardinality)
print("------------ Test Index Join PF:FK")
groups = [10, 100, 1000]
cardinality = [1000000, 5000000, 10000000]
IndexJoinFKPK(groups, cardinality)
print("------------ Test Index Join Many to Many Join zipfan 1")
groups = [10, 100]
cardinality = [1000, 10000, 100000]
IndexJoinMtM(groups, cardinality)

########### Write results to CSV
if args.save_csv:
    PersistResults(results)
