import duckdb
import pandas as pd
import argparse
import csv
import numpy as np

from utils import MicroDataZipfan, MicroDataSelective, DropLineageTables, Run

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

################### Order By ###########################
##  order on 'z' with 'g' unique values and table size
#   of 'card' cardinality. Goal: see the effect of
#   large table size on lineage capture overhead
########################################################
def getStats(con, q):
    q_list = "select * from queries_list where query='{}'".format(q)
    query_info = con.execute(q_list).fetchdf()
    print("Query info: ", query_info)
    query_id = query_info.loc[0, 'query_id']
    lineage_size = query_info.loc[0, 'lineage_size']
    nchunks = query_info.loc[0, 'nchunks']
    postprocess_time = query_info.loc[0, 'postprocess_time']

    return lineage_size, nchunks, postprocess_time

def ScanMicro(con, args, folder, lineage_type, groups, cardinality, results):
    print("------------ Test Scan zipfan 1", lineage_type)
    for g in groups:
        for card in cardinality:
            filename = "zipfan_g"+str(g)+"_card"+str(card)+"_a1.csv"
            print(filename, g, card)
            zipf1 = pd.read_csv(folder+filename)
            con.register('zipf1_view', zipf1)
            con.execute("create table zipf1 as select * from zipf1_view")
            q = "SELECT count(idx) as c FROM zipf1"
            table_name = None
            if lineage_type == "Perm":
                q = "SELECT rowid, idx FROM zipf1"
                q = "create table zipf1_perm_lineage as "+ q
                table_name='zipf1_perm_lineage'
            avg, df = Run(q, args, con, table_name)
            if lineage_type == "Perm":
                df = con.execute("select count(*) as c from zipf1_perm_lineage").fetchdf()
                output_size = df.loc[0,'c']
                con.execute("drop table zipf1_perm_lineage")
            else:
                output_size = df.loc[0,'c']
            stats = ""
            if args.enable_lineage and args.stats:
                lineage_size, nchunks, postprocess_time= getStats(con, q)
                stats = "{},{},{}".format(lineage_size, nchunks, postprocess_time*1000)
            results.append(["scan", avg, card, g, output_size, stats, lineage_type, args.notes])
            if args.enable_lineage:
                DropLineageTables(con)
            con.execute("drop table zipf1")

def OrderByMicro(con, args, folder, lineage_type, groups, cardinality, results):
    print("------------ Test Order By zipfan 1", lineage_type)
    for g in groups:
        for card in cardinality:
            filename = "zipfan_g"+str(g)+"_card"+str(card)+"_a1.csv"
            print(filename, g, card)
            zipf1 = pd.read_csv(folder+filename)
            con.register('zipf1_view', zipf1)
            con.execute("create table zipf1 as select * from zipf1_view")
            q = "select count(*) as c from (SELECT z FROM zipf1 Order By z) as t"
            table_name = None
            if lineage_type == "Perm":
                q = "SELECT rowid, z FROM zipf1 Order By z"
                q = "create table zipf1_perm_lineage as "+ q
                table_name='zipf1_perm_lineage'
            avg, df = Run(q, args, con, table_name)
            if lineage_type == "Perm":
                df = con.execute("select count(*) as c from zipf1_perm_lineage").fetchdf()
                output_size = df.loc[0,'c']
                con.execute("drop table zipf1_perm_lineage")
            else:
                output_size = df.loc[0,'c']
            stats = ""
            if args.enable_lineage and args.stats:
                lineage_size, nchunks, postprocess_time= getStats(con, q)
                stats = "{},{},{}".format(lineage_size, nchunks, postprocess_time*1000)
            results.append(["orderby", avg, card, g, output_size, stats, lineage_type, args.notes])
            if args.enable_lineage:
                DropLineageTables(con)
            con.execute("drop table zipf1")

################### Filter ###########################
##  filter on 'z' with 'g' unique values and table size
#   of 'card' cardinality. Test on values on z with
#   different selectivity
#   TODO: specify data cardinality: [nothing, 50%, 100%]
########################################################
def FilterMicro(con, args, folder, lineage_type, selectivity, cardinality, results, pushdown):
    print("------------ Test Filter zipfan 1 ", lineage_type, " filter_pushdown: ", pushdown)
    con.execute("PRAGMA set_filter='{}'".format(pushdown))
    for sel in selectivity:
        for card in cardinality:
            filename = "filter_sel"+str(sel)+"_card"+str(card)+".csv"
            print(filename, sel, card)
            t1 = pd.read_csv(folder+filename)
            con.register('t1_view', t1)
            con.execute("create table t1 as select * from t1_view")
            q = "select count(*) as c from (SELECT v FROM t1 where z=0) as t"
            table_name = None
            if lineage_type == "Perm":
                q = "SELECT rowid, v FROM t1 WHERE z=0"
                q = "create table t1_perm_lineage as "+ q
                table_name='t1_perm_lineage'
            avg, df = Run(q, args, con, table_name)
            if lineage_type == "Perm":
                df = con.execute("select count(*) as c from t1_perm_lineage").fetchdf()
                output_size = df.loc[0,'c']
                con.execute("drop table t1_perm_lineage")
            else:
                output_size = df.loc[0,'c']
            stats = ""
            if args.enable_lineage and args.stats:
                lineage_size, nchunks, postprocess_time= getStats(con, q)
                stats = "{},{},{}".format(lineage_size, nchunks, postprocess_time*1000)
            name = "filter"
            if (pushdown == "clear"):
                name += "_scan"
            results.append([name, avg, card, sel, output_size, stats, lineage_type, args.notes])
            if args.enable_lineage:
                DropLineageTables(con)
            con.execute("drop table t1")
    con.execute("PRAGMA set_filter='clear'")

################### int Hash Aggregate  ############
##  Group by on 'z' with 'g' unique values and table size
#   of 'card'. Test on various 'g' values.
########################################################
def int_hashAgg(con, args, folder, lineage_type, groups, cardinality, results, agg_type):
    print("------------ Test Int Group By zipfan 1, ", lineage_type, agg_type)
    con.execute("PRAGMA set_agg='{}'".format(agg_type))
    for g in groups:
        for card in cardinality:
            filename = "zipfan_g"+str(g)+"_card"+str(card)+"_a1.csv"
            print(filename, g, card)
            zipf1 = pd.read_csv(folder+filename)
            con.register('zipf1_view', zipf1)
            con.execute("create table zipf1 as select * from zipf1_view")
            q = "select count(*) as c from (SELECT z, count(*) FROM zipf1 GROUP BY z) as t"
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
            avg, df = Run(q, args, con, table_name)
            if args.perm:
                df = con.execute("select count(*) as c from zipf1_perm_lineage").fetchdf()
                output_size = df.loc[0,'c']
                con.execute("drop table zipf1_perm_lineage")
            else:
                output_size = df.loc[0,'c']
            stats = ""
            if args.enable_lineage and args.stats:
                lineage_size, nchunks, postprocess_time= getStats(con, q)
                stats = "{},{},{}".format(lineage_size, nchunks, postprocess_time*1000)
            name = agg_type+"_agg"
            results.append([name, avg, card, g, output_size,stats, lineage_type+method, args.notes])
            if args.enable_lineage:
                DropLineageTables(con)
            con.execute("drop table zipf1")
    con.execute("PRAGMA set_agg='clear'")

################### Hash Aggregate  ############
##  Group by on 'z' with 'g' unique values and table size
#   of 'card'. Test on various 'g' values.
########################################################
def hashAgg(con, args, folder, lineage_type, groups, cardinality, results):
    print("------------ Test Group By zipfan 1, ", lineage_type)
    for g in groups:
        for card in cardinality:
            filename = "zipfan_g"+str(g)+"_card"+str(card)+"_a1.csv"
            print(filename, g, card)
            zipf1 = pd.read_csv(folder+filename)
            zipf1 = zipf1.astype({"z": str})

            con.register('zipf1_view', zipf1)
            con.execute("create table zipf1 as select * from zipf1_view")
            q = "select count(*) as c from (SELECT z, count(*) FROM zipf1 GROUP BY z) as t"
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
            avg, df = Run(q, args, con, table_name)
            if args.perm:
                df = con.execute("select count(*) as c from zipf1_perm_lineage").fetchdf()
                output_size = df.loc[0,'c']
                con.execute("drop table zipf1_perm_lineage")
            else:
                output_size = df.loc[0,'c']
            stats = ""
            if args.enable_lineage and args.stats:
                lineage_size, nchunks, postprocess_time= getStats(con, q)
                stats = "{},{},{}".format(lineage_size, nchunks, postprocess_time*1000)
            results.append(["groupby", avg, card, g, output_size, stats, lineage_type+method, args.notes])
            if args.enable_lineage:
                DropLineageTables(con)
            con.execute("drop table zipf1")

################### Joins ###########################
###### Cross Product
def crossProduct(con, args, folder, lineage_type, cardinality, results):
    print("------------ Test Cross Product ", lineage_type)
    for card in cardinality:
        # create tables & insert values
        con.execute("create table t1 as SELECT i FROM range(0,"+str(card[0])+") tbl(i)")
        con.execute("create table t2 as SELECT i FROM range(0,"+str(card[1])+") tbl(i)")
        # Run query
        q = "select count(*) as c from (select * from t1, t2)"
        table_name=None
        if args.perm:
            q = "SELECT t1.rowid as t1_rowid, t2.rowid as t2_rowid, *  FROM t1, t2"
            q = "create table zipf1_perm_lineage as "+ q
            table_name='zipf1_perm_lineage'
        avg, df = Run(q, args, con, table_name)
        if args.perm:
            df = con.execute("select count(*) as c from zipf1_perm_lineage").fetchdf()
            output_size = df.loc[0,'c']
            con.execute("drop table zipf1_perm_lineage")
        else:
            output_size = df.loc[0,'c']
        stats = ""
        if args.enable_lineage and args.stats:
            lineage_size, nchunks, postprocess_time= getStats(con, q)
            stats = "{},{},{}".format(lineage_size, nchunks, postprocess_time*1000)
        results.append(["cross_product", avg, card[0], card[1], output_size, stats, lineage_type, args.notes])
        if args.enable_lineage:
            DropLineageTables(con)
        con.execute("drop table t1")
        con.execute("drop table t2")

###### Picewise Merge Join (predicate: less/greater than)
def join_lessthan(con, args, folder, lineage_type, cardinality, results, jointype):
    print("------------ Test Join Less than ", jointype)
    con.execute("PRAGMA set_join='{}'".format(jointype))

    for card in cardinality:
        # create tables & insert values
        con.execute("create table t1 as SELECT i FROM range(0,"+str(card[0])+") tbl(i)")
        con.execute("create table t2 as SELECT i FROM range(0,"+str(card[1])+") tbl(i)")
        # Run query
        q = "select count(*) as c from (select * from t1, t2 where t1.i < t2.i) as t"
        table_name = None
        if args.perm:
            q = "SELECT t1.rowid as t1_rowid, t2.rowid as t2_rowid, * FROM t1, t2 WHERE t1.i<t2.i"
            q = "create table zipf1_perm_lineage as "+ q
            table_name='zipf1_perm_lineage'
        avg, df = Run(q, args, con, table_name)
        if args.perm:
            df = con.execute("select count(*) as c from zipf1_perm_lineage").fetchdf()
            output_size = df.loc[0,'c']
            con.execute("drop table zipf1_perm_lineage")
        else:
            output_size = df.loc[0,'c']
        stats = ""
        if args.enable_lineage and args.stats:
            lineage_size, nchunks, postprocess_time= getStats(con, q)
            stats = "{},{},{}".format(lineage_size, nchunks, postprocess_time*1000)
        results.append(["join_lessthan"+jointype, avg, card[0], card[1], output_size, stats, lineage_type, args.notes])
        if args.enable_lineage:
            DropLineageTables(con)
        con.execute("drop table t1")
        con.execute("drop table t2")
    con.execute("PRAGMA set_join='clear'")

# NLJ (predicate: inequality)
def NLJ(con, args, folder, lineage_type, cardinality, results):
    print("------------ Test Nested Loop Join")
    for card in cardinality:
        # create tables & insert values
        con.execute("create table t1 as SELECT i FROM range(0,"+str(card[0])+") tbl(i)")
        con.execute("create table t2 as SELECT i FROM range(0,"+str(card[1])+") tbl(i)")
        # Run query
        q = "select count(*) as c from (select * from t1, t2 where t1.i <> t2.i) as t"
        table_name = None
        if args.perm:
            q = "SELECT t1.rowid as t1_rowid, t2.rowid as t2_rowid, * FROM t1, t2 WHERE t1.i<>t2.i"
            q = "create table zipf1_perm_lineage as "+ q
            table_name='zipf1_perm_lineage'
        avg, df = Run(q, args, con, table_name)
        if args.perm:
            df = con.execute("select count(*) as c from zipf1_perm_lineage").fetchdf()
            output_size = df.loc[0,'c']
            con.execute("drop table zipf1_perm_lineage")
        else:
            output_size = df.loc[0,'c']
        stats = ""
        if args.enable_lineage and args.stats:
            lineage_size, nchunks, postprocess_time= getStats(con, q)
            stats = "{},{},{}".format(lineage_size, nchunks, postprocess_time*1000)
        results.append(["nl_join", avg, card[0], card[1], output_size, stats, lineage_type, args.notes])
        if args.enable_lineage:
            DropLineageTables(con)
        con.execute("drop table t1")
        con.execute("drop table t2")

# BNLJ (predicate: or)
def BNLJ(con, args, folder, lineage_type, cardinality, results):
    print("------------ Test Block Nested Loop Join")
    for card in cardinality:
        # create tables & insert values
        con.execute("create table t1 as SELECT i FROM range(0,"+str(card[0])+") tbl(i)")
        con.execute("create table t2 as SELECT i FROM range(0,"+str(card[1])+") tbl(i)")
        # Run query
        q = "select count(*) as c from (select * from t1, t2 where t1.i=t2.i or t1.i<t2.i) as t"
        table_name = None
        if args.perm:
            q = "SELECT t1.rowid as t1_rowid, t2.rowid as t2_rowid, * FROM t1, t2 WHERE t1.i=t2.i or t1.i<t2.i"
            q = "create table zipf1_perm_lineage as "+ q
            table_name='zipf1_perm_lineage'
        avg, df = Run(q, args, con, table_name)
        if args.perm:
            df = con.execute("select count(*) as c from zipf1_perm_lineage").fetchdf()
            output_size = df.loc[0,'c']
            con.execute("drop table zipf1_perm_lineage")
        else:
            output_size = df.loc[0,'c']
        stats = ""
        if args.enable_lineage and args.stats:
            lineage_size, nchunks, postprocess_time= getStats(con, q)
            stats = "{},{},{}".format(lineage_size, nchunks, postprocess_time*1000)
        results.append(["bnl_join", avg, card[0], card[1], output_size, stats, lineage_type, args.notes])
        if args.enable_lineage:
            DropLineageTables(con)
        con.execute("drop table t1")
        con.execute("drop table t2")

# Hash Join
def HashJoinFKPK(con, args, folder, lineage_type, groups, cardinality, results):
    print("------------ Test Hash Join FK-PK")
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
            q = "select count(*) as c from (SELECT * FROM gids, zipf1 WHERE gids.id=zipf1.z)"
            table_name = None
            if args.perm:
                q = "SELECT zipf1.rowid as zipf1_rowid, gids.rowid as gids_rowid, * FROM zipf1, gids WHERE zipf1.z=gids.id"
                q = "create table zipf1_perm_lineage as "+ q
                table_name='zipf1_perm_lineage'
            avg, df = Run(q, args, con, table_name)
            if args.perm:
                df = con.execute("select count(*) as c from zipf1_perm_lineage").fetchdf()
                output_size = df.loc[0,'c']
                con.execute("drop table zipf1_perm_lineage")
            else:
                output_size = df.loc[0,'c']
            stats = ""
            if args.enable_lineage and args.stats:
                lineage_size, nchunks, postprocess_time= getStats(con, q)
                stats = "{},{},{}".format(lineage_size, nchunks, postprocess_time*1000)
            results.append(["hash_join_pkfk", avg, card, g, output_size, stats, lineage_type, args.notes])
            if args.enable_lineage:
                DropLineageTables(con)
            con.execute("drop table zipf1")
        con.execute("drop table gids")
############## Hash Join many-to-many ##########
# zipf1.z is within [1,10] or [1,100]
# zipf2.z is [1,100]
# left size=1000, right size: 1000 .. 100000
def HashJoinMtM(con, args, folder, lineage_type, groups, cardinality, results):
    print("------------ Test Many to Many Join zipfan 1")
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
            q = "select count(*) as c from (SELECT * FROM zipf1, zipf2 WHERE zipf1.z=zipf2.z)"
            table_name = None
            if args.perm:
                q = "SELECT zipf1.rowid as zipf1_rowid, zipf2.rowid as zipf2_rowid, * FROM zipf1, zipf2 WHERE zipf1.z=zipf2.z"
                q = "create table zipf1_perm_lineage as "+ q
                table_name='zipf1_perm_lineage'
            avg, df = Run(q, args, con, table_name)
            if args.perm:
                df = con.execute("select count(*) as c from zipf1_perm_lineage").fetchdf()
                output_size = df.loc[0,'c']
                con.execute("drop table zipf1_perm_lineage")
            else:
                output_size = df.loc[0,'c']
            stats = ""
            if args.enable_lineage and args.stats:
                lineage_size, nchunks, postprocess_time= getStats(con, q)
                stats = "{},{},{}".format(lineage_size, nchunks, postprocess_time*1000)
            results.append(["hash_join_mtm", avg, card, g, output_size, stats, lineage_type, args.notes])
            if args.enable_lineage:
                DropLineageTables(con)
            con.execute("drop table zipf1")
    con.execute("drop table zipf2")
    return results
# Index Join (predicate: join on index attribute)
def IndexJoinFKPK(con, args, folder, lineage_type, groups, cardinality, results):
    print("------------ Test Index Join PF:FK")
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
            q = "select count(*) as c from (SELECT gids.* FROM gids, zipf1 WHERE gids.id=zipf1.z) as t"
            table_name = None
            if args.perm:
                q = "SELECT zipf1.rowid as zipf1_rowid, gids.rowid as gids_rowid, gids.* FROM zipf1, gids WHERE zipf1.z=gids.id"
                q = "create table zipf1_perm_lineage as "+ q
                table_name='zipf1_perm_lineage'
            avg, df = Run(q, args, con, table_name)
            if args.perm:
                df = con.execute("select count(*) as c from zipf1_perm_lineage").fetchdf()
                output_size = df.loc[0,'c']
                con.execute("drop table zipf1_perm_lineage")
            else:
                output_size = df.loc[0,'c']
            stats = ""
            if args.enable_lineage and args.stats:
                lineage_size, nchunks, postprocess_time= getStats(con, q)
                stats = "{},{},{}".format(lineage_size, nchunks, postprocess_time*1000)
            results.append(["index_join_pkfk", avg, card, g, output_size, stats, lineage_type, args.notes])
            if args.enable_lineage:
                DropLineageTables(con)
            con.execute("DROP INDEX i_index")
            con.execute("drop table zipf1")
        con.execute("drop table gids")

def IndexJoinMtM(con, args, folder, lineage_type, groups, cardinality, results):
    print("------------ Test Index Join Many to Many Join zipfan 1")
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
            
            q = "select count(*) as c from (SELECT zipf1.* FROM zipf1, zipf2 WHERE zipf1.z=zipf2.z) as t"
            table_name = None
            if args.perm:
                q = "SELECT zipf1.rowid as zipf1_rowid, zipf2.rowid as zipf2_rowid, zipf1.* FROM zipf1, zipf2 WHERE zipf1.z=zipf2.z"
                q = "create table zipf1_perm_lineage as "+ q
                table_name='zipf1_perm_lineage'
            avg, df = Run(q, args, con, table_name)
            if args.perm:
                df = con.execute("select count(*) as c from zipf1_perm_lineage").fetchdf()
                output_size = df.loc[0,'c']
                con.execute("drop table zipf1_perm_lineage")
            else:
                output_size = df.loc[0,'c']
            stats = ""
            if args.enable_lineage and args.stats:
                lineage_size, nchunks, postprocess_time= getStats(con, q)
                stats = "{},{},{}".format(lineage_size, nchunks, postprocess_time*1000)
            results.append(["index_join_mtm", avg, card, g, output_size, stats, lineage_type, args.notes])
            if args.enable_lineage:
                DropLineageTables(con)
            con.execute("drop table zipf1")
    con.execute("DROP INDEX i_index")
    con.execute("drop table zipf2")
