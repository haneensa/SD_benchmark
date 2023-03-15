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

################### Order By ###########################
##  order on 'z' with 'g' unique values and table size
#   of 'card' cardinality. Goal: see the effect of
#   large table size on lineage capture overhead
########################################################

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
def join_lessthan(con, args, folder, lineage_type, cardinality, results, op, force_join, pred):
    print("------------ Test Join  ", op, pred, force_join)
    if (force_join):
        con.execute("PRAGMA set_join='{}'".format(op))

    for card in cardinality:
        # create tables & insert values
        v1 = np.random.uniform(0, card[0], card[0])
        v2 = np.random.uniform(2*card[0], 3*card[0], card[1])
        idx1 = list(range(0, card[0]))
        idx2 = list(range(0, card[1]))
        t1 = pd.DataFrame({'v':v1, 'id':idx1})
        t2 = pd.DataFrame({'v':v2, 'id':idx2})
        con.register('t1_view', t1)
        con.execute("create table t1 as select * from t1_view")
        con.register('t2_view', t2)
        con.execute("create table t2 as select * from t2_view")
        # Run query
        q = "select count(*) as c from (select t1.v from t1, t2 {}) as t".format(pred)
        table_name = None
        if args.perm:
            q = "SELECT t1.rowid as t1_rowid, t2.rowid as t2_rowid, t1.v FROM t1, t2 {}".format(pred)
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
        results.append([op, avg, card[0], card[1], output_size, stats, lineage_type, args.notes])
        if args.enable_lineage:
            DropLineageTables(con)
        con.execute("drop table t1")
        con.execute("drop table t2")
    con.execute("PRAGMA set_join='clear'")

def FKPK(con, args, folder, lineage_type, groups, cardinality, a_list, results, op, index_scan):
    print("------------ Test FK-PK")
    if (op == "index_join"):
        con.execute("PRAGMA explain_output = PHYSICAL_ONLY;")
        con.execute("PRAGMA force_index_join")
    for g in groups:
        idx = list(range(0, g))
        PT = pd.DataFrame({'id':idx})
        con.register('PT_view', PT)
        con.execute("create table PT as select * from PT_view")
        fname =  "zipfan_g{}_card{}_a{}.csv"

        for a in a_list:
            for card in cardinality:
                filename = fname.format(g, card, a)
                print(filename, "g", g, "card", card, "a", a, op, index_scan)
                FT = pd.read_csv(folder+filename)
                con.register('FT_view', FT)
                con.execute("create table FT as select * from FT_view")
                q = "select count(*) as c from (SELECT * FROM FT, PT WHERE PT.id=FT.z)"
                if (op == "index_join"):
                    con.execute("create index i_index ON FT using art(z);");
                    if (index_scan):
                        q = "select count(*) as c from (SELECT PT.* FROM PT, FT WHERE PT.id=FT.z) as t"
                    else:
                        q = "select count(t.id), count(t.idx) as c from (SELECT PT.id,FT.idx FROM PT, FT WHERE PT.id=FT.z) as t"
                table_name = None
                if args.perm:
                    q = "SELECT FT.rowid as ft_rowid, PT.rowid as pt_rowid, * FROM FT, PT WHERE FT.z=PT.id"
                    if (op == "index_join"):
                        if (index_scan):
                            q = "SELECT FT.rowid as ft_rowid, PT.rowid as pt_rowid, PT.* FROM FT, PT WHERE FT.z=PT.id"
                        else:
                            q = "SELECT FT.rowid as ft_rowid, PT.rowid as pt_rowid, PT.id, FT.idx FROM FT, PT WHERE FT.z=PT.id"
                    q = "create table perm_lineage as "+ q
                    table_name='perm_lineage'
                avg, df = Run(q, args, con, table_name)
                if args.perm:
                    df = con.execute("select count(*) as c from perm_lineage").fetchdf()
                    output_size = df.loc[0,'c']
                    con.execute("drop table perm_lineage")
                else:
                    output_size = df.loc[0,'c']
                stats = ""
                if args.enable_lineage and args.stats:
                    lineage_size, nchunks, postprocess_time= getStats(con, q)
                    stats = "{},{},{}".format(lineage_size, nchunks, postprocess_time*1000)
                results.append(["{}_pkfk{}".format(op,a), avg, card, g, output_size, stats, lineage_type, args.notes])
                if args.enable_lineage:
                    DropLineageTables(con)
                if (op == "index_join"):
                    con.execute("DROP INDEX i_index")
                con.execute("drop table FT")
        con.execute("drop table PT")

############## many-to-many ##########
# zipf1.z is within [1,10] or [1,100]
# zipf2.z is [1,100]
# left size=1000, right size: 1000 .. 100000
def MtM(con, args, folder, lineage_type, groups, cardinality, results, op, index_scan):
    print("------------ Test Many to Many Join zipfan 1")
    if (op == "index_join"):
        con.execute("PRAGMA explain_output = PHYSICAL_ONLY;")
        con.execute("PRAGMA force_index_join")

    filename = "zipfan_g100_card1000_a1.csv"
    zipf2 = pd.read_csv(folder+filename)
    con.register('zipf2_view', zipf2)
    con.execute("create table zipf2 as select * from zipf2_view")
    if (op == "index_join"):
        con.execute("create index i_index ON zipf2 using art(z);");
    for g in groups:
        for card in cardinality:
            fname =  "zipfan_g{}_card{}_a1.csv"
            filename = fname.format(g, card)
            print(filename, "g", g, "card", card, "a", 1, op, index_scan)
            zipf1 = pd.read_csv(folder+filename)
            con.register('zipf1_view', zipf1)
            con.execute("create table zipf1 as select * from zipf1_view")
            q = "select count(*) as c from (SELECT * FROM zipf1, zipf2 WHERE zipf1.z=zipf2.z)"
            if (index_scan):
                q = "select count(*) as c from (SELECT zipf1.* FROM zipf1, zipf2 WHERE zipf1.z=zipf2.z) as t"
            else:
                q = "select count(t.idx), count(t.v) as c from (SELECT zipf1.idx, zipf2.v FROM zipf1, zipf2 WHERE zipf1.z=zipf2.z) as t"
            table_name = None
            if args.perm:
                q = "SELECT zipf1.rowid as zipf1_rowid, zipf2.rowid as zipf2_rowid, * FROM zipf1, zipf2 WHERE zipf1.z=zipf2.z"
                if (index_scan):
                    q = "SELECT zipf1.rowid as zipf1_rowid, zipf2.rowid as zipf2_rowid, zipf1.* FROM zipf1, zipf2 WHERE zipf1.z=zipf2.z"
                else:
                    q = "SELECT zipf1.rowid as zipf1_rowid, zipf2.rowid as zipf2_rowid, zipf1.idx, zipf2.v FROM zipf1, zipf2 WHERE zipf1.z=zipf2.z"
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
            results.append(["{}_mtm".format(op), avg, card, g, output_size, stats, lineage_type, args.notes])
            if args.enable_lineage:
                DropLineageTables(con)
            con.execute("drop table zipf1")
    if (index_scan):
        con.execute("DROP INDEX i_index")
    con.execute("drop table zipf2")
    return results

def int_hashAggCopies(con, args, folder, lineage_type, copies, cardinality, results, agg_type):
    print("------------ Test Int Group By zipfan 1, ", lineage_type, agg_type)
    con.execute("PRAGMA set_agg='{}'".format(agg_type))
    for card in cardinality:
        for m in copies:
            filename = "m"+str(m)+"copies_card"+str(card)+".csv"
            print(filename, m, card)
            zipf1 = pd.read_csv(folder+filename)
            con.register('input_view', zipf1)
            con.execute("create table input as select * from input_view")
            q = "select count(*) as c from (SELECT m, count(*) FROM input GROUP BY m) as t"
            table_name, method = None, ''
            if args.perm and args.group_concat:
                q = "SELECT m, count(*), group_concat(rowid,',') FROM input GROUP BY m"
                method="_group_concat"
            elif args.perm and args.list:
                q = "SELECT m, count(*), list(rowid) FROM input GROUP BY m"
                method="_list"
            elif args.perm:
                q = "SELECT input.rowid, m FROM (SELECT m, count(*) FROM input  GROUP BY m) join input using (m)"
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
            name = agg_type+"_agg_copies"
            results.append([name, avg, card, m, output_size,stats, lineage_type+method, args.notes])
            if args.enable_lineage:
                DropLineageTables(con)
            con.execute("drop table input")
    con.execute("PRAGMA set_agg='clear'")

