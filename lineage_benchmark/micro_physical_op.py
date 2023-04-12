import random
import os.path
import duckdb
import pandas as pd
import argparse
import csv
import json

import numpy as np

from utils import getStats, MicroDataZipfan, MicroDataSelective, DropLineageTables, MicroDataMcopies,  Run

def gettimings(plan, res={}):
    for c in  plan['children']:
        op_name = c['name']
        timing = c['timing']
        res[op_name + str(len(res))] = timing
        gettimings(c, res)
    return res

def PersistResults(results, filename, append):
    print("Writing results to ", filename, " Append: ", append)
    header = ['r', "query", "p", "runtime", "cardinality", "groups", "output", "stats", "lineage_type", "notes", "plan_timings"]
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
    projections = [0, 2, 4, 8] 

    for card in cardinality:
        for g in groups:
            for p in projections:
                filename = "zipfan_g"+str(g)+"_card"+str(card)+"_a1.csv"
                zipf1 = pd.read_csv(folder+filename)
                for col in range(p):
                    zipf1["col{}".format(col)]  = np.random.randint(0, 100, len(zipf1))
                perm_rid = ''
                if args.perm:
                    perm_rid = 'zipf1.rowid as rid,'

                con.register('zipf1_view', zipf1)
                con.execute("create table zipf1 as select * from zipf1_view")
                args.qid='Scan_ltype{}g{}card{}p{}'.format(lineage_type,g, card, p)
                for r in range(args.r):
                    print(filename, p, g, card)
                    q = "SELECT {}* FROM zipf1".format(perm_rid)
                    q = "create table zipf1_perm_lineage as "+ q
                    table_name='zipf1_perm_lineage'
                    avg, df = Run(q, args, con, table_name)
                    df = con.execute("select count(*) as c from zipf1_perm_lineage").fetchdf()
                    output_size = df.loc[0,'c']
                    con.execute("drop table zipf1_perm_lineage")
                    stats = ""
                    if args.enable_lineage and args.stats:
                        lineage_size, nchunks, postprocess_time= getStats(con, q)
                        stats = "{},{},{}".format(lineage_size, nchunks, postprocess_time*1000)
                    plan_fname = '{}_plan.json'.format(args.qid)
                    with open(plan_fname, 'r') as f:
                        plan = json.load(f)
                        print(plan)
                        
                        plan_timings = gettimings(plan, {})
                        print(plan_timings)
                    os.remove(plan_fname)
                    results.append([r, "scan", p, avg, card, g, output_size, stats, lineage_type, args.notes, plan_timings])
                    if args.enable_lineage:
                        DropLineageTables(con)
                con.execute("drop table zipf1")

def OrderByMicro(con, args, folder, lineage_type, groups, cardinality, results):
    print("------------ Test Order By zipfan 1", lineage_type)
    projections = [0, 2, 4, 8] 
    for g in groups:
        for card in cardinality:
            for p in projections:
                args.qid='OB_ltype{}g{}card{}p{}'.format(lineage_type,g, card, p)
                filename = "zipfan_g"+str(g)+"_card"+str(card)+"_a1.csv"
                zipf1 = pd.read_csv(folder+filename)
                proj_ids = 'idx'
                for col in range(p):
                    zipf1["col{}".format(col)]  = np.random.randint(0, 100, len(zipf1))
                perm_rid = ''
                if args.perm:
                    perm_rid = 'zipf1.rowid as rid,'

                con.register('zipf1_view', zipf1)
                con.execute("create table zipf1 as select * from zipf1_view")
                for r in range(args.r):
                    print(r, filename, p, g, card)
                    q = "SELECT {}* FROM zipf1 Order By z".format(perm_rid)
                    table_name = None
                    q = "create table zipf1_perm_lineage as "+ q
                    table_name='zipf1_perm_lineage'
                    avg, df = Run(q, args, con, table_name)
                    df = con.execute("select count(*) as c from zipf1_perm_lineage").fetchdf()
                    output_size = df.loc[0,'c']
                    con.execute("drop table zipf1_perm_lineage")
                    stats = ""
                    if args.enable_lineage and args.stats:
                        lineage_size, nchunks, postprocess_time= getStats(con, q)
                        stats = "{},{},{}".format(lineage_size, nchunks, postprocess_time*1000)
                    plan_fname = '{}_plan.json'.format(args.qid)
                    with open(plan_fname, 'r') as f:
                        plan = json.load(f)
                        print(plan)
                        
                        plan_timings = gettimings(plan, {})
                        print(plan_timings)
                    os.remove(plan_fname)
                    results.append([r, "orderby", p, avg, card, g, output_size, stats, lineage_type, args.notes, plan_timings])
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
    projections = [0, 2, 4, 8] 
    for sel in selectivity:
        for card in cardinality:
            for p in projections:
                args.qid='Filter_ltype{}g{}card{}p{}'.format(lineage_type,sel, card, p)
                filename = "filter_sel"+str(sel)+"_card"+str(card)+".csv"
                t1 = pd.read_csv(folder+filename)
                for col in range(p):
                    t1["col{}".format(col)]  = np.random.randint(0, 100, len(t1))
                perm_rid = ''
                if args.perm:
                    perm_rid = 't1.rowid as rid,'

                con.register('t1_view', t1)
                con.execute("create table t1 as select * from t1_view")
                for r in range(args.r):
                    print(r, filename, p, sel, card)
                    q = "SELECT {}* FROM t1 where z=0".format(perm_rid)
                    table_name = None
                    q = "create table t1_perm_lineage as "+ q
                    table_name='t1_perm_lineage'
                    avg, df = Run(q, args, con, table_name)
                    df = con.execute("select count(*) as c from t1_perm_lineage").fetchdf()
                    output_size = df.loc[0,'c']
                    con.execute("drop table t1_perm_lineage")
                    stats = ""
                    if args.enable_lineage and args.stats:
                        lineage_size, nchunks, postprocess_time= getStats(con, q)
                        stats = "{},{},{}".format(lineage_size, nchunks, postprocess_time*1000)
                    name = "filter"
                    if (pushdown == "clear"):
                        name += "_scan"
                    
                    plan_fname = '{}_plan.json'.format(args.qid)
                    with open(plan_fname, 'r') as f:
                        plan = json.load(f)
                        print(plan)
                        
                        plan_timings = gettimings(plan, {})
                        print(plan_timings)
                    os.remove(plan_fname)
                    results.append([r, name, p, avg, card, sel, output_size, stats, lineage_type, args.notes, plan_timings])
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
    p  = 2
    for g in groups:
        for card in cardinality:
            args.qid='gb_ltype{}g{}card{}p{}'.format(lineage_type,g, card, p)
            filename = "zipfan_g"+str(g)+"_card"+str(card)+"_a1.csv"
            zipf1 = pd.read_csv(folder+filename)
            con.register('zipf1_view', zipf1)
            con.execute("create table zipf1 as select * from zipf1_view")
            for r in range(args.r):
                print(r, filename, g, card)
                q = "SELECT z, count(*) as agg FROM zipf1 GROUP BY z"
                table_name, method = None, ''
                if args.perm and args.group_concat:
                    q = "SELECT z, count(*) as c, group_concat(rowid,',') FROM zipf1 GROUP BY z"
                    method="_group_concat"
                elif args.perm and args.list:
                    q = "SELECT z, count(*) as c , list(rowid) FROM zipf1 GROUP BY z"
                    method="_list"
                elif args.perm and args.mat:
                    q = "SELECT z, count(*) as c FROM zipf1 GROUP BY z) join zipf1 using (z)"
                elif args.perm:
                    q = "SELECT zipf1.rowid as rid, z, c FROM (SELECT z, count(*) as c FROM zipf1 GROUP BY z) join zipf1 using (z)"
                q = "create table zipf1_perm_lineage as "+ q
                table_name='zipf1_perm_lineage'
                avg, df = Run(q, args, con, table_name)
                df = con.execute("select count(*) as c from zipf1_perm_lineage").fetchdf()
                output_size = df.loc[0,'c']
                con.execute("drop table zipf1_perm_lineage")
                stats = ""
                if args.enable_lineage and args.stats:
                    lineage_size, nchunks, postprocess_time= getStats(con, q)
                    stats = "{},{},{}".format(lineage_size, nchunks, postprocess_time*1000)
                name = agg_type+"_agg"
                plan_fname = '{}_plan.json'.format(args.qid)
                with open(plan_fname, 'r') as f:
                    plan = json.load(f)
                    print(plan)
                    plan_timings = gettimings(plan, {})
                    print('X', plan_timings)
                os.remove(plan_fname)
                results.append([r, name, p, avg, card, g, output_size,stats, lineage_type+method, args.notes, plan_timings])
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
    p = 2
    for g in groups:
        for card in cardinality:
            args.qid='gb_ltype{}g{}card{}p{}'.format(lineage_type,g, card, p)
            filename = "zipfan_g"+str(g)+"_card"+str(card)+"_a1.csv"
            zipf1 = pd.read_csv(folder+filename)
            zipf1 = zipf1.astype({"z": str})

            con.register('zipf1_view', zipf1)
            con.execute("create table zipf1 as select * from zipf1_view")
            for r in range(args.r):
                print(r, filename, g, card)
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
                q = "create table zipf1_perm_lineage as "+ q
                table_name='zipf1_perm_lineage'
                avg, df = Run(q, args, con, table_name)
                df = con.execute("select count(*) as c from zipf1_perm_lineage").fetchdf()
                output_size = df.loc[0,'c']
                con.execute("drop table zipf1_perm_lineage")
                stats = ""
                if args.enable_lineage and args.stats:
                    lineage_size, nchunks, postprocess_time= getStats(con, q)
                    stats = "{},{},{}".format(lineage_size, nchunks, postprocess_time*1000)
                plan_fname = '{}_plan.json'.format(args.qid)
                with open(plan_fname, 'r') as f:
                    plan = json.load(f)
                    print(plan)
                    
                    plan_timings = gettimings(plan, {})
                    print(plan_timings)
                os.remove(plan_fname)
                results.append([r, "groupby", p, avg, card, g, output_size, stats, lineage_type+method, args.notes, plan_timings])
                if args.enable_lineage:
                    DropLineageTables(con)
            con.execute("drop table zipf1")

################### Joins ###########################
def join_lessthan(con, args, folder, lineage_type, cardinality, results, op, force_join, pred, sels=[0.0]):
    print("------------ Test Join  ", op, pred, force_join)
    if (force_join):
        con.execute("PRAGMA set_join='{}'".format(op))
    projections = [0] 
    for sel in sels:
        for card in cardinality:
            for p in projections:
                # create tables & insert values
                v1 = np.random.uniform(0, card[0], card[0])
                k = card[0] * sel
                # pick k random indexes between 0 and card[0]
                # replace them with values outside the range
                IDX = random.sample(range(card[0]+1), int(k))
                v1_new = [10*card[0] if i in IDX else v1[i] for i in range(len(v1))]
                v1 = v1_new
                v2 = np.random.uniform(2*card[0], 3*card[0], card[1])
                idx1 = list(range(0, card[0]))
                idx2 = list(range(0, card[1]))
                t1 = pd.DataFrame({'v':v1, 'id':idx1})
                t2 = pd.DataFrame({'v':v2, 'id':idx2})
                proj_ids = 'v+id'
                for col in range(p):
                    t1["col{}".format(col)]  = np.random.randint(0, 100, len(t1))
                    t2["col{}".format(col)]  = np.random.randint(0, 100, len(t2))
                perm_rid = ''
                if args.perm:
                    perm_rid = "t1.rowid as r1_rowid, t2.rowid as t2_rowid, "
                con.register('t1_view', t1)
                con.execute("create table t1 as select * from t1_view")
                con.register('t2_view', t2)
                con.execute("create table t2 as select * from t2_view")
                for r in range(args.r):
                    args.qid='ineq_ltype{}g{}card{}p{}'.format(lineage_type,sel, card, p)
                    print(r, "sel: ", sel, "card", card, "p", p)
                    # Run query
                    q = "select {}* from t1, t2 {}".format(perm_rid, pred)
                    table_name = None
                    q = "create table zipf1_perm_lineage as "+ q
                    table_name='zipf1_perm_lineage'
                    avg, df = Run(q, args, con, table_name)
                    df = con.execute("select count(*) as c from zipf1_perm_lineage").fetchdf()
                    output_size = df.loc[0,'c']
                    con.execute("drop table zipf1_perm_lineage")
                    stats = ""
                    if args.enable_lineage and args.stats:
                        lineage_size, nchunks, postprocess_time= getStats(con, q)
                        stats = "{},{},{}".format(lineage_size, nchunks, postprocess_time*1000)
                    plan_fname = '{}_plan.json'.format(args.qid)
                    with open(plan_fname, 'r') as f:
                        plan = json.load(f)
                        print(plan)
                        
                        plan_timings = gettimings(plan, {})
                        print('X', plan_timings)
                    os.remove(plan_fname)
                    results.append([r, op, p, avg, card[0], "{},{}".format(card[1], sel), output_size, stats, lineage_type, args.notes, plan_timings])
                    if args.enable_lineage:
                        DropLineageTables(con)
                con.execute("drop table t1")
                con.execute("drop table t2")
    con.execute("PRAGMA set_join='clear'")

def FKPK(con, args, folder, lineage_type, groups, cardinality, a_list, results, op, index_scan):
    print("------------ Test FK-PK ", op, index_scan)
    if (op == "index_join"):
        con.execute("PRAGMA explain_output = PHYSICAL_ONLY;")
        con.execute("PRAGMA force_index_join")
    p = 2
    for g in groups:
        idx = list(range(0, g))
        vals = np.random.uniform(0, 100, g)
        PT = pd.DataFrame({'id':idx, 'v':vals})
        con.register('PT_view', PT)
        con.execute("create table PT as select * from PT_view")
        fname =  "zipfan_g{}_card{}_a{}{}.csv"
        sels = [0, 0.2, 0.5, 0.8]

        for a in a_list:
            for sel in sels:
                for card in cardinality:
                    args.qid='ineq_ltype{}g{}card{}p{}'.format(lineage_type,g, card, p, a)
                    filename = fname.format(g, card, a, '_sel'+str(sel))
                    k = g * sel

                    if not os.path.exists(folder+filename):
                        filename_orig = fname.format(g, card, a, '')
                        FT = pd.read_csv(folder+filename_orig)
                        
                        for i in range(int(k)):
                            FT['z'] = FT['z'].replace(i, g*100)
                        
                        FT.to_csv(folder+filename, index=False)
                    else:
                        FT = pd.read_csv(folder+filename)
                    
                    con.register('FT_view', FT)
                    con.execute("create table FT as select * from FT_view")
                    if (op == "index_join"):
                        con.execute("create index i_index ON FT using art(z);");
                    for r in range(args.r):
                        print(filename, "k", k, "g", g, "card", card, "a", a, op, index_scan)
                        perm_rid = ''
                        if args.perm:
                            perm_rid = "FT.rowid as FT_rowid, PT.rowid as PT_rowid,"
                        if (op == "index_join"):
                            if (index_scan):
                                q = "SELECT {}PT.id, PT.v FROM PT, FT WHERE PT.id=FT.z".format(perm_rid)
                            else:
                                q = "SELECT {}PT.id, FT.v FROM PT, FT WHERE PT.id=FT.z".format(perm_rid)
                        else:
                            q = "SELECT {}FT.v, PT.id FROM FT, PT WHERE PT.id=FT.z".format(perm_rid)
                        table_name = None
                        q = "create table perm_lineage as "+ q
                        table_name='perm_lineage'
                        avg, df = Run(q, args, con, table_name)
                        df = con.execute("select count(*) as c from perm_lineage").fetchdf()
                        output_size = df.loc[0,'c']
                        con.execute("drop table perm_lineage")
                        stats = ""
                        if args.enable_lineage and args.stats:
                            lineage_size, nchunks, postprocess_time= getStats(con, q)
                            stats = "{},{},{}".format(lineage_size, nchunks, postprocess_time*1000)
                        plan_fname = '{}_plan.json'.format(args.qid)
                        with open(plan_fname, 'r') as f:
                            plan = json.load(f)
                            print(plan)
                            
                            plan_timings = gettimings(plan, {})
                            print(plan_timings)
                        os.remove(plan_fname)
                        results.append([r, "{}_pkfk".format(op),p,  avg, card, "{},{},{},{}".format(g, sel,a,index_scan), output_size, stats, lineage_type, args.notes, plan_timings])
                        if args.enable_lineage:
                            DropLineageTables(con)
                    if (op == "index_join"):
                        con.execute("DROP INDEX i_index")
                    con.execute("drop table FT")
        con.execute("drop table PT")


