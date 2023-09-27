create table lineage as (
  SELECT nation, o_year, sum(amount) over (partition by nation, o_year) AS sum_profit,
        part_rowid, 
        supplier_rowid,
        lineitem_rowid, 
        partsupp_rowid,
        orders_rowid, 
        nation_rowid
  FROM (
    SELECT n_name AS nation, extract(year FROM o_orderdate) AS o_year,
           l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity AS amount,
          part.rowid as part_rowid, supplier.rowid as supplier_rowid,
           lineitem.rowid as lineitem_rowid, partsupp.rowid as partsupp_rowid,
           orders.rowid as orders_rowid, nation.rowid as nation_rowid
    FROM part, supplier, lineitem, partsupp, orders, nation
    WHERE s_suppkey = l_suppkey AND ps_suppkey = l_suppkey
        AND ps_partkey = l_partkey AND p_partkey = l_partkey
        AND o_orderkey = l_orderkey AND s_nationkey = n_nationkey
        AND p_name LIKE '%green%'
  )
);
