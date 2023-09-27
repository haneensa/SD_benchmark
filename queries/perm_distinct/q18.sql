SELECT  distinct c_name,  c_custkey,  o_orderkey,  o_orderdate,  o_totalprice,  sum_l_quantity
from lineage
ORDER BY
    o_totalprice DESC,
    o_orderdate;
