SELECT distinct c_custkey, c_name, revenue, c_acctbal, n_name, c_address, c_phone, c_comment
from lineage
ORDER BY revenue DESC;
