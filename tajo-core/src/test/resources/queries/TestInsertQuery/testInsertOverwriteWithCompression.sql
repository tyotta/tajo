insert overwrite into testInsertOverwriteWithCompression select  l_orderkey, l_partkey, l_quantity from default.lineitem where l_orderkey = 3;