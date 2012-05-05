from pycassa.pool import ConnectionPool
from pycassa.columnfamily import ColumnFamily

pool = ConnectionPool('test', ['137.112.150.61:9160'])
col_fam = pycassa.ColumnFamily(pool, 'users')
col_fam.insert('row_key', {'id' : 'andrew', 'age' : '20', 'grade' : 'A'})