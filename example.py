import pandas as pd
from sqlalchemy import create_engine

import pandas_sql_method
import model_example

conn = create_engine("postgresql://scott:tiger@localhost/test")
model_example.Base.metadata.create_all(conn)

df = pd.read_csv(r'users.csv')

df.to_sql(
    name='mytable', 
    con=conn, 
    schema='my_schema',
    if_exists='append', 
    index=False, 
    method=lambda pd_table, conn, keys, data_iter: 
        pandas_sql_method.insert_on_duplicate(model, pd_table, conn, keys, data_iter)
)
