from sqlglot import select
from sqlglot import exp

query = select("column1", "column2").from_("my_table").where(exp.column("column1").eq(1))
e6data_sql = query.sql(dialect="e6data")
print(e6data_sql)