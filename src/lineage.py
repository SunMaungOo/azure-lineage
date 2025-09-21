from sqlglot.expressions import Expression
from sqlglot import parse_one,exp
from typing import Set
import sqlglot



def get_sql_lineage(sql:str,dialect:str=None)->Set[str]:
    """
    Get the set of base table (excluding CTE)
    """

    ast = None

    if dialect is None:
        ast = parse_one(sql=sql)
    else:
        ast = parse_one(sql=sql,dialect=dialect)
    

    return find_base_tables(ast)

def find_base_tables(ast:Expression)->Set[str]:
    
    ctes:Set[str] = set()

    tables:Set[str] = set()

    for cte in ast.find_all(exp.CTE):
        ctes.add(cte.alias_or_name)

    for table in ast.find_all(exp.Table):
        #ignore if the table is the cte
        if table.name not in ctes:
            tables.add(internal_get_table_name(table))

    return tables

def internal_add_surfix(text:str,surfix:str,false_value:str)->str:
    if text is None:
        return false_value
    
    return text+surfix

def internal_get_table_name(table_expression:Expression)->str:
    """
    From the table_expression , try to get the table name in 
    database.schema.table format
    """

    database_name = None

    schema_name = None

    table_name = table_expression.name

    if "db" in table_expression.args.keys() and table_expression.args["db"] is not None:
        schema_name = table_expression.args["db"].name

    if "catalog" in table_expression.args.keys() and table_expression.args["catalog"] is not None:
        database_name = table_expression.args["catalog"].name

    return internal_add_surfix(database_name,".","") + internal_add_surfix(schema_name,".","") + table_name
