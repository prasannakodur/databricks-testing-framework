from pyspark.sql.functions import *
from src.utility.report_lib import write_output


def null_value_check(df,null_cols):
    failures = []
    for column in null_cols:
        print("columns",column)
        failure_rows = df.filter((col(column).isNull()) | (trim(col(column)) == ""))
        failure_count = failure_rows.count()
        print("count is ",failure_count)


        if failure_count > 0:
            failure_records = failure_rows.limit(5).collect() #this will collect the first 5 rows in list
            failure_preview = [row.asDict() for row in failure_records] # this will print the rows in dict format
            print("failed preview",failure_preview)
            failures.append({"columns":column,"failure_count":failure_count,"null values":failure_preview})
            print("failures",failures)



    if failures:
        status = 'FAIL'
        write_output("null_validation",status,f"failed records are :{failures}")
        return status
    else:
        status = 'PASS'
        write_output("null_validation",status,"no failures")
        return status