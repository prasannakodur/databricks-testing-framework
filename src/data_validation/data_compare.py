from pyspark.sql.functions import col,lit
from src.utility.report_lib import write_output


def data_compare(source,target,num_records):
    smt = source.exceptAll(target).withColumn("origin",lit("source_record"))
    print("source_minus_target is : " , smt)
    tms = target.exceptAll(source).withColumn("origin",lit("target_record"))
    print("target_minus_source is : ", smt)
    failed = smt.union(tms)
    failed_count = failed.count()
    print("failed_count is : ",failed_count)

    if failed_count > 0:
        failed_records = failed.limit(num_records).collect()
        failed_preview = '\n'.join([str(row.asDict()) for row in failed_records])
        # for row in failed_preview:
        #     print(row)
        status = 'FAIL'
        write_output("data_compare_check",
                     status,
                     f"the data mismatches are: {failed_preview}")
        return status
    else:
        status = 'PASS'
        write_output("data_compare_check",
                     status,
                     "there are no data discrepancy")
        return status
