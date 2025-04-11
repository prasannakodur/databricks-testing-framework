from src.utility.report_lib import write_output


def duplicate_validation(df,key_cols):
    duplicate = df.groupBy(key_cols).count().filter("count > 1")
    print('duplicate',duplicate)
    duplicate.show()
    duplicate_count = duplicate.count()
    print('duplicate_count',duplicate_count)

    if duplicate_count > 0:
        failed_records = duplicate.limit(5).collect()
        failed_preview = [row.asDict() for row in failed_records]
        status = 'FAIL'
        write_output('duplicate_validation',status,f"duplicate validation failed with records "
                                                   f"{failed_records} and those are {failed_preview}")
        return status
    else:
        status = 'PASS'
        print('duplicate', duplicate)
        print('duplicate_count', duplicate_count)
        write_output('duplicate validation',status,"there are no duplicates")
        return status
