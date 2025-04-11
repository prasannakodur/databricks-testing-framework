from src.utility.report_lib import write_output


def uniqueness_check(df,uniq_cols):
    duplicates = {}
    for column in uniq_cols:
        duplicate_count = df.groupBy(column).count().filter("count > 1").count()
        print("the duplicate count",duplicate_count)

        duplicates[column] = duplicate_count
    print("duplicates are :",duplicates)

    status = 'PASS' if all(count == 0 for count in duplicates.values()) else "FAIL"
    write_output("uniqueness_check:",f"has been {status}",f"duplicate counts for columns"
                                                                  f" are {duplicates}")
    return status











# from src.utility.report_lib import write_output
#
#
# def uniqueness_check(df, uniq_cols):
#     duplicates = {}
#
#     # Check for duplicates for each column in uniq_cols
#     for column in uniq_cols:
#         duplicate_count = df.groupBy(column).count().filter("count > 1").count()
#         print(f"The duplicate count for column {column}: {duplicate_count}")
#         duplicates[column] = duplicate_count  # Store the duplicate count for each column
#
#     print("Duplicates are:", duplicates)
#
#     # Determine status
#     status = 'PASS' if all(count == 0 for count in duplicates.values()) else "FAIL"
#
#     # Prepare detailed output for each column
#     details = "; ".join(
#         [f"for column: {col} and the count is {count}" for col, count in duplicates.items()]
#     )
#
#     # Call write_output with detailed information
#     write_output("uniqueness_check:", f"has been {status}", details)
#
#     return status
