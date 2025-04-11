from src.data_validation.records_only_in_source import records_only_in_source
from src.data_validation.records_only_in_target import records_only_in_target
from src.utility.report_lib import write_output


def count_check(source,target,key_columns):
    source_count = source.count()
    target_count = target.count()

    if source_count == target_count:
        status = 'PASS'
        write_output(details="count is matching between source and target",validation_type="count_check",status=status)
    else:
        status = 'FAIL'
        write_output(details="count is not matching between source and target", validation_type="count_check",
                     status=status)
        # print("count is not matching")
        records_only_in_source(source_df=source,target_df=target,key_columns=key_columns)
        records_only_in_target(source_df=source,target_df=target,key_columns=key_columns)

    return status

# count_check()


