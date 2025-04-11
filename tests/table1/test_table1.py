from src.data_validation.Uniqueness_check import uniqueness_check
from src.data_validation.count_check import count_check
from src.data_validation.data_compare import data_compare
from src.data_validation.duplicate_check import duplicate_validation
from src.data_validation.null_check import null_value_check

def test_count(read_data,read_config):
    source,target = read_data
    source.show()
    target.show()
    read_config = read_config
    keyword_columns = read_config['validations']['count_check']['key_columns']
    status = count_check(source,target,key_columns=keyword_columns)
    assert status == "PASS"

def test_duplicate_validation(read_data,read_config):
    source,target = read_data
    read_config = read_config
    key_cols = read_config['validations']['duplicate_check']['key_columns']
    status = duplicate_validation(df=target,key_cols = key_cols)
    assert status == "PASS"


def test_null_validation(read_data,read_config):
    source,target = read_data
    read_config = read_config
    null_cols = read_config['validations']['null_check']['null_columns']
    status = null_value_check(target,null_cols)
    assert status == 'PASS'

def test_unique_check(read_data,read_config):
    source,target = read_data
    read_config = read_config
    uniq_cols = read_config['validations']['uniqueness_check']['unique_columns']
    status = uniqueness_check(df=target,uniq_cols=uniq_cols)
    assert status == 'PASS'

def test_data_compare_check(read_data,read_config):
    source,target = read_data
    read_config = read_config
    num_records = read_config["validations"]["data_compare_check"]["num_records"]
    status = data_compare(source,target,num_records)
    assert status == 'PASS'