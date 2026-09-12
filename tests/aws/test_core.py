import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock
import pytest
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / 'aws'))
from preview_core import identifier, owned_run
from run import request, wait

@pytest.mark.parametrize('bad', ['main', 'pr_0_abcdef123456_r1', 'pr_1_abcdef123456_r0', 'pr_1_abcdef123456_r1;drop', '../x'])
def test_cleanup_rejects_unowned_names(bad):
    with pytest.raises(ValueError): owned_run(bad)

def test_identifier_rejects_injection():
    with pytest.raises(ValueError): identifier('db; DROP TABLE x')

def test_job_contract():
    a=SimpleNamespace(bucket='example-bucket',application='app',role='arn:aws:iam::123456789012:role/emr',namespace='lab',run='pr_1_abcdef123456_r1')
    payload=request(a,'preview')
    assert payload['executionTimeoutMinutes']==20
    assert '/usr/share/aws/iceberg/lib/iceberg-spark3-runtime.jar' in payload['jobDriver']['sparkSubmit']['sparkSubmitParameters']
    assert 'access-key' not in str(payload)
    assert request(a,'cleanup')['clientToken'] != payload['clientToken']

def test_failed_emr_job_fails_controller():
    api=Mock();api.get_job_run.return_value={'jobRun':{'state':'FAILED','stateDetails':'Spark error'}}
    with pytest.raises(RuntimeError,match='Spark error'): wait(api,'app','job',interval=0)

def test_successful_emr_job_returns():
    api=Mock();api.get_job_run.return_value={'jobRun':{'state':'SUCCESS'}}
    assert wait(api,'app','job',interval=0)['state']=='SUCCESS'
