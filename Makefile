test:
	python -m pytest tests/aws/test_core.py -q

cfn:
	cfn-lint aws/template.yaml
