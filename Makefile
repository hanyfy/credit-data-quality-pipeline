.PHONY: test data run docker
test:
	pytest -q
data:
	PYTHONPATH=src python -m dq.generate --rows 1000000
run:
	PYTHONPATH=src python -m dq.run
docker:
	docker build -t credit-dq . && docker run --rm credit-dq
