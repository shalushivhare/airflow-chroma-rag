.PHONY: test up down logs query

test:
	PYTHONPATH=src python -m pytest

up:
	docker compose up --build -d

down:
	docker compose down

logs:
	docker compose logs -f airflow rag-api chroma

query:
	curl -s http://localhost:8001/query \
	  -H 'content-type: application/json' \
	  -d '{"question":"What is Apache Airflow used for?","top_k":4}' | python -m json.tool
