default:
	python bin/gen.py --src "sqlite:///./data/db.sqlite3" > data/tables.json
	python bin/convert.py data/tables.json > data/schema
