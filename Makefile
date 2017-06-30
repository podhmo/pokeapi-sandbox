TARGET ?= ./data/db.sqlite3

default:
	python bin/gen.py --src "sqlite:///${TARGET}" > data/tables.json
	python bin/convert.py data/tables.json > data/schema
