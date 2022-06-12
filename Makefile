SHELL=/bin/bash
PYTHON=.venv/bin/python

.venv:
	@python -m venv .venv
	@.venv/bin/pip install -U pip
	@.venv/bin/pip install --no-cache-dir -r requirements.txt

clean-tpch-dbgen:
	$(MAKE) -C tpch-dbgen clean

clean-venv:
	rm -r .venv

clean-tables:
	rm -r tables_scale_1

clean: clean-tpch-dbgen clean-venv

tables_scale_1: .venv
	$(MAKE) -C tpch-dbgen all
	cd tpch-dbgen && ./dbgen -vf -s 1 && cd ..
	mkdir -p "tables_scale_1"
	mv tpch-dbgen/*.tbl tables_scale_1/
	.venv/bin/python prepare_files.py

run: .venv tables_scale_1
	.venv/bin/python -m polars_queries.q1
	.venv/bin/python -m polars_queries.q2
	.venv/bin/python -m polars_queries.q3
	.venv/bin/python -m polars_queries.q4
	.venv/bin/python -m polars_queries.q5
	.venv/bin/python -m polars_queries.q6

pre-commit:
	.venv/bin/python -m isort .
	.venv/bin/python -m black .