
all: shell

shell: 
	pipenv shell

venv:
	python -m pip install pipenv
	pipenv install
test:
	pipenv run python -m unittest discover .
lint:
	pipenv run pylint *.py
black: 
	pipenv run black *.py 
clean:
	rm -rf output

.PHONY: all shell venv test lint black