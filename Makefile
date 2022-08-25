.PHONY: pretty docs

pretty:
	black src tests
	isort src tests

docs:
	yarn --cwd ./docs run dev
