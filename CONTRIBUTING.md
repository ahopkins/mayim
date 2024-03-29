# Contributing to 💧 Mayim

## Code style

Mayim uses `black` and `isort`. If you do not have them configured to run automatically in your IDE, you can apply them before committing your changes with a single `make` command:

```
make pretty
```

## Testing

```
# Install tox
pip install tox

# Run the unit tests
tox -e py310

# Run the linters and type checker
tox -e check
```

## Documentation

The documentation is built using [VuePress](https://vuepress.vuejs.org/). There are two aspects to the documentation:

- The user guide [./docs/src/guide](https://github.com/ahopkins/mayim/tree/main/docs/src/guide)
- The API docs [./docs/src/api](https://github.com/ahopkins/mayim/tree/main/docs/src/api)

The API docs are autogenerated from the docstrings. Mayim follows the [Google docstring](https://google.github.io/styleguide/pyguide.html#38-comments-and-docstrings) convention. To build the API docs, install the required packages into your virtual environment:

```
pip install -e ".[docs]"
```

Then, run the build script:

```
python build_api_docs.py
```

This will generate the markdown and JS files needed to build the API docs.

To run the documentation server locally, go into the `./docs` dir and install the requirements.

```
npm install
# or
yarn
```

Next, run the dev server

```
npm run dev
# or
yarn dev
```

## Questions?

Post in [Discussions](https://github.com/ahopkins/mayim/discussions) or find me on Discord. I'm pretty visible on the [Sanic Discord server](https://discord.gg/FARQzAEMAA).
