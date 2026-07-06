# DataFusion Distributed Documentation

This directory contains the documentation for DataFusion Distributed, built using [Sphinx](https://www.sphinx-doc.org/).

## Building the Documentation

### Prerequisites

Install the required dependencies:

```bash
pip install -r requirements.txt
```

### Build HTML Documentation

```bash
make html
```

The generated documentation will be available in `build/html/index.html`.

### Clean Build Files

```bash
make clean
```

### Live Preview While Editing

For active development, use `sphinx-autobuild` (included in `requirements.txt`) to rebuild and refresh your browser automatically on every save:

```bash
sphinx-autobuild source build/html
```

This starts a local server (default `http://127.0.0.1:8000`) and live-reloads the page whenever a source file changes.

## Documentation Structure

- `source/` - Documentation source files (reStructuredText and Markdown)
  - `user-guide/` - User-facing documentation
  - `architecture/` - Architecture documentation
  - `contributor-guide/` - Contributor documentation
  - `_static/` - Static files (images, CSS, etc.)
  - `_templates/` - Custom templates

## Contributing

When adding new documentation:

1. Create new `.md` or `.rst` files in the appropriate subdirectory
2. Add references to new files in the relevant `index.rst` or `index.md`
3. Build and preview your changes locally
4. Ensure all links and references work correctly
