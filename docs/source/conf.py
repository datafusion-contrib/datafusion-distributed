# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

import os
import sys

# -- Project information -----------------------------------------------------

project = "DataFusion Distributed"
copyright = "2025, DataFusion Distributed Contributors"
author = "DataFusion Distributed Contributors"

# -- General configuration ---------------------------------------------------

# Add any Sphinx extension module names here, as strings.
extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.autosummary",
    "sphinx.ext.viewcode",
    "myst_parser",
    "sphinx_reredirects",
]

source_suffix = {
    ".rst": "restructuredtext",
    ".md": "markdown",
}

# Add any paths that contain templates here, relative to this directory.
templates_path = ["_templates"]

# List of patterns, relative to source directory, that match files and
# directories to ignore when looking for source files.
exclude_patterns = []

# -- Options for HTML output -------------------------------------------------

# The theme to use for HTML and HTML Help pages.
html_theme = "pydata_sphinx_theme"

html_theme_options = {
    "use_edit_page_button": True,
    "navbar_center": [],
    "navbar_end": ["theme-switcher"],
}

html_context = {
    "github_user": "YOUR_GITHUB_USER",
    "github_repo": "datafusion-distributed",
    "github_version": "main",
    "doc_path": "docs/source",
}

# Add any paths that contain custom static files (such as style sheets) here,
# relative to this directory.
html_static_path = ["_static"]

html_css_files = ["theme_overrides.css"]

html_sidebars = {
    "**": ["docs-sidebar.html"],
}

# tell myst_parser to auto-generate anchor links for headers h1, h2, h3
myst_heading_anchors = 3

# enable nice rendering of checkboxes for the task lists
myst_enable_extensions = ["colon_fence", "deflist", "tasklist"]

# Suppress highlighting warnings
suppress_warnings = ["misc.highlighting_failure"]

# Configure redirects (if needed)
redirects = {}
