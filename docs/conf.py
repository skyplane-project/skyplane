# Configuration file for the Sphinx documentation builder.
# borrowed from JAX https://github.com/google/jax/blob/main/docs/conf.py

# -- Path setup --------------------------------------------------------------

# If extensions (or modules to document with autodoc) are in another directory,
# add these directories to sys.path here. If the directory is relative to the
# documentation root, use os.path.abspath to make it absolute, like shown here.
#
import os
import sys

sys.path.insert(0, os.path.abspath('..'))

# -- Project information

project = 'Skyplane'
copyright = '2021, The Skyplane authors'
author = 'The Skyplane authors'

release = '0.1'
version = '0.1.0'

# -- General configuration

extensions = [
    'myst_nb',
    'sphinx_autodoc_typehints',
    'sphinx_copybutton',
    'sphinx.ext.autodoc',
    'sphinx.ext.autosummary',
    'sphinx.ext.doctest',
    'sphinx.ext.duration',
    'sphinx.ext.intersphinx',
    'sphinx.ext.mathjax',
    'sphinx.ext.napoleon',
    'sphinx.ext.viewcode',
    # 'matplotlib.sphinxext.plot_directive',
]

intersphinx_mapping = {
    'python': ('https://docs.python.org/3/', None),
    'sphinx': ('https://www.sphinx-doc.org/en/master/', None),
}
intersphinx_disabled_domains = ['std']

templates_path = ['_templates']

# md support
extensions = ['myst_parser']
source_suffix = ['.rst', '.md']

# -- Options for HTML output

html_theme = 'sphinx_book_theme'
# Theme options are theme-specific and customize the look and feel of a theme
# further.  For a list of options available for each theme, see the
# documentation.
# html_theme_options = {
#     'logo_only': True,
#     'show_toc_level': 2,
# }
# html_logo = '_static/jax_logo_250px.png'
# html_favicon = '_static/favicon.png'
# html_static_path = ['_static']


# -- Options for EPUB output
epub_show_urls = 'footnote'
