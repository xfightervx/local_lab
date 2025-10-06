import os
import sys
import pathspec

# Make sure your project root is accessible
sys.path.insert(0, os.path.abspath('../../../..'))

extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.autosummary",
    "sphinx.ext.napoleon",
    "sphinx_autodoc_typehints",
    "myst_parser",
    "autoapi.extension",
]

# --- AutoAPI configuration ---
autoapi_type = "python"
autoapi_dirs = ["../../../.."]
autoapi_keep_files = True
autoapi_add_toctree_entry = True
autoapi_root = "api"

# --- Markdown support ---
source_suffix = {
    '.rst': 'restructuredtext',
    '.md': 'markdown',
}

master_doc = "index"
autoapi_ignore = ["/home/souhail/local_lab/.venv"]
