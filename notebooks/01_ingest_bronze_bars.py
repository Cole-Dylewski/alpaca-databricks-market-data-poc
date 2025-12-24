# Databricks notebook source
# MAGIC %md
# MAGIC # Get S&P 500 Stock Symbols
# MAGIC
# MAGIC Simple notebook to retrieve and display S&P 500 stock symbols from stockanalysis.com.

# COMMAND ----------

import sys
import os
from pathlib import Path
from typing import Optional

# Add project root to Python path for imports
# Works dynamically in local Python, Databricks Repos, and Workspace environments
def add_project_root_to_path() -> str:
    """Add project root directory to sys.path for imports.
    
    Dynamically detects project root regardless of where the repository is cloned.
    Works in:
    - Local Python environments
    - Databricks Repos (/Workspace/Repos/...)
    - Databricks Workspace files (/Workspace/Users/...)
    - Any other location where the repo is cloned
    
    Returns:
        Path to project root directory
    
    Raises:
        RuntimeError: If project root cannot be determined
    """
    project_root: Optional[Path] = None
    
    # Method 1: Try using __file__ (works when running as a Python script)
    try:
        if '__file__' in globals():
            notebook_dir = Path(__file__).parent
            if (notebook_dir / '01_ingest_bronze_bars.py').exists():
                project_root = notebook_dir.parent
    except (NameError, AttributeError):
        pass
    
    # Method 2: Try using dbutils (Databricks-specific)
    # dbutils is only available in Databricks notebooks, not in local Python
    if project_root is None:
        try:
            # Access dbutils which is available as a global in Databricks
            # type: ignore - dbutils is injected by Databricks runtime
            notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()  # type: ignore[name-defined]
            # notebook_path format: /Workspace/Repos/user/repo/notebooks/01_ingest_bronze_bars
            # or /Users/user@domain.com/notebooks/01_ingest_bronze_bars
            notebook_path_obj = Path(notebook_path)
            # Navigate up from notebook file to notebooks/ directory, then to project root
            if notebook_path_obj.name.endswith('.py') or 'notebooks' in notebook_path_obj.parts:
                # Find the notebooks directory in the path
                parts = list(notebook_path_obj.parts)
                if 'notebooks' in parts:
                    notebooks_idx = parts.index('notebooks')
                    project_root = Path(*parts[:notebooks_idx])
                else:
                    # If notebooks not in path, assume parent of notebook file
                    project_root = notebook_path_obj.parent.parent
        except (NameError, AttributeError, Exception):
            # dbutils not available or error accessing it - this is expected in local Python
            pass
    
    # Method 3: Search from current working directory
    if project_root is None or not _is_valid_project_root(project_root):
        current_dir = Path(os.getcwd())
        
        # Quick check: if we're in notebooks/ directory, go up one level
        if current_dir.name == 'notebooks' and _is_valid_project_root(current_dir.parent):
            project_root = current_dir.parent
        else:
            # Search up the directory tree for project root
            project_root = _find_project_root(current_dir)
    
    if project_root is None or not _is_valid_project_root(project_root):
        raise RuntimeError(
            "Could not determine project root. "
            "Please ensure you're running this notebook from within the market_data_poc repository."
        )
    
    project_root_str = str(project_root.resolve())
    if project_root_str not in sys.path:
        sys.path.insert(0, project_root_str)
    return project_root_str


def _is_valid_project_root(path: Path) -> bool:
    """Check if a path is a valid project root.
    
    Args:
        path: Path to check
    
    Returns:
        True if path contains required project structure
    """
    if not path.exists() or not path.is_dir():
        return False
    
    # Must have src/ directory with utils.py
    has_src = (path / 'src' / 'utils.py').exists()
    # Must have notebooks/ directory
    has_notebooks = (path / 'notebooks').exists()
    # Should have requirements.txt (optional but good indicator)
    has_requirements = (path / 'requirements.txt').exists()
    
    # Require at least src and notebooks
    return has_src and has_notebooks


def _find_project_root(start_path: Path, max_depth: int = 15) -> Optional[Path]:
    """Search up directory tree to find project root.
    
    Args:
        start_path: Directory to start searching from
        max_depth: Maximum depth to search
    
    Returns:
        Path to project root if found, None otherwise
    """
    current = start_path.resolve()
    depth = 0
    
    while depth < max_depth and current != current.parent:
        if _is_valid_project_root(current):
            return current
        current = current.parent
        depth += 1
    
    return None


project_root = add_project_root_to_path()
print(f"Added project root to path: {project_root}")

# COMMAND ----------

from src.utils import get_sp500_symbols


# COMMAND ----------

# Run the function
symbols = get_sp500_symbols()
print(f"\n=== S&P 500 Stock Symbols ({len(symbols)} total) ===")
for symbol in symbols:
    print(symbol)