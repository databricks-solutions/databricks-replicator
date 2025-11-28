#!/usr/bin/env python3
"""
Main CLI entry point for the data replication system.

This module provides a lightweight CLI wrapper that defers heavy imports
until the command is actually executed.
"""

import sys


def main():
    """Main CLI entry point that defers heavy imports."""
    try:
        # Import the actual main function only when needed
        from data_replication.main import main as main_impl

        return main_impl()
    except ImportError as e:
        print(f"Error importing data replication modules: {e}", file=sys.stderr)
        print(
            "Make sure all dependencies are installed correctly.",
            file=sys.stderr,
        )
        return 1
    except Exception as e:
        print(f"Error running data replication: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
