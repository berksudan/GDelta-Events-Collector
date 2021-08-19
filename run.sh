#!/bin/bash

# Change current directory to project directory.
cd "$(dirname "$0")" || exit

# Paths of Python 3 and main module.
PYTHON_BIN=venv/bin/python3
PYTHON_MAIN=src/main.py

# Run the program.
$PYTHON_BIN "$PYTHON_MAIN"
