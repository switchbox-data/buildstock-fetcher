#! /usr/bin/env bash

# Install uv
curl -LsSf https://astral.sh/uv/install.sh | sh

# Install Dependencies
uv sync

# Install pre-commit hooks
git config --global --add safe.directory /workspaces/buildstock-fetch # Needed for pre-commit install to work
uv run pre-commit install --install-hooks
