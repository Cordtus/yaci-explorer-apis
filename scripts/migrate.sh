#!/bin/bash
# SQL Migration Runner for YACI Explorer APIs
# Usage: ./scripts/migrate.sh [--dry-run]

set -e

MIGRATIONS_DIR="$(dirname "$0")/../migrations"
DRY_RUN=false

if [[ "$1" == "--dry-run" ]]; then
	DRY_RUN=true
	echo "DRY RUN: No changes will be applied"
fi

if [[ -z "$DATABASE_URL" ]]; then
	echo "Error: DATABASE_URL environment variable is required"
	exit 1
fi

echo "Running migrations from: $MIGRATIONS_DIR"

for file in "$MIGRATIONS_DIR"/*.sql; do
	if [[ -f "$file" ]]; then
		echo "Applying: $(basename "$file")"
		if [[ "$DRY_RUN" == "false" ]]; then
			psql "$DATABASE_URL" -f "$file"
		fi
	fi
done

echo "Migration complete"
