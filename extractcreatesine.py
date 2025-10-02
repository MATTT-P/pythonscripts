#!/usr/bin/env python3
import os
import re
import shutil
import sys

create_start_re = re.compile(r'^\s*(CREATE(\s+OR\s+REPLACE)?\b)', re.I)
delimiter_re = re.compile(r'^\s*DELIMITER\s+(.+)$', re.I)
dollar_open_re = re.compile(r'\$[A-Za-z0-9_]*\$')  # matches $$ or $tag$

def extract_creates(file_path):
    """
    Extract CREATE statements from a .sql file.
    Handles MySQL DELIMITER and PostgreSQL dollar-quoted bodies.
    Rewrites CREATE -> CREATE IF NOT EXISTS.
    """
    current_delim = ';'
    accum = []
    recording = False
    dollar_tag = None
    statements = []

    with open(file_path, 'r', encoding='utf-8', errors='replace') as f:
        for raw_line in f:
            line = raw_line

            m = delimiter_re.match(line)
            if m:
                current_delim = m.group(1)
                continue

            if not recording:
                if create_start_re.match(line):
                    recording = True
                    accum.append(line)
                    d = dollar_open_re.search(line)
                    if d:
                        dollar_tag = d.group(0)
                        if line.count(dollar_tag) % 2 == 0:
                            dollar_tag = None
                    continue
                else:
                    continue
            else:
                accum.append(line)

                if dollar_tag:
                    if dollar_tag in line and line.count(dollar_tag) % 2 == 1:
                        dollar_tag = None
                    continue

                d = dollar_open_re.search(line)
                if d:
                    dollar_tag = d.group(0)
                    if line.count(dollar_tag) % 2 == 0:
                        dollar_tag = None
                    continue

                if line.rstrip().endswith(current_delim):
                    if current_delim != ';':
                        accum[-1] = accum[-1].rstrip()
                        if accum[-1].endswith(current_delim):
                            accum[-1] = accum[-1][:-len(current_delim)]
                        accum[-1] = accum[-1].rstrip() + "\n"
                    stmt = ''.join(accum).rstrip()
                    stmt = rewrite_create(stmt)
                    statements.append(stmt)
                    accum = []
                    recording = False
                    dollar_tag = None
                    continue

    if recording and accum:
        stmt = ''.join(accum).rstrip()
        stmt = rewrite_create(stmt)
        statements.append(stmt)

    return statements


def rewrite_create(statement: str) -> str:
    """
    Only modify CREATE TABLE statements to use IF NOT EXISTS.
    Leave other CREATE statements unchanged.
    """
    if re.match(r'^\s*CREATE\s+OR\s+REPLACE', statement, re.I):
        return statement
    
    statement = re.sub(
        r'^\s*CREATE\s+TABLE\b',
        'CREATE TABLE IF NOT EXISTS',
        statement,
        flags=re.I
    )
    
    return statement


def process_folder(base_folder):
    done_dir = os.path.join(base_folder, "createscripts")
    archive_dir = os.path.join(base_folder, "archive")
    os.makedirs(done_dir, exist_ok=True)
    os.makedirs(archive_dir, exist_ok=True)

    for filename in os.listdir(base_folder):
        if filename.lower().endswith(".sql"):
            full_path = os.path.join(base_folder, filename)
            print(f"Processing {filename}...")

            creates = extract_creates(full_path)
            if creates:
                out_name = os.path.splitext(filename)[0] + "_creates.sql"
                out_path = os.path.join(done_dir, out_name)
                with open(out_path, "w", encoding="utf-8") as out:
                    out.write("\n\n-- STATEMENT END --\n\n".join(creates))
                print(f"  -> Extracted {len(creates)} CREATE statements into {out_name}")

            shutil.move(full_path, os.path.join(archive_dir, filename))


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python extract_all.py /path/to/sql/folder")
        sys.exit(1)

    folder = sys.argv[1]
    process_folder(folder)
