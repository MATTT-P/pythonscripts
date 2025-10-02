#!/usr/bin/env python3
import os
import re
import shutil
import sys

# Regex to detect start of INSERT
insert_start_re = re.compile(r'^\s*INSERT\b', re.I)
delimiter_re = re.compile(r'^\s*DELIMITER\s+(.+)$', re.I)
dollar_open_re = re.compile(r'\$[A-Za-z0-9_]*\$')  # for PostgreSQL bodies

def extract_inserts(file_path):
    """
    Extract INSERT statements from a .sql file.
    Handles MySQL DELIMITER and PostgreSQL dollar-quoted bodies.
    """
    current_delim = ';'
    accum = []
    recording = False
    dollar_tag = None
    statements = []

    with open(file_path, 'r', encoding='utf-8', errors='replace') as f:
        for raw_line in f:
            line = raw_line

            # Handle delimiter change
            m = delimiter_re.match(line)
            if m:
                current_delim = m.group(1)
                continue

            # If not recording, check for INSERT start
            if not recording:
                if insert_start_re.match(line):
                    recording = True
                    accum.append(line)
                    continue
                else:
                    continue
            else:
                # Append lines while recording
                accum.append(line)

                # Inside dollar-quoted block?
                if dollar_tag:
                    if dollar_tag in line and line.count(dollar_tag) % 2 == 1:
                        dollar_tag = None
                    continue

                # Opening new dollar-quote
                d = dollar_open_re.search(line)
                if d:
                    dollar_tag = d.group(0)
                    if line.count(dollar_tag) % 2 == 0:
                        dollar_tag = None
                    continue

                # Statement termination
                if line.rstrip().endswith(current_delim):
                    if current_delim != ';':
                        accum[-1] = accum[-1].rstrip()
                        if accum[-1].endswith(current_delim):
                            accum[-1] = accum[-1][:-len(current_delim)]
                        accum[-1] = accum[-1].rstrip() + "\n"
                    stmt = ''.join(accum).rstrip()
                    statements.append(stmt)
                    accum = []
                    recording = False
                    dollar_tag = None
                    continue

    # Catch dangling statement
    if recording and accum:
        stmt = ''.join(accum).rstrip()
        statements.append(stmt)

    return statements


def process_folder(base_folder):
    done_dir = os.path.join(base_folder, "done")
    archive_dir = os.path.join(base_folder, "archive")
    os.makedirs(done_dir, exist_ok=True)
    os.makedirs(archive_dir, exist_ok=True)

    for filename in os.listdir(base_folder):
        if filename.lower().endswith(".sql"):
            full_path = os.path.join(base_folder, filename)
            print(f"Processing {filename}...")

            inserts = extract_inserts(full_path)
            if inserts:
                out_name = os.path.splitext(filename)[0] + "_inserts.sql"
                out_path = os.path.join(done_dir, out_name)
                with open(out_path, "w", encoding="utf-8") as out:
                    out.write("\n\n-- STATEMENT END --\n\n".join(inserts))
                print(f"  -> Extracted {len(inserts)} INSERT statements into {out_name}")

            # Move original file to archive
            shutil.move(full_path, os.path.join(archive_dir, filename))


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python extract_inserts.py /path/to/sql/folder")
        sys.exit(1)

    folder = sys.argv[1]
    process_folder(folder)
