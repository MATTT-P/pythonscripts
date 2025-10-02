import os
import shutil
import sys
import re

ALLOWED_TABLES = {'customer', 'call_outcome'}

def split_multi_tuple_insert(statement: str):
    """
    Splits an INSERT statement with multiple tuples into multiple single-tuple INSERTs.
    Adds promotionid, promotionname columns and NULL, NULL to each tuple.
    """
    idx_values = statement.lower().find('values')
    if idx_values == -1:
        return [statement.strip()]

    columns_part = statement[:idx_values].strip()
    values_part = statement[idx_values + len('values'):].strip().rstrip(';')

    if '(' in columns_part and ')' in columns_part:
        start = columns_part.find('(')
        end = columns_part.rfind(')')
        cols = columns_part[start+1:end].strip()
        if 'promotionid' not in cols.lower():
            new_cols = cols + ', promotionid, promotionname'
        else:
            new_cols = cols
        columns_part = columns_part[:start+1] + new_cols + columns_part[end:]
    else:
        columns_part += ' (promotionid, promotionname)'

    tuples = []
    buf = ''
    paren_level = 0
    inside_string = False
    string_char = ''
    for c in values_part:
        buf += c
        if inside_string:
            if c == string_char:
                inside_string = False
        else:
            if c in ("'", '"'):
                inside_string = True
                string_char = c
            elif c == '(':
                paren_level += 1
            elif c == ')':
                paren_level -= 1
                if paren_level == 0:
                    # End of tuple
                    buf = buf[:-1] + ', NULL, NULL)'
                    tuples.append(buf.strip())
                    buf = ''
            elif c == ',' and paren_level == 0:
                buf = ''
    if buf.strip():
        tuples.append(buf.strip())

    single_inserts = []
    for t in tuples:
        single_inserts.append(f"{columns_part} VALUES {t};")
    return single_inserts


def table_allowed(statement: str) -> bool:
    """
    Returns True if the INSERT statement is for an allowed table.
    """
    m = re.match(r'^\s*INSERT\s+INTO\s+`?(\w+)`?', statement, re.I)
    if m:
        table_name = m.group(1).lower()
        return table_name in ALLOWED_TABLES
    return False


def extract_inserts(file_path):
    """
    Extracts all INSERT statements from a file, including multi-line and multi-tuple inserts.
    Splits multi-tuple INSERTs into single-tuple INSERTs.
    Only returns inserts for allowed tables.
    """
    inserts = []
    stmt_lines = []
    paren_level = 0
    inside_string = False
    string_char = ''

    with open(file_path, 'r', encoding='utf-8') as f:
        for line in f:
            stmt_lines.append(line)
            for c in line:
                if inside_string:
                    if c == string_char:
                        inside_string = False
                else:
                    if c in ("'", '"'):
                        inside_string = True
                        string_char = c
                    elif c == '(':
                        paren_level += 1
                    elif c == ')':
                        paren_level -= 1
                    elif c == ';' and paren_level == 0:
                        statement = ''.join(stmt_lines).strip()
                        lines = statement.splitlines()
                        while lines and lines[0].strip().startswith('--'):
                            lines.pop(0)
                        clean_statement = '\n'.join(lines).strip()
                        if clean_statement.lower().startswith('insert into') and table_allowed(clean_statement):
                            split_statements = split_multi_tuple_insert(clean_statement)
                            inserts.extend(split_statements)
                        stmt_lines = []

    if stmt_lines:
        statement = ''.join(stmt_lines).strip()
        lines = statement.splitlines()
        while lines and lines[0].strip().startswith('--'):
            lines.pop(0)
        clean_statement = '\n'.join(lines).strip()
        if clean_statement.lower().startswith('insert into') and table_allowed(clean_statement):
            split_statements = split_multi_tuple_insert(clean_statement)
            inserts.extend(split_statements)

    return inserts


def process_folder(base_folder):
    done_dir = os.path.join(base_folder, "insertscripts")
    archive_dir = os.path.join(base_folder, "archive")
    os.makedirs(done_dir, exist_ok=True)
    os.makedirs(archive_dir, exist_ok=True)

    for filename in os.listdir(base_folder):
        if filename.lower().endswith('.sql'):
            full_path = os.path.join(base_folder, filename)
            print(f"Processing {filename}...")

            inserts = extract_inserts(full_path)
            print(f"Extracted {len(inserts)} INSERT statements")

            if inserts:
                out_name = os.path.splitext(filename)[0] + "_inserts.sql"
                out_path = os.path.join(done_dir, out_name)
                with open(out_path, 'w', encoding='utf-8') as out:
                    out.write('\n\n-- STATEMENT END --\n\n'.join(inserts))
                print(f"  -> Output written to {out_name}")

            shutil.move(full_path, os.path.join(archive_dir, filename))


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python extract_inserts.py /path/to/sql/folder")
        sys.exit(1)

    folder = sys.argv[1]
    process_folder(folder)

