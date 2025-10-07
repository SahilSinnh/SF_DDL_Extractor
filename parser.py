"""
Contains all the sophisticated logic for parsing raw DDL text into
structured Python objects.
"""
import re
from typing import List, Dict, Optional

def strip_identifier_quotes(ident: Optional[str]) -> str:
    """Removes surrounding double quotes from a SQL identifier and un-escapes internal quotes."""
    if not ident:
        return ""
    ident = ident.strip()
    if len(ident) >= 2 and ident[0] == ident[-1] == '"':
        return ident[1:-1].replace('""', '"')
    return ident

def normalize_type(prefix: Optional[str], base_type: str) -> str:
    """Combines a prefix and base type into a canonical object type name."""
    bt = re.sub(r"\s+", " ", base_type.strip().upper())
    px = (prefix or "").strip().upper()
    if px == "MATERIALIZED" and bt == "VIEW":
        return "MATERIALIZED VIEW"
    if px == "DYNAMIC" and bt == "TABLE":
        return "DYNAMIC TABLE"
    return bt

def split_qualified_name(name: str) -> List[str]:
    """
    Splits a dot-separated qualified name, correctly handling quoted identifiers.
    Example: '"DB"."SCH"."TBL"' -> ['"DB"', '"SCH"', '"TBL"']
    """
    parts, buf, in_dq, i = [], [], False, 0
    while i < len(name):
        ch = name[i]
        if in_dq:
            buf.append(ch)
            if ch == '"':
                # Handle escaped double quote
                if i + 1 < len(name) and name[i+1] == '"':
                    buf.append('"'); i += 2; continue
                in_dq = False
        elif ch == '"':
            in_dq = True
            buf.append(ch)
        elif ch == '.':
            parts.append(''.join(buf).strip())
            buf = []
        else:
            buf.append(ch)
        i += 1
    if buf:
        parts.append(''.join(buf).strip())
    return [p for p in parts if p]

def split_sql_statements(ddl_text: str) -> List[str]:
    """
    Splits a block of SQL text into individual statements, correctly handling
    comments, strings, and procedure bodies.
    """
    stmts, buf, s = [], [], ddl_text
    n = len(s); i = 0
    in_sq = in_dq = in_dollar = in_line_c = in_block_c = False
    
    while i < n:
        # State machine to handle different SQL contexts
        if in_line_c:
            buf.append(s[i])
            if s[i] == '\n': in_line_c = False
            i += 1; continue
        if in_block_c:
            if i + 1 < n and s[i] == '*' and s[i+1] == '/':
                buf.append('*/'); i += 2; in_block_c = False; continue
            buf.append(s[i]); i += 1; continue
        if in_sq:
            buf.append(s[i])
            if s[i] == "'":
                if i + 1 < n and s[i+1] == "'": buf.append("'"); i += 2; continue
                in_sq = False
            i += 1; continue
        if in_dq:
            buf.append(s[i])
            if s[i] == '"':
                if i + 1 < n and s[i+1] == '"': buf.append('"'); i += 2; continue
                in_dq = False
            i += 1; continue
        if in_dollar:
            if i + 1 < n and s[i] == '$' and s[i+1] == '$':
                buf.append('$$'); i += 2; in_dollar = False; continue
            buf.append(s[i]); i += 1; continue

        # Not in any special context, check for context entry or semicolon
        if i + 1 < n and s[i] == '-' and s[i+1] == '-': buf.append('--'); i += 2; in_line_c = True; continue
        if i + 1 < n and s[i] == '/' and s[i+1] == '*': buf.append('/*'); i += 2; in_block_c = True; continue
        if s[i] == "'": in_sq = True; buf.append(s[i]); i += 1; continue
        if s[i] == '"': in_dq = True; buf.append(s[i]); i += 1; continue
        if i + 1 < n and s[i] == '$' and s[i+1] == '$': in_dollar = True; buf.append('$$'); i += 2; continue
        
        if s[i] == ';':
            stmt = ''.join(buf).strip()
            if stmt: stmts.append(stmt)
            buf = []; i += 1
            while i < n and s[i].isspace(): i += 1 # Skip whitespace after semicolon
            continue
        
        buf.append(s[i]); i += 1
        
    tail = ''.join(buf).strip()
    if tail: stmts.append(tail)
    return stmts

def extract_object_metadata(stmt: str) -> Optional[Dict[str, str]]:
    """
    Parses a CREATE statement to extract its type, name, and components.
    """
    # Regex to capture CREATE [MODIFIERS] [PREFIX] TYPE <name> ...
    pattern = re.compile(r"""
        ^\s*CREATE\s+(?:OR\s+REPLACE\s+)?(?:SECURE\s+|TRANSIENT\s+|TEMPORARY\s+|EXTERNAL\s+)*
        (?:(?P<prefix>MATERIALIZED|DYNAMIC)\s+)?
        (?P<type>FILE\s+FORMAT|MASKING\s+POLICY|ROW\s+ACCESS\s+POLICY|DATABASE|SCHEMA|TABLE|VIEW|SEQUENCE|PIPE|TASK|STAGE|STREAM|FUNCTION|PROCEDURE|TAG)\s+
        (?:IF\s+NOT\s+EXISTS\s+)?
        (?P<name>(?:"[^"]+"|[A-Za-z_][\w$]*)(?:\.(?:"[^"]+"|[A-Za-z_][\w$]*)){0,2})
        """, re.IGNORECASE | re.VERBOSE | re.DOTALL)
    
    m = pattern.search(stmt)
    if not m: return None
    
    obj_type = normalize_type(m.group("prefix"), m.group("type"))
    raw_name = m.group("name")
    parts = split_qualified_name(raw_name)
    
    db, schema, obj = None, None, None
    if len(parts) == 3:
        db, schema, obj = parts
    elif len(parts) == 2:
        schema, obj = parts
    else:
        obj = parts[0]
        
    return {
        "object_type": obj_type,
        "database": strip_identifier_quotes(db),
        "schema": strip_identifier_quotes(schema),
        "object_name": strip_identifier_quotes(obj),
        "fully_qualified_name": ".".join([strip_identifier_quotes(p) for p in parts]),
    }

def remove_database_references(ddl: str, db_name: str) -> str:
    """
    Removes references to a specific database in a DDL statement,
    if the reference matches the provided db_name.
    Handles various quoting styles and avoids replacement in string literals.
    """
    if not db_name:
        return ddl

    # Un-quote an identifier
    def unquote(s):
        return s.strip('"')

    # Regex for a single identifier (quoted or not)
    ID = r'"(?:[^"]|"")*"|[a-zA-Z_][a-zA-Z0-9_$]*'
    # Regex for a 3-part FQN: db.schema.object
    FQN_REGEX = re.compile(rf'({ID})\s*\.\s*({ID})\s*\.\s*({ID})')

    def replacer(match):
        db_part, schema_part, object_part = match.groups()
        if unquote(db_part).lower() == db_name.lower():
            return f'{schema_part}.{object_part}'
        else:
            return match.group(0)

    # Avoid replacing inside string literals by splitting the string
    # This is a simplified approach and may not handle all edge cases
    # like escaped single quotes within string literals.
    parts = ddl.split("'")
    result = []
    for i, part in enumerate(parts):
        if i % 2 == 0: # Outside of single quotes
            result.append(FQN_REGEX.sub(replacer, part))
        else: # Inside single quotes
            result.append(part)
    return "'".join(result)

def get_material_icon(obj_type: Optional[str]) -> str:
    """
    Returns a Material icon name string for a given Snowflake object type.
    Falls back to 'help_outline' for unknown or None values.
    """

    if not obj_type:
        return "help_outline"

    obj_type = obj_type.strip().upper()

    icon_map = {
        "DATABASE": "database",
        "SCHEMA": "schema",
        "SEQUENCE": "123",
        "TABLE": "table",
        "DYNAMIC TABLE": "dynamic_form",
        "VIEW": "table_view",
        "STAGE": "cloud_upload",
        "EXTERNAL TABLE": "cloud_sync",
        "FILE FORMAT": "files",
        "PROCEDURE": "flowsheet",
        "FUNCTION": "function",
        "PIPE": "route",
        "MATERIALIZED VIEW": "table_view",
        "STREAM": "flowchart",
        "TASK": "check_circle",
        "MASKING POLICY": "visibility_off",
        "TAG": "label",
        "UNKNOWN": "view_object_track",
    }

    return f':material/{icon_map.get(obj_type, "view_object_track")}:'

# Helper function to build snippet for a block
def build_block_snippet(block, obj_ddl_lines, final_script_lines):
    """Build formatted snippet for a block of lines."""
    lines = []
    for k, is_match, final_k in block:  # Sort by k (line in DDL)
        content = obj_ddl_lines[k].strip()
        prefix = f"{final_k}     " if final_k != 'N/A' else f"N/A     "
        if is_match:
            prefix = f"{final_k} >>> " if final_k != 'N/A' else f"N/A >>> "
        lines.append(f"{prefix}{content}")
    return "\n".join(lines)