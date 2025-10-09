# Contains the topological sort implementation to order database objects based on their dependencies.

import re
from collections import defaultdict, deque
from typing import List, Dict, Set, Any, Optional, Tuple


def order_objects_by_dependencies(objects: List[Dict]) -> Tuple[List[Dict], Dict[str, Set[str]]]:
    # Topologically sorts a list of Snowflake objects and returns the dependency graph.
    # This function implements Kahn's algorithm for topological sorting.
    
    def u(x: Optional[str]) -> Optional[str]:
        # Helper to normalize an identifier to uppercase and remove quotes.
        if x is None: return None
        x = x.strip()
        if len(x) >= 2 and x[0] == x[-1] == '"':
            x = x[1:-1].replace('""', '"')
        return x.upper()

    def canon_fqn(db: Optional[str], sch: Optional[str], obj: Optional[str]) -> Optional[str]:
        # Creates a canonical, fully-qualified name string.
        db_u, sch_u, obj_u = u(db), u(sch), u(obj)
        if db_u and sch_u and obj_u: return f"{db_u}.{sch_u}.{obj_u}"
        if sch_u and obj_u: return f"{sch_u}.{obj_u}"
        return obj_u
    
    # Regex to find qualified identifiers like DB.SCHEMA.OBJ or SCHEMA.OBJ
    # It correctly handles quoted parts.
    ID = r'(?:\"[^\"]+\"|[A-Za-z_][A-Za-z0-9_\$]*)'
    QUAL_ID_REGEX = re.compile(rf'(?<!\w)({ID})\s*\.\s*({ID})(?:\s*\.\s*({ID}))?(?!\w)')

    # --- 1. Pre-process and Index all objects ---
    objs = []
    for o in objects:
        db = o.get("database") or None
        sch = o.get("schema") or None
        obj = o.get("object_name") or None
        ddl = o.get("ddl") or ""
        fqn = canon_fqn(db, sch, obj)
        objs.append({**o, "_CANON_FQN": fqn, "_DB": u(db), "_SC": u(sch), "_OBJ": u(obj), "_DDL_UPPER": ddl.upper()})

    by_fqn = {o["_CANON_FQN"]: o for o in objs if o["_CANON_FQN"]}
    by_schema_obj: Dict[str, List[Dict]] = defaultdict(list)
    for o in objs:
        if o["_SC"] and o["_OBJ"]:
            by_schema_obj[f"{o['_SC']}.{o['_OBJ']}"].append(o)
    
    # --- 2. Build Dependency Graph ---
    deps: Dict[str, Set[str]] = defaultdict(set)  # node -> {dependencies}
    outs: Dict[str, Set[str]] = defaultdict(set)  # node -> {dependents}
    nodes: Set[str] = {o["_CANON_FQN"] for o in objs if o["_CANON_FQN"]}

    for o in objs:
        cur_fqn = o["_CANON_FQN"]
        if not cur_fqn: continue
        
        o_deps: Set[str] = set()

        # A. Find explicit dependencies via regex on DDL
        for m in QUAL_ID_REGEX.finditer(o["_DDL_UPPER"]):
            p1, p2, p3 = m.groups()
            
            target_fqn = None
            if p3:  # 3-part reference: DB.SCHEMA.OBJECT
                cand_fqn = canon_fqn(p1, p2, p3)
                if cand_fqn in by_fqn:
                    target_fqn = cand_fqn
            else:  # 2-part reference: SCHEMA.OBJECT
                key = f"{u(p1)}.{u(p2)}"
                cands = by_schema_obj.get(key, [])
                if len(cands) == 1:
                    target_fqn = cands[0]["_CANON_FQN"]
                elif len(cands) > 1:
                    same_db = [c for c in cands if c["_DB"] == o["_DB"] and c["_DB"] is not None]
                    if len(same_db) == 1:
                        target_fqn = same_db[0]["_CANON_FQN"]

            if target_fqn and target_fqn != cur_fqn:
                o_deps.add(target_fqn)
        
        # B. Add implicit dependencies (e.g., table depends on its schema)
        if o["object_type"] not in ["DATABASE", "SCHEMA"] and o["_SC"]:
            schema_fqn = canon_fqn(o["_DB"], o["_SC"], o["_SC"])
            if schema_fqn and schema_fqn in by_fqn and by_fqn[schema_fqn]['object_type'] == 'SCHEMA':
                o_deps.add(schema_fqn)
        
        if o["object_type"] != "DATABASE" and o["_DB"]:
            db_fqn = canon_fqn(None, None, o["_DB"])
            if db_fqn and db_fqn in by_fqn and by_fqn[db_fqn]['object_type'] == 'DATABASE':
                 o_deps.add(db_fqn)

        deps[cur_fqn].update(o_deps)
        for d in o_deps:
            if d:
                outs[d].add(cur_fqn)

    # --- 3. Kahn's Algorithm for Topological Sort ---
    in_degree = {n: len(deps.get(n, set())) for n in nodes}
    queue: deque[str] = deque([n for n, d in in_degree.items() if d == 0])
    ordered = []

    while queue:
        n = queue.popleft()
        if n in by_fqn:
            ordered.append(by_fqn[n])
        for m in outs.get(n, set()):
            in_degree[m] -= 1
            if in_degree[m] == 0:
                queue.append(m)
    
    # --- 4. Finalize and Return ---
    # Handle cycles by appending any remaining objects
    seen_fqns = {o["_CANON_FQN"] for o in ordered}
    remaining = [o for o in objs if o["_CANON_FQN"] and o["_CANON_FQN"] not in seen_fqns]
    result = ordered + remaining

    # Re-index before returning
    for i, o in enumerate(result):
        o["index"] = i
        
    return result, deps

