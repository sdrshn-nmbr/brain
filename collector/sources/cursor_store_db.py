"""Decode and search Cursor's SQLite chat history (``~/.cursor/chats/*/*/store.db``).

Cursor stores newer agent chats as a content-addressed protobuf blob DAG inside a SQLite db, not as
JSONL. Each ``store.db`` has two tables::

    blobs(id TEXT PRIMARY KEY, data BLOB)   -- id = sha256 hex, data = protobuf (or JSON for some roots)
    meta(key TEXT, value TEXT)              -- meta['0'] = hex-encoded JSON {agentId,name,latestRootBlobId,...}

The root blob (``meta['0'].latestRootBlobId``) is a ``conversationState`` protobuf whose field 8 lists
ordered Merkle tree nodes; each tree node lists its child message-blob hashes (field 1 = first, field 2 =
rest). Walking the tree in order reproduces the chronological message timeline.

Message leaf schema (reverse-engineered):
    USER      : f1 = direct UTF-8 prompt text, f2 = 36-char message UUID (sometimes f8 = ProseMirror doc).
    ASSISTANT : text nested at f1->f1 or f3->f1 (status lines + synthesis answers).
    TOOL      : large nested f2 holding call + result (+ f57 id, f59/f60 epoch-ms timestamps).
    JSON      : some blobs are plain ``{"role": ..., "content": ...}`` JSON (the exact shape sent to
                the model API) instead of protobuf — seen in compacted/summarized-context roots, and
                as the sole recoverable content in sessions whose tree commit never completed (root
                pointer missing/empty/orphaned; see ``_fallback_order``). ``classify()`` tags these
                JUSER/JASSISTANT/JSYSTEM/JTOOL; ``display_role()`` maps back to a plain role string.

Root shape variants (also reverse-engineered, also best-effort):
    Normal    : field 8 lists ordered Merkle tree nodes; each tree node lists its child message-blob
                hashes (field 1 = first, field 2 = rest). Walking the tree in order reproduces the
                chronological message timeline (``_walk_order``).
    Compacted : no field 8 at all — field 1 lists message-blob hashes directly, field 6 is a single
                compaction-summary blob (``_walk_compacted_order``).
    Orphaned  : ``latestRootBlobId`` is missing, empty, or points to a stale/empty blob (session
                interrupted before the tree commit finished), but the underlying request blobs
                still exist in the blobs table — rescued in SQLite insertion order, JSON-shaped
                blobs only (``_fallback_order``).

Search correctness contract: ``search_fast`` MUST return the same matches as ``search_naive``. The fast
path may only use raw-byte prefilters as a *superset* and then re-check on extracted text.
"""

from __future__ import annotations

import glob
import hashlib
import json
import multiprocessing as mp
import os
import pickle
import re
import sqlite3
from concurrent.futures import ProcessPoolExecutor
from dataclasses import dataclass, field

CHATS_GLOB = os.path.expanduser("~/.cursor/chats/*/*/store.db")


# --------------------------------------------------------------------------------------
# protobuf wire decoding
# --------------------------------------------------------------------------------------
def read_varint(buf, i):
    s = r = 0
    while True:
        b = buf[i]
        i += 1
        r |= (b & 0x7F) << s
        if not (b & 0x80):
            break
        s += 7
    return r, i


def parse(buf):
    out = []
    i, n = 0, len(buf)
    while i < n:
        try:
            tag, i = read_varint(buf, i)
            fn, wt = tag >> 3, tag & 7
            if wt == 0:
                v, i = read_varint(buf, i)
                out.append((fn, wt, v))
            elif wt == 2:
                ln, i = read_varint(buf, i)
                if i + ln > n:
                    break
                out.append((fn, wt, buf[i : i + ln]))
                i += ln
            elif wt == 5:
                out.append((fn, wt, buf[i : i + 4]))
                i += 4
            elif wt == 1:
                out.append((fn, wt, buf[i : i + 8]))
                i += 8
            else:
                break
        except IndexError:
            break
    return out


def fdict(fs):
    d = {}
    for fn, wt, v in fs:
        d.setdefault(fn, []).append((wt, v))
    return d


def is_text(v, thr=0.95):
    try:
        s = v.decode("utf-8")
    except Exception:
        return None
    if not s:
        return None
    if sum(c.isprintable() or c in "\n\t\r" for c in s) / len(s) >= thr:
        return s
    return None


def _clean_head(t):
    return all(c.isprintable() or c in "\n\t\r " for c in t[:4])


def deep_text(v, maxd=12):
    direct = is_text(v)
    if maxd <= 0:
        return direct
    candidates = []
    for _ifn, iwt, iv in parse(v):
        if iwt == 2:
            sub = deep_text(iv, maxd - 1)
            if sub and sub.strip():
                candidates.append(sub)
    if candidates:
        longest = max(candidates, key=len)
        if direct is None or len(longest) >= len(v) - 8 or not _clean_head(direct):
            return longest
    return direct


_CTRL = {c: None for c in range(32) if c not in (9, 10, 13)}
_CTRL[127] = None


def sanitize(s):
    return s.translate(_CTRL) if s else s


# --------------------------------------------------------------------------------------
# message classification + text extraction
# --------------------------------------------------------------------------------------
_UUID = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$")

_JSON_ROLES = {"system", "user", "assistant", "tool"}
_JSON_ROLE_TYPE = {"system": "JSYSTEM", "user": "JUSER", "assistant": "JASSISTANT", "tool": "JTOOL"}
_JSON_TYPE_ROLE = {v: k for k, v in _JSON_ROLE_TYPE.items()}


def extract_json_message(raw):
    """Cursor also stores some messages as raw OpenAI-style JSON ({"role": ..., "content": ...}) —
    the exact shape sent to the model API — rather than the usual protobuf leaf. This shows up in
    compacted/summarized-context roots (alongside a protobuf "Summary" blob under field 6) and,
    critically, is the *only* recoverable content in sessions whose tree commit never completed
    (see `_fallback_order`): the underlying request blobs got written before the crash/interrupt,
    they just never got linked into a committed conversationState tree. Returns (role, text) or
    None if `raw` isn't this shape.
    """
    if not raw or raw[:1] != b"{":
        return None
    try:
        obj = json.loads(raw)
    except (ValueError, UnicodeDecodeError):
        return None
    if not isinstance(obj, dict):
        return None
    role = obj.get("role")
    if role not in _JSON_ROLES:
        return None
    content = obj.get("content")
    if isinstance(content, str):
        text = content
    elif isinstance(content, list):
        parts = []
        for item in content:
            if isinstance(item, str):
                parts.append(item)
            elif isinstance(item, dict):
                t = item.get("text")
                if isinstance(t, str):
                    parts.append(t)
        text = "\n".join(parts)
    else:
        text = ""
    return role, text


def display_role(typ):
    """Canonical lowercase role for a `classify()` type — handles the JSON-message types
    (JUSER/JASSISTANT/JSYSTEM/JTOOL) alongside the normal protobuf USER/ASSISTANT/TOOL."""
    return _JSON_TYPE_ROLE.get(typ, typ.lower())


def classify(raw, blobkeys):
    json_msg = extract_json_message(raw)
    if json_msg is not None:
        return _JSON_ROLE_TYPE[json_msg[0]], json_msg
    fs = parse(raw)
    if not fs:
        return "EMPTY", fs
    d = fdict(fs)
    if set(d) == {1} and d[1][0][0] == 2:
        inner = parse(d[1][0][1])
        if any(iwt == 2 and len(iv) == 32 and iv.hex() in blobkeys for _, iwt, iv in inner):
            return "TREE", fs
    # USER: direct f1 text + f2 uuid (newer models omit the ProseMirror f8 doc)
    if 1 in d and d[1][0][0] == 2:
        f1t = is_text(d[1][0][1])
        f2t = is_text(d[2][0][1]) if 2 in d and d[2][0][0] == 2 else None
        f8t = is_text(d[8][0][1]) if 8 in d and d[8][0][0] == 2 else None
        if f1t and _clean_head(f1t) and ((f2t and _UUID.match(f2t.strip())) or (f8t and f8t.startswith('{"'))):
            return "USER", fs
    if 2 in d and d[2][0][0] == 2 and is_text(d[2][0][1]) is None and len(d[2][0][1]) > 40:
        return "TOOL", fs
    if (3 in d and d[3][0][0] == 2) or (1 in d and d[1][0][0] == 2):
        return "ASSISTANT", fs
    return "OTHER", fs


def extract_user(fs):
    d = fdict(fs)
    if 1 in d and d[1][0][0] == 2:
        return is_text(d[1][0][1]) or ""
    return ""


def extract_assistant(fs):
    d = fdict(fs)
    for key in (1, 3):
        if key in d and d[key][0][0] == 2:
            t = deep_text(d[key][0][1])
            if t and t.strip():
                return t.strip()
    return ""


def extract_tool(fs):
    """Return (title, command, outputs[]) for a tool blob."""
    d = fdict(fs)
    f2 = d[2][0][1]
    d2 = fdict(parse(f2))
    title = is_text(d2[3][0][1]) if 3 in d2 and is_text(d2[3][0][1]) else None
    container = None
    for ck in (1, 8):
        if ck in d2 and d2[ck][0][0] == 2:
            container = parse(d2[ck][0][1])
            break
    command = None
    outputs = []
    if container is not None:
        dc = fdict(container)
        call = parse(dc[1][0][1]) if 1 in dc and dc[1][0][0] == 2 else []
        dcall = fdict(call)
        for k in (1, 3):
            if k in dcall and dcall[k][0][0] == 2 and is_text(dcall[k][0][1]):
                command = is_text(dcall[k][0][1])
                break
        if not title and 15 in dcall and is_text(dcall[15][0][1]):
            title = is_text(dcall[15][0][1])
        result_bytes = dc[2][0][1] if 2 in dc and dc[2][0][0] == 2 else f2
        seen = set()
        for t in _gather(result_bytes):
            k = t.strip()
            if k and k != (command or "").strip() and k not in seen and len(k) > 1:
                seen.add(k)
                outputs.append(t)
    return title, command, outputs


def _gather(raw, maxd=12, acc=None, depth=0):
    if acc is None:
        acc = []
    for _fn, wt, v in parse(raw):
        if wt != 2:
            continue
        t = deep_text(v)
        if t and len(t.strip()) > 1 and _clean_head(t):
            acc.append(t)
        elif depth < maxd:
            _gather(v, maxd, acc, depth + 1)
    return acc


def message_text(typ, fs):
    """Concatenated searchable text for a decoded message."""
    if typ in _JSON_TYPE_ROLE:
        return fs[1]
    if typ == "USER":
        return extract_user(fs)
    if typ == "ASSISTANT":
        return extract_assistant(fs)
    if typ == "TOOL":
        title, command, outputs = extract_tool(fs)
        return "\n".join(x for x in ([title, command] + outputs) if x)
    return ""


# --------------------------------------------------------------------------------------
# store.db reading
# --------------------------------------------------------------------------------------
@dataclass
class Chat:
    db_path: str
    agent_id: str
    name: str
    model: str
    created_at: int
    workspace: str | None
    root: str
    order: list = field(default_factory=list)  # ordered message blob hashes
    blobmap: dict = field(default_factory=dict)  # hash -> raw bytes


def _connect(db_path):
    return sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)


def read_meta(db_path):
    con = _connect(db_path)
    try:
        row = con.execute("SELECT value FROM meta WHERE key='0'").fetchone()
        if not row:
            return None
        return json.loads(bytes.fromhex(row[0]))
    finally:
        con.close()


def load_chat(db_path):
    """Load full chat: metadata + ordered message hashes + all blob bytes.

    Three tiers are tried in order, each only attempted if the previous one produced nothing:
      1. `_walk_order` — the normal field-8 Merkle tree walk (the vast majority of chats).
      2. `_walk_compacted_order` — newer/compacted-context roots that list message hashes
         directly under field 1 (+ a field-6 compaction summary), with no field-8 tree at all.
      3. `_fallback_order` — rescue path for chats whose root pointer is missing, empty, or an
         orphaned/stale hash (interrupted before Cursor committed a tree): recovers whatever raw
         JSON message blobs (see `extract_json_message`) survive in the blobs table, in SQLite
         insertion order, since that's the only ordering signal left once the tree is gone.
    Returns None only if there's truly nothing recoverable (no meta row, or no blobs at all).
    """
    con = _connect(db_path)
    try:
        meta = con.execute("SELECT value FROM meta WHERE key='0'").fetchone()
        if not meta:
            return None
        meta = json.loads(bytes.fromhex(meta[0]))
        blobmap = {i: d for i, d in con.execute("SELECT id, data FROM blobs ORDER BY rowid")}
    finally:
        con.close()
    if not blobmap:
        return None

    root = meta.get("latestRootBlobId") or ""
    workspace = None
    order = []
    if root and root in blobmap:
        rootdata = blobmap[root]
        for fn, wt, v in parse(rootdata):
            if fn == 9 and wt == 2:
                t = is_text(v)
                if t and t.startswith("file://"):
                    workspace = t[len("file://") :]
        order = _walk_order(rootdata, blobmap)
        if not order:
            order = _walk_compacted_order(rootdata, blobmap)

    if not order:
        order = _fallback_order(blobmap, exclude={root} if root else set())

    if not order:
        return None

    return Chat(
        db_path=db_path,
        agent_id=meta.get("agentId", ""),
        name=meta.get("name", ""),
        model=meta.get("lastUsedModel", ""),
        created_at=meta.get("createdAt", 0),
        workspace=workspace,
        root=root,
        order=order,
        blobmap=blobmap,
    )


def _walk_order(rootdata, blobmap):
    """Ordered message-blob hashes via the field-8 Merkle tree walk."""
    order = []
    seen = set()
    for fn, wt, v in parse(rootdata):
        if fn == 8 and wt == 2 and len(v) == 32 and v.hex() in blobmap:
            node = blobmap[v.hex()]
            for _, nwt, nv in parse(node):
                if nwt != 2:
                    continue
                for _ifn, iwt, iv in parse(nv):
                    if iwt == 2 and len(iv) == 32 and iv.hex() in blobmap:
                        h = iv.hex()
                        if h not in seen:
                            seen.add(h)
                            order.append(h)
    return order


def _walk_compacted_order(rootdata, blobmap):
    """Fallback for compacted-context roots: no field-8 tree, but field 1 lists message-blob
    hashes directly (and field 6 is a compaction summary blob) — observed on newer sessions
    after Cursor compacts/summarizes older context. Order follows raw field-encoding order."""
    order = []
    seen = set()
    for fn, wt, v in parse(rootdata):
        if fn in (1, 6) and wt == 2 and len(v) == 32 and v.hex() in blobmap:
            h = v.hex()
            if h not in seen:
                seen.add(h)
                order.append(h)
    return order


def _references_other_blob(raw, blobmap):
    """True if `raw` contains a 32-byte field value that is itself a key in `blobmap` — i.e. this
    blob acts as a pointer/checkpoint node rather than a true leaf message. Real leaf messages
    (USER/ASSISTANT/TOOL) never reference other blobs this way; only tree/checkpoint nodes do."""
    return any(wt == 2 and len(v) == 32 and v.hex() in blobmap for fn, wt, v in parse(raw))


def _fallback_order(blobmap, exclude=None):
    """Last-resort rescue when the root is missing/empty/orphaned but the blobs table still has
    message content (the session was interrupted before Cursor could commit a valid tree —
    request blobs get written before the response/commit step, so they can survive a crash that
    the tree pointer doesn't). Recovers, in SQLite insertion order:
      - unambiguous raw-JSON message blobs (see `extract_json_message`), and
      - protobuf USER/ASSISTANT/TOOL leaf blobs that don't themselves reference another blob in
        the table (`_references_other_blob`) — that check excludes pointer/checkpoint blobs that
        would otherwise be misclassified as USER by the heuristic (observed in the wild: a cached
        tool-definitions dump that happens to match the USER field shape)."""
    exclude = exclude or set()
    blobkeys = set(blobmap)
    order = []
    for h, raw in blobmap.items():
        if h in exclude:
            continue
        if extract_json_message(raw) is not None:
            order.append(h)
            continue
        typ, fs = classify(raw, blobkeys)
        if typ in ("USER", "ASSISTANT", "TOOL") and not _references_other_blob(raw, blobmap):
            order.append(h)
    return order


def discover_dbs():
    return sorted(glob.glob(CHATS_GLOB))


# --------------------------------------------------------------------------------------
# persistent decode cache (db mtime keyed) — old chats never change, so decode once
# --------------------------------------------------------------------------------------
CACHE_DIR = os.path.expanduser("~/.cache/brain/cursor-decode")
CACHE_VERSION = 2


def _cache_path(db_path):
    h = hashlib.sha1(db_path.encode()).hexdigest()
    return os.path.join(CACHE_DIR, f"{h}.pkl")


def db_messages(db_path):
    """Return (meta_dict, [(idx, role, text), ...]) for a db, using a persistent mtime-keyed cache.

    The cached text per message is exactly ``message_text(...)`` — identical to what search_naive
    extracts — so searching the cache stays equivalent to the naive baseline.
    """
    try:
        mtime = os.path.getmtime(db_path)
    except OSError:
        return None, []
    cp = _cache_path(db_path)
    try:
        with open(cp, "rb") as f:
            blob = pickle.load(f)
        if blob.get("v") == CACHE_VERSION and blob.get("mtime") == mtime:
            return blob["meta"], blob["msgs"]
    except Exception:
        pass
    chat = load_chat(db_path)
    if chat is None:
        meta, msgs = None, []
    else:
        blobkeys = set(chat.blobmap)
        msgs = []
        for idx, h in enumerate(chat.order):
            typ, fs = classify(chat.blobmap[h], blobkeys)
            if typ in ("TREE", "EMPTY", "OTHER"):
                continue
            text = message_text(typ, fs)
            if text:
                msgs.append((idx, display_role(typ), text))
        meta = {"name": chat.name, "model": chat.model, "workspace": chat.workspace, "created_at": chat.created_at}
    try:
        os.makedirs(CACHE_DIR, exist_ok=True)
        tmp = cp + ".tmp"
        with open(tmp, "wb") as f:
            pickle.dump(
                {"v": CACHE_VERSION, "mtime": mtime, "meta": meta, "msgs": msgs}, f, protocol=pickle.HIGHEST_PROTOCOL
            )
        os.replace(tmp, cp)
    except Exception:
        pass
    return meta, msgs


def load_full_messages(db_path):
    """Return (meta_dict, [{"index","role","text","title"}, ...]) for the `read` skill's full-fidelity
    dump: unlike db_messages(), TOOL blobs keep command+outputs joined but title separated out (not a
    canonical tool name — Cursor's format only exposes a generated, human-readable label here), and
    nothing is truncated. Always re-decodes; skips the db_messages() pickle cache since full dumps are
    infrequent and want the freshest possible read.
    """
    chat = load_chat(db_path)
    if chat is None:
        return None, []
    blobkeys = set(chat.blobmap)
    entries = []
    for idx, h in enumerate(chat.order):
        typ, fs = classify(chat.blobmap[h], blobkeys)
        if typ in ("TREE", "EMPTY", "OTHER"):
            continue
        title = None
        if typ in _JSON_TYPE_ROLE:
            text = fs[1]
        elif typ == "USER":
            text = extract_user(fs)
        elif typ == "ASSISTANT":
            text = extract_assistant(fs)
        else:  # TOOL
            title, command, outputs = extract_tool(fs)
            text = "\n".join(x for x in ([command] + outputs) if x)
        text = sanitize(text or "").strip()
        if not text and not title:
            continue
        entries.append({"index": idx, "role": display_role(typ), "text": text, "title": title})
    meta = {
        "name": chat.name,
        "model": chat.model,
        "workspace": chat.workspace,
        "created_at": chat.created_at,
        "agent_id": chat.agent_id,
    }
    return meta, entries


# --------------------------------------------------------------------------------------
# search
# --------------------------------------------------------------------------------------
@dataclass
class Match:
    db_path: str
    name: str
    model: str
    workspace: str | None
    created_at: int
    idx: int  # position in ordered timeline
    role: str
    text: str


def _iter_messages(chat):
    blobkeys = set(chat.blobmap)
    for idx, h in enumerate(chat.order):
        typ, fs = classify(chat.blobmap[h], blobkeys)
        yield idx, h, typ, fs


_STOPWORDS = frozenset(
    (
        "a",
        "an",
        "and",
        "are",
        "as",
        "at",
        "be",
        "but",
        "by",
        "do",
        "does",
        "did",
        "for",
        "from",
        "had",
        "has",
        "have",
        "if",
        "in",
        "into",
        "is",
        "it",
        "its",
        "me",
        "my",
        "no",
        "not",
        "of",
        "on",
        "or",
        "our",
        "out",
        "over",
        "per",
        "so",
        "than",
        "that",
        "the",
        "their",
        "them",
        "then",
        "there",
        "these",
        "this",
        "those",
        "to",
        "up",
        "via",
        "was",
        "we",
        "were",
        "what",
        "when",
        "where",
        "whether",
        "which",
        "who",
        "will",
        "with",
        "you",
        "your",
        "i",
    )
)


def _parse_terms(query):
    """Whitespace-split into AND terms; a quoted `"..."` query is one exact phrase.

    Kept local (not imported from common) so process-pool fork workers stay self-contained.
    Mirrors common.parse_query_terms, including stopword/single-char filtering.
    """
    q = (query or "").strip()
    if len(q) >= 2 and q[0] == '"' and q[-1] == '"':
        inner = q[1:-1].strip()
        return [inner] if inner else []
    tokens = q.split()
    substantive = [t for t in tokens if len(t) > 1 and t.lower() not in _STOPWORDS]
    return substantive or tokens


def _terms_pattern(terms, ignore_case=True):
    """Smart-case alternation over terms, longest-first. None when no terms.

    Case sensitivity is scoped per term with inline (?i:) groups, mirroring
    common.build_highlight_pattern: uppercase-bearing terms (acronyms like NAT)
    match case-sensitively; lowercase terms stay insensitive.
    """
    if not terms:
        return None
    ordered = sorted(terms, key=len, reverse=True)
    if not ignore_case:
        return re.compile("|".join(re.escape(t) for t in ordered))
    parts = [f"(?i:{re.escape(t)})" if t.islower() else re.escape(t) for t in ordered]
    return re.compile("|".join(parts))


def search_naive(query, dbs=None, ignore_case=True):
    """Reference implementation: decode every message in every db, match on extracted text."""
    if dbs is None:
        dbs = discover_dbs()
    pat = _terms_pattern(_parse_terms(query), ignore_case)
    if pat is None:
        return []
    results = []
    for db in dbs:
        try:
            chat = load_chat(db)
        except Exception:
            continue
        if chat is None:
            continue
        for idx, _h, typ, fs in _iter_messages(chat):
            if typ in ("TREE", "EMPTY", "OTHER"):
                continue
            text = message_text(typ, fs)
            if text and pat.search(text):
                results.append(
                    Match(db, chat.name, chat.model, chat.workspace, chat.created_at, idx, display_role(typ), text)
                )
    return results


def _search_one_db(args):
    """Search a single db. Top-level (picklable) for process-pool use.

    Optimization: byte-level prefilter. ``extract(m)`` is a subset of ``raw_bytes(m)``, so a blob whose
    raw bytes (case-folded) lack the query cannot match — skip its expensive protobuf text extraction.
    Candidates are re-checked on extracted text, making this exactly equivalent to search_naive.
    """
    db, query, ignore_case = args
    pat = _terms_pattern(_parse_terms(query), ignore_case)
    if pat is None:
        return []
    try:
        meta, msgs = db_messages(db)
    except Exception:
        return []
    if not meta or not msgs:
        return []
    out = []
    for idx, role, text in msgs:
        if pat.search(text):
            out.append(Match(db, meta["name"], meta["model"], meta["workspace"], meta["created_at"], idx, role, text))
    return out


def search_fast(query, dbs=None, ignore_case=True, workers=None):
    """Optimized path. MUST be equivalent to search_naive.

    Fans per-db work (CPU-bound protobuf parsing) across a process pool to scale on multicore.
    """
    if dbs is None:
        dbs = discover_dbs()
    args = [(db, query, ignore_case) for db in dbs]
    if workers is None:
        workers = min(os.cpu_count() or 4, 16)
    if workers <= 1 or len(dbs) < 8:
        results = []
        for a in args:
            results.extend(_search_one_db(a))
        return results
    # 'fork' avoids re-importing this module in every worker (much cheaper startup than macOS 'spawn').
    try:
        ctx = mp.get_context("fork")
    except ValueError:
        ctx = None
    results = []
    with ProcessPoolExecutor(max_workers=workers, mp_context=ctx) as ex:
        for part in ex.map(_search_one_db, args, chunksize=8):
            results.extend(part)
    return results


# --------------------------------------------------------------------------------------
# CLI-facing helpers: scoping + context grouping
# --------------------------------------------------------------------------------------
def _under(child, parent):
    if not child or not parent:
        return False
    child, parent = os.path.normpath(child), os.path.normpath(parent)
    return child == parent or child.startswith(parent + os.sep)


def scope_dbs(cwd=None, project=None, all_projects=False):
    """Return (db_path, meta) pairs filtered by workspace scope, using the decode cache for meta."""
    out = []
    for db in discover_dbs():
        meta, _ = db_messages(db)
        if not meta:
            continue
        ws = meta.get("workspace")
        if all_projects:
            keep = True
        elif project:
            keep = bool(ws and project.lower() in ws.lower()) or project.lower() in meta.get("name", "").lower()
        elif cwd:
            keep = (_under(ws, cwd) or _under(cwd, ws)) if ws else False
        else:
            keep = True
        if keep:
            out.append((db, meta))
    return out


def search_with_context(query, dbs, context=2, max_results=10):
    """Group matches per chat with surrounding messages. Returns list of chat-result dicts."""
    matches = search_fast(query, dbs=dbs)
    terms = _parse_terms(query)
    by_db = {}
    for m in matches:
        by_db.setdefault(m.db_path, []).append(m)
    # AND relevance: search_fast flags any-term hits; a chat only qualifies if EVERY term
    # appears somewhere in it (smart-case). The same scan yields term_coverage for ranking.
    coverage_by_db = {}
    if len(terms) > 1:
        qualified = {}
        for db, ms in by_db.items():
            _, msgs = db_messages(db)
            blob = "\n".join(text for _, _, text in msgs)
            blob_lower = blob.lower()
            coverage = sum(1 for t in terms if (t in blob_lower if t.islower() else t in blob))
            if coverage == len(terms):
                qualified[db] = ms
                coverage_by_db[db] = coverage
        by_db = qualified
    else:
        coverage_by_db = {db: 1 for db in by_db}
    # No recency cap here: the corpus is small (dozens of dbs) and the dispatcher
    # ranks by relevance before cutting, so pre-cutting by mtime would reintroduce
    # the buried-old-conversation bug. max_results is applied downstream.
    ordered = sorted(by_db, key=lambda d: os.path.getmtime(d) if os.path.exists(d) else 0, reverse=True)
    results = []
    for db in ordered:
        meta, msgs = db_messages(db)
        if not msgs:
            continue
        hit_idx = sorted({m.idx for m in by_db[db]})
        by_pos = {idx: (idx, role, text) for idx, role, text in msgs}
        positions = [idx for idx, _, _ in msgs]
        wanted = sorted({p for hi in hit_idx for p in positions if abs(p - hi) <= context})
        groups = []
        cur = []
        prev = None
        for p in wanted:
            if prev is not None and p != prev + 1:
                groups.append(cur)
                cur = []
            cur.append((by_pos[p], p in hit_idx))
            prev = p
        if cur:
            groups.append(cur)
        results.append(
            {
                "db_path": db,
                "name": meta.get("name", ""),
                "model": meta.get("model", ""),
                "workspace": meta.get("workspace"),
                "created_at": meta.get("created_at", 0),
                "total_messages": len(msgs),
                "n_matches": len(hit_idx),
                "groups": groups,
                "term_coverage": coverage_by_db.get(db, 0),
                "match_count": len(hit_idx),
            }
        )
    return results
