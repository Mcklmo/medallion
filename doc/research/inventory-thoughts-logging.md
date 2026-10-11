# Inventory: thoughts' logging

Research for [#11](https://github.com/Mcklmo/medallion/issues/11) (map [#10](https://github.com/Mcklmo/medallion/issues/10)). thoughts paths are relative to `~/Projects/work/thoughts/thoughts` at commit `c4bda78`. Real-line counts come from all 87 `*.jsonl` files under its `output/logs` on 2026-10-11.

## 1. What thoughts logs

### Loggers and handlers

| # | What | Where | Process | Sink |
|---|---|---|---|---|
| L1 | Singleton app logger `src.log.create_logger`, `propagate=False`, level `THOUGHTS_LOG_LEVEL` (default `INFO`) | `src/log/create_logger.py` `create_logger()` | every thoughts process | `StreamHandler` (stderr) with `ColoredFormatter`: `<cyan time.ms> - <colored LEVEL> - <message>`, datefmt `%Y-%m-%d %H:%M:%S` |
| L2 | JSON file handler, added to L1 when `create_logger(log_dir, origin)` gets a dir (once per dir) | `create_logger.py` + `execute/api/json_log.py` `json_file_handler()` | see F1–F3 | `logging.FileHandler` (no rotation), one file per process start |
| L3 | L2's `FileHandler` also added to **medallion's** logger `medallion.log`, so medallion's own lines (listener start, "Fatal error in message handler") land in the file | `src/data_pipelines/scrape_synonyms/extract_words_from_db.py` `WordExtractor.__init__` | synonym scrape (medallion) | same file as F2 |
| L4 | API: `logging.basicConfig(level=INFO, format="%(levelname)s %(name)s %(message)s")` on root; logger `thoughts.api` (also used by `execute/api/{db,graph}.py`, `accounts/mail.py`, `files/*.py`, `sync/routes.py`) | `execute/api/main.py` | API (uvicorn; deployed on Cloud Run) | stderr, **plain text**. Deployed, this is what reaches Cloud Logging (`textPayload`), as Terraform sets no `THOUGHTS_API_LOG_DIR` |
| L5 | API JSON file handler on root, `uvicorn` and `uvicorn.access` (uvicorn's loggers don't propagate) | `execute/api/main.py`, only if `THOUGHTS_API_LOG_DIR` is set (`.vscode/launch.json`: `output/logs/api`) | API, local only | F3 |
| L6 | `logging_redirect_tqdm([medallion.log, create_logger()])`, entered once and kept open, so log lines print above the bar | `store_in_kg.py` `Progress.open_bar()` | synonym scrape | — |

Other `create_logger()` callers (stderr only, no file): `execute/parse_grammar/main.py`, `execute/ui/{graph,library}/main.py`, `src/natural_language_translation/main.py`, `src/data_pipelines/natural_language_parsing/{identify_sentences,identify_lexical_units,marker_from_pdf_transformer,transform_knowledge_graph}.py`, `scripts/version_caches.py`, `store_in_kg.py` (`SynonymStore.logger`, class attribute). `execute/ui/chat/app_state.py` passes a dir through, and `execute/parse_natural_language/parse_simplified/main.py` uses that to write a JSON file **with no `origin`** under its `--output_folder`.

So **the medallion PDF/NLP pipeline (`PDFExtractor`…`KnowledgeGraphTransformer`) writes no log file today**. Only the synonym scrape, the worker and the API do.

### Files

| # | Path | Written by | `origin` | Lines seen |
|---|---|---|---|---|
| F1 | `$THOUGHTS_WORKER_LOG_DIR/YYYY/MM/DD/HH/log-<start>.jsonl` (default `output/logs`) | indexing worker `python -m execute.worker` (`execute/worker/__main__.py`). **Not a medallion process**: `execute/worker/pipeline.py` calls the transformers one after another in-process | `worker` | 412 |
| F2 | `$THOUGHTS_SYNONYM_LOG_DIR/YYYY/MM/DD/HH/log-<start>.jsonl` (default `output/logs`), **same folder and name as F1** | `medallion WordExtractor SynonymPopulator SynonymStore`: three processors, one process, one file | `synonyms` | 4.24 M (3.41 M from logger `src.log.create_logger`, 0.84 M from `medallion.log`) |
| F3 | `$THOUGHTS_API_LOG_DIR/YYYY/MM/DD/HH/api-<start>.jsonl` (locally `output/logs/api/…`) | API | `api` | 22 k, 21.9 k of them `uvicorn.access` |
| F4 | `output/logs/log.jsonl` (flat, legacy) and old run files with no `origin` | earlier versions | none | 0.65 M |

`run_log_path(dir, name="log")` builds the path from medallion's own helpers (`generate_utc_timestamp_foldername`, `build_timestamp_path_segments`), so `<start>` is e.g. `2026-10-11T13-23-52160`. Each start logs `Logs are also written to "<abs path>"` as its first line.

### JSON line format (`execute/api/json_log.py` `JsonFormatter`)

Always: `timestamp` (UTC ISO-8601, ms: `2026-10-11T13:23:52.161+00:00`), `level` (Python `levelname`), `message` (`getMessage()`), `logger`, `module`, `function`, `line`.
Optional: `origin` (constructor arg), `exception` (`formatException`, a separate key, not in `message`).
Every `extra=` key becomes a **flat top-level key** (anything not a stock `LogRecord` attribute). `json.dumps(ensure_ascii=False, default=str)`.

Real line:

```json
{"timestamp": "2026-10-11T13:15:21.320+00:00", "level": "INFO", "message": "Sent the bundles of <redacted: 13 chars>", "logger": "medallion.log", "module": "store_in_kg", "function": "publish", "line": 287, "origin": "synonyms", "bundles_created": 0, "bundles_replaced": 1, "bundles_unchanged": 0}
```

### `extra` keys written today

| Keys | Message | Where |
|---|---|---|
| `duration_ms`, `nodes_created`, `relationships_created` | `Stored '<word>' in …s: …` | `store_in_kg.py` `store_message_data` |
| `ignored`, `unfetched`, `stored before` (a key with a space) | `Skipped N of M entries: …` (WARNING) | same |
| `bundles_created`, `bundles_replaced`, `bundles_unchanged` (prefixed, since bare `created` collides with a `LogRecord` attribute and raises) | `Sent the bundles of …` | `store_in_kg.py` `publish` |
| `duration_ms` (optional) | `Ignoring '<word>' from now on…` (ERROR) | `populate_synonyms.py` `ignore_word` |
| `done`, `total`, `fraction_done`, `seconds_per_word`, `seconds_left`, `time_left`, `eta`, `graph_objects`, `graph_nodes`, `graph_relationships` | `Progress` | `store_in_kg.py` `Progress.add` (below) |
| `color_message` | uvicorn startup lines (uvicorn's own `extra`, ANSI codes) | API, F3 |

Old files also carry `queries`, `lookups`, `vertices_found`, `vertices_created`, `edge_writes`, `words_per_min`; `src/store/neo4j.py` still counts `STORE_EVENTS` but nothing logs them now. `origin: benchmark-capture` came from a removed benchmark script.

### The synonyms `Progress` class (`src/data_pipelines/scrape_synonyms/store_in_kg.py`)

One module-level `progress` per process. `WordExtractor.extract` sets `progress.total = len(words)` (after dropping words already done). Each `SynonymStore.store_message_data` calls `progress.add(handled, stored_before, …)`:

- tqdm bar `desc="Synonyms", unit="word", disable=None` (off without a TTY), `smoothing=0.005`, **with `total`**, advanced by `handled + stored_before`.
- At most every 60 s, if `total` is known, an INFO `Progress` line with: `done`; `total` = extracted minus "stored before" (so the pace is this run's own); `fraction_done`; `seconds_per_word` (over the last interval); `seconds_left`, `time_left` (pendulum `in_words()`), `eta` (local ISO); and `graph_objects`, `graph_nodes`, `graph_relationships` from a Neo4j count-store query (`None`s plus a WARNING if Neo4j can't be asked).

### tqdm usages

| Where | Bar | Kind |
|---|---|---|
| `store_in_kg.py` `Progress.open_bar` | `Synonyms`, words, with total | **per-processor message progress**, medallion's bar would cover the bar but not the total/ETA |
| `src/knowledge_graph/parse.py` ×4 | `Parsing paragraphs`, `Parsing sections`, `Parsing section paragraphs`, `Parsing section subsections` | inner loop within one message (`KnowledgeGraphTransformer`, worker), not covered |
| `marker_from_pdf_transformer.py` `split_and_hash` | `Splitting pdf into pages and hashing each` | inner loop within one PDF, not covered |
| `src/natural_language_translation/chunk.py` | `Parsing elements` | inner loop, not covered |
| `scripts/scrape_progress.py` | external watcher: polls the newest `WordExtractor/…/data.json` vs `SynonymPopulator/…/data.json` output files, own rate window and ETA | a separate process, not logging |

### Consumers that depend on today's format

- `scripts/lnav-thoughts-format.json` (`lnav -i`), value fields `origin`, `logger`, `module`, `function`, `line`, `exception`, plus readme filters `:filter-expr :origin = 'synonyms'` and `:log_body REGEXP '^Progress.*'`.
- `scripts/query-logs.sh`: DuckDB `read_json_auto('<dir>/**/*.jsonl', union_by_name=true)`; readme queries on `level`, `origin`, `message`.
- `scripts/progress_table.py`, `progress_html.py`: glob `*/*/*/*/log-*.jsonl`, pick files whose **first line** has `origin == "synonyms"`, select `message == "Progress"`, and read `done`, `total`, `seconds_per_word`, `seconds_left`, `eta`, `graph_nodes`, `time_left`.
- `scripts/cloud_costs.py`: walks `output/logs`, classes a file as `api` when its name starts with `api`, otherwise as `worker`.
- `test/test_json_log.py`, `test/test_logging_no_content.py`.
- Content policy: `src/log/content.py` `redact()` logs only the length of document text, search terms and Cypher parameters unless `THOUGHTS_LOG_CONTENT=true`.

## 2. What lnav needs

- A JSON-lines file is read through a **format definition file**: JSON-lines logs "can be parsed and pretty-printed in lnav by creating a log format file" ([lnav formats](https://docs.lnav.org/en/latest/formats.html)). The docs don't say these files are recognised without one.
- Install: "you can pass the -i option to lnav to automatically install formats from the command-line"; they are "placed in the `installed` subdirectory" and named after the first format in the file, e.g. `~/.lnav/formats/installed/myformat_log.json` ([formats](https://docs.lnav.org/en/latest/formats.html)).
- Keys ([formats](https://docs.lnav.org/en/latest/formats.html)): `"file-type": "json"` (the older `"json": true` is deprecated); `timestamp-field` (default `timestamp`; a JSON line must have it to match); `level-field` (default `level`); `body-field` (default `body`); `level` maps lnav levels to regexes (unanchored); `value` declares fields with `kind` (`string`, `integer`, `float`, `json`, …) and `identifier`; `line-format` is the rendered line (`field`, `prefix`, `default-value`, `min-width`); `hide-extra` hides fields not in `value`; optional `file-pattern`; schema `https://lnav.org/schemas/format-v1.schema.json`.
- Filtering on an extra field: `:filter-expr` takes "The SQL expression to evaluate for each log message. The message values can be accessed using column names prefixed with a colon", e.g. `:filter-expr :log_procname = 'syslogd'` ([commands](https://docs.lnav.org/en/latest/commands.html)). A field declared under `value` is a column, so `:filter-expr :processor = 'synonym-scraper'` works. thoughts' readme already does this with `:origin`.

Minimal shape for medallion (field names follow the Cloud Logging spelling in section 3):

```json
{
  "$schema": "https://lnav.org/schemas/format-v1.schema.json",
  "medallion_json": {
    "title": "medallion JSON log",
    "file-type": "json",
    "file-pattern": "\\.jsonl$",
    "timestamp-field": "time",
    "level-field": "severity",
    "body-field": "message",
    "level": { "critical": "CRITICAL", "error": "ERROR", "warning": "WARNING", "info": "INFO", "debug": "DEBUG" },
    "value": { "processor": { "kind": "string", "identifier": true } },
    "line-format": [
      { "field": "__timestamp__" }, " ",
      { "field": "severity", "min-width": 7 }, " ",
      { "field": "processor" }, " ",
      { "field": "message" }
    ]
  }
}
```

thoughts' working equivalent with `timestamp`/`level`/`message` is `scripts/lnav-thoughts-format.json`.

## 3. What Cloud Logging takes from a JSON line on Cloud Run

- Cloud Run ships stdout/stderr; "a single line of serialized JSON" is parsed into `jsonPayload`, and plain text goes to `textPayload` ([Cloud Run logging](https://docs.cloud.google.com/run/docs/logging)).
- Special fields, removed from `jsonPayload` and set on the `LogEntry` ([special fields](https://docs.cloud.google.com/logging/docs/agent/logging/configuration#special-fields), which the Cloud Run page links to):
  - `severity` → `severity`
  - `message` → main display text (stays in `jsonPayload` unless it is the only field left)
  - `httpRequest` → `httpRequest`
  - time: a `timestamp` **object** `{seconds, nanos}`, else `timestampSeconds`+`timestampNanos`, else a `time` **string in RFC 3339** → `timestamp`. A string `timestamp` field matches none of these. It stays in `jsonPayload` and the entry gets the receive time ("If this field is omitted … Logging assigns it the current time", [LogEntry](https://docs.cloud.google.com/logging/docs/reference/v2/rest/v2/LogEntry))
  - `logging.googleapis.com/labels` → `labels`
  - `logging.googleapis.com/insertId` → `insertId`
  - `logging.googleapis.com/operation` → `operation`
  - `logging.googleapis.com/sourceLocation` → `sourceLocation`
  - `logging.googleapis.com/trace`, `/spanId`, `/trace_sampled` → `trace`, `spanId`, `traceSampled`
- Everything else stays in `jsonPayload`, which is why a `processor` key is filterable as `jsonPayload.processor`.
- Severity spelling: the agent "attempts to match a variety of common severity strings, which includes the list of LogSeverity strings recognized by the Logging API" ([special fields](https://docs.cloud.google.com/logging/docs/agent/logging/configuration#special-fields)). These are `DEFAULT, DEBUG, INFO, NOTICE, WARNING, ERROR, CRITICAL, ALERT, EMERGENCY` ([LogEntry / LogSeverity](https://docs.cloud.google.com/logging/docs/reference/v2/rest/v2/LogEntry)). Python's `levelname` values (`DEBUG`, `INFO`, `WARNING`, `ERROR`, `CRITICAL`) are all in that list, so `severity = record.levelname` is enough. The docs list no other spellings (`WARN`, lowercase), so don't depend on them.
- The special-fields page says to put stack traces in `message` so Error Reporting can parse them. thoughts keeps them in a separate `exception` key.

So thoughts' current keys `level` and `timestamp` (string) are **not** picked up by Cloud Logging. medallion's line needs `severity` and `time` (RFC 3339), or must emit both spellings.
