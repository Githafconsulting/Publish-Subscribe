
# kstreams (demo)
A tiny Kafka pub/sub helper to illustrate point-to-point vs publish-subscribe like the diagram.

## Files
- `kstreams.py` — library
- `demo.py` — runs two patterns:
    - Topic **B** with one subscription `subscription-b` shared by B1/B2 (work split)
    - Topic **C** with subscriptions `subscription-yc` and `subscription-zc` (fan-out)
- `docker-compose.yml` — local Kafka
- `requirements.txt` — deps
- `single_demo.py` — self-contained (no imports)

## Run (multi-file)
```bash
docker compose up -d
pip install -r requirements.txt
python demo.py
```

## Run (single file)
```bash
docker compose up -d
pip install kafka-python
python single_demo.py
```

## Troubleshooting "FileNotFoundError"
- Ensure you are in the folder with the files: `ls` (macOS/Linux) or `dir` (Windows).
- Use absolute paths: `python /full/path/to/demo.py`.
- If `kstreams` import fails, run `python single_demo.py` which is self-contained.
