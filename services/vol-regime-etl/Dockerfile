# Cloud Run Job image for the volatility-regime / VRP ETL.
#
# Unlike the SPX snapshot job -- whose Dockerfile sits at the repo root so `gcloud run jobs
# deploy --source .` finds it -- this one lives beside its service because a repo root can only
# hold one default Dockerfile. It is built explicitly via cloudbuild.yaml (docker build -f),
# which keeps the two images independent: a change here never forces a rebuild of the
# non-reproducible snapshot job.
#
# Build context is still the pipeline root, since the job imports from src/.

FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

WORKDIR /app

# Dependencies first so the layer caches across code changes.
COPY services/vol-regime-etl/requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Only the modules this job imports -- not the whole src/ tree.
COPY src/__init__.py                  src/__init__.py
COPY src/tools/__init__.py            src/tools/__init__.py
COPY src/tools/api_db.py              src/tools/api_db.py
COPY src/tools/api_yfinance.py        src/tools/api_yfinance.py
COPY src/tools/db_upload.py           src/tools/db_upload.py
# db_upload imports both of these at module level, so they are needed even though this job
# never fetches news or line items.
COPY src/tools/api_alphavantage.py    src/tools/api_alphavantage.py
# No src/cfg/__init__.py to copy -- unlike src/tools and src/data, cfg is an implicit namespace
# package, so `from src.cfg.line_items_list import ...` resolves without one.
COPY src/cfg/line_items_list.py       src/cfg/line_items_list.py
# api_db.py imports the pydantic models at module level; cheaper to carry the one file than to
# fork the shared DB helper.
COPY src/data/__init__.py             src/data/__init__.py
COPY src/data/models.py               src/data/models.py
COPY src/agents/__init__.py           src/agents/__init__.py
COPY src/agents/vol_regime.py         src/agents/vol_regime.py
COPY vol-regime/run.py                vol-regime/run.py

# Cloud Run Jobs run to completion and exit; there is no server to listen on.
ENTRYPOINT ["python", "vol-regime/run.py"]
