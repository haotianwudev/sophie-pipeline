# Cloud Run Job image for the SPX option-chain snapshot ETL.
#
# Lives at the repo root because the job imports from src/tools/, so the build context has to
# be the pipeline root -- `gcloud run jobs deploy --source .` picks up the Dockerfile at the
# root of whatever it is given. Deploy via services/spx-snapshot-etl/deploy.ps1.
#
# Only the modules the job actually needs are copied in; see .dockerignore for what is kept out.

FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

WORKDIR /app

# Dependencies first so the layer caches across code changes.
COPY services/spx-snapshot-etl/requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Only the modules the ETL imports -- not the whole src/ tree.
# src/data/models.py comes along because api_db.py imports it at module level; it is cheaper to
# carry one pydantic model file than to fork the shared DB helper.
COPY src/__init__.py                      src/__init__.py
COPY src/tools/__init__.py                src/tools/__init__.py
COPY src/tools/api_cboe.py                src/tools/api_cboe.py
COPY src/tools/api_db.py                  src/tools/api_db.py
COPY src/data/__init__.py                 src/data/__init__.py
COPY src/data/models.py                   src/data/models.py
COPY src/agents/__init__.py               src/agents/__init__.py
COPY src/agents/spx_option_snapshot.py    src/agents/spx_option_snapshot.py
COPY spx-option-snapshot/run.py           spx-option-snapshot/run.py

# Cloud Run Jobs run to completion and exit; there is no server to listen on.
ENTRYPOINT ["python", "spx-option-snapshot/run.py"]
