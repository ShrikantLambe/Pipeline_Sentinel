#!/bin/sh
# Railway injects $PORT at runtime. This script guarantees shell expansion
# regardless of whether the builder (Railpack/Nixpacks) uses exec or sh -c.
exec streamlit run app/streamlit_app.py \
    --server.port="${PORT:-8501}" \
    --server.address=0.0.0.0 \
    --server.headless=true
