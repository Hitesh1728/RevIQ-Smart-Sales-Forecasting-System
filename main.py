"""
FastAPI Backend for AI Sales Forecasting System
Fixed for Railway/cloud deployment:
  - OUTPUT_DIR uses /tmp (writable on any cloud platform)
  - Results saved to disk as JSON (survives in-process restarts)
  - Absolute path handling for ml_engine / powerbi imports
  - CORS already configured
"""

import os
import sys
import json
import tempfile
import shutil
import logging
import traceback
from typing import Optional
from pathlib import Path

import pandas as pd
import numpy as np
from fastapi import FastAPI, UploadFile, File, HTTPException, BackgroundTasks
from fastapi.responses import FileResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
import uvicorn


def make_serializable(obj):
    """Recursively convert numpy/non-serializable types to JSON-safe types."""
    import math
    if isinstance(obj, dict):
        return {str(k): make_serializable(v) for k, v in obj.items()}
    elif isinstance(obj, (list, tuple)):
        return [make_serializable(i) for i in obj]
    elif isinstance(obj, (np.integer,)):
        return int(obj)
    elif isinstance(obj, (np.floating,)):
        v = float(obj)
        return 0.0 if (math.isnan(v) or math.isinf(v)) else v
    elif isinstance(obj, np.ndarray):
        return [make_serializable(i) for i in obj.tolist()]
    elif isinstance(obj, float):
        return 0.0 if (math.isnan(obj) or math.isinf(obj)) else obj
    elif isinstance(obj, (int, str, bool)) or obj is None:
        return obj
    else:
        return str(obj)


# ── Path setup — works locally AND on Railway ──────────────────────────────
# __file__ = backend/main.py  →  parent = backend/  →  parent.parent = project root
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from ml_engine.automl import AutoMLForecaster
from powerbi.exporter import PowerBIExporter

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

app = FastAPI(
    title="AI Sales Forecasting API",
    description="AutoML-powered sales forecasting with Power BI integration",
    version="1.0.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── Storage: use /tmp on cloud, ./outputs locally ──────────────────────────
# Railway containers have a writable /tmp directory.
# We store results as JSON files so they survive Python restarts
# (in-memory dicts are wiped on every redeploy).
def _get_output_dir() -> Path:
    """Returns a writable output directory on any platform."""
    # Railway / Render / any container → use /tmp
    if os.getenv("RAILWAY_ENVIRONMENT") or os.getenv("RENDER"):
        base = Path("/tmp/sales_forecasting")
    else:
        # Local dev → use ./outputs next to main.py
        base = PROJECT_ROOT / "outputs"
    base.mkdir(parents=True, exist_ok=True)
    return base

OUTPUT_DIR = _get_output_dir()
RESULTS_DIR = OUTPUT_DIR / "_results"
RESULTS_DIR.mkdir(parents=True, exist_ok=True)
logger.info(f"OUTPUT_DIR = {OUTPUT_DIR}")


def _save_results(session_id: str, results: dict):
    """Persist results to disk so they survive restarts."""
    path = RESULTS_DIR / f"{session_id}.json"
    with open(path, "w") as f:
        json.dump(make_serializable(results), f)


def _load_results(session_id: str) -> Optional[dict]:
    """Load results from disk."""
    path = RESULTS_DIR / f"{session_id}.json"
    if path.exists():
        with open(path) as f:
            return json.load(f)
    return None


def _save_files_index(session_id: str, files: dict):
    """Persist file paths index to disk."""
    path = RESULTS_DIR / f"{session_id}_files.json"
    with open(path, "w") as f:
        json.dump(files, f)


def _load_files_index(session_id: str) -> Optional[dict]:
    """Load file paths index from disk."""
    path = RESULTS_DIR / f"{session_id}_files.json"
    if path.exists():
        with open(path) as f:
            return json.load(f)
    return None


def load_dataframe(file_path: str, filename: str) -> pd.DataFrame:
    """Load any common data format into DataFrame."""
    ext = filename.lower().rsplit('.', 1)[-1] if '.' in filename else ''

    loaders = {
        'csv':     lambda p: pd.read_csv(p),
        'xlsx':    lambda p: pd.read_excel(p, engine='openpyxl'),
        'xls':     lambda p: pd.read_excel(p, engine='xlrd'),
        'json':    lambda p: pd.read_json(p),
        'parquet': lambda p: pd.read_parquet(p),
        'tsv':     lambda p: pd.read_csv(p, sep='\t'),
        'txt':     lambda p: pd.read_csv(p),
    }

    loader = loaders.get(ext, lambda p: pd.read_csv(p))
    try:
        df = loader(file_path)
        logger.info(f"Loaded {filename}: {df.shape}")
        return df
    except Exception:
        for name, fn in loaders.items():
            try:
                df = fn(file_path)
                logger.info(f"Loaded {filename} as {name}: {df.shape}")
                return df
            except Exception:
                continue
        raise ValueError(
            f"Could not load '{filename}'. Supported: CSV, Excel, JSON, Parquet, TSV"
        )


# ── Routes ─────────────────────────────────────────────────────────────────

@app.get("/")
async def root():
    return {"message": "AI Sales Forecasting API", "version": "1.0.0", "status": "running"}


@app.get("/health")
async def health():
    return {"status": "healthy"}


@app.post("/api/upload-and-analyze")
async def upload_and_analyze(file: UploadFile = File(...)):
    """Upload data file and run AutoML analysis."""
    if not file.filename:
        raise HTTPException(status_code=400, detail="No file provided")

    allowed_exts = {'csv', 'xlsx', 'xls', 'json', 'parquet', 'tsv', 'txt'}
    ext = file.filename.lower().rsplit('.', 1)[-1] if '.' in file.filename else ''
    if ext not in allowed_exts:
        raise HTTPException(
            status_code=400,
            detail=f"Unsupported file type '.{ext}'. Allowed: {', '.join(allowed_exts)}"
        )

    tmp_dir = tempfile.mkdtemp()
    file_path = os.path.join(tmp_dir, file.filename)

    try:
        content = await file.read()
        if len(content) == 0:
            raise HTTPException(status_code=400, detail="Uploaded file is empty")

        with open(file_path, 'wb') as f:
            f.write(content)

        df = load_dataframe(file_path, file.filename)

        if df.empty:
            raise HTTPException(status_code=400, detail="File contains no data")
        if df.shape[0] < 5:
            raise HTTPException(
                status_code=400,
                detail=f"Dataset too small ({df.shape[0]} rows). Need at least 5 rows."
            )

        # Run AutoML
        forecaster = AutoMLForecaster()
        results = forecaster.fit(df)

        # Generate session ID
        import hashlib, time
        session_id = hashlib.md5(f"{file.filename}{time.time()}".encode()).hexdigest()[:12]

        # ── Persist results to disk (not just in-memory) ──────────────────
        _save_results(session_id, results)

        # Generate exports
        pbi_dir = str(OUTPUT_DIR / session_id)
        exporter = PowerBIExporter(output_dir=pbi_dir)
        files = exporter.export(results, filename_prefix="sales_forecast")

        # Persist file index to disk
        _save_files_index(session_id, files)

        response_data = make_serializable({
            "session_id": session_id,
            "status": "success",
            "message": f"AutoML analysis complete. Best model: {results['metrics']['best_model']}",
            "metrics": results['metrics'],
            "summary": results.get('summary', {}),
            "forecast_summary": results.get('forecast_summary', {}),
            "profile": {
                "rows":          results['profile']['shape'][0],
                "columns":       results['profile']['shape'][1],
                "target_column": results['profile']['target_column'],
                "date_column":   results['profile']['date_column'],
            },
            "model_comparison": {
                k: {kk: vv for kk, vv in v.items() if kk != 'predictions'}
                for k, v in results.get('model_comparison', {}).items()
                if isinstance(v, dict)
            },
            "history_length":   len(results.get('history', [])),
            "forecast_periods": len(results.get('forecast', [])),
        })
        return JSONResponse(response_data)

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Analysis failed: {traceback.format_exc()}")
        raise HTTPException(status_code=500, detail=f"Analysis failed: {str(e)}")
    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)


@app.get("/api/results/{session_id}")
async def get_results(session_id: str):
    """Get full results for a session."""
    results = _load_results(session_id)
    if results is None:
        raise HTTPException(status_code=404, detail="Session not found")
    return JSONResponse(results)


@app.get("/api/dashboard/{session_id}")
async def get_dashboard(session_id: str):
    """Get the HTML dashboard file."""
    files = _load_files_index(session_id)
    if files is None:
        raise HTTPException(status_code=404, detail="Session not found. Run analysis first.")

    html_path = files.get('html_dashboard')
    if not html_path or not os.path.exists(html_path):
        raise HTTPException(status_code=404, detail="Dashboard file not found")

    return FileResponse(html_path, media_type="text/html",
                        headers={"Content-Disposition": "inline"})


@app.get("/api/download/{session_id}/{file_type}")
async def download_file(session_id: str, file_type: str):
    """Download specific file: html_dashboard, pbit, historical_csv, forecast_csv."""
    files = _load_files_index(session_id)
    if files is None:
        raise HTTPException(status_code=404, detail="Session not found")

    file_path = files.get(file_type)
    if not file_path or not os.path.exists(file_path):
        available = [k for k, v in files.items() if v and os.path.exists(v)]
        raise HTTPException(
            status_code=404,
            detail=f"File '{file_type}' not found. Available: {available}"
        )

    filename = os.path.basename(file_path)
    ext = filename.rsplit('.', 1)[-1].lower()
    media_types = {
        'html': 'text/html',
        'csv':  'text/csv',
        'pbit': 'application/octet-stream',
        'json': 'application/json',
        'md':   'text/markdown',
    }
    return FileResponse(
        file_path,
        media_type=media_types.get(ext, 'application/octet-stream'),
        headers={"Content-Disposition": f"attachment; filename={filename}"}
    )


@app.get("/api/files/{session_id}")
async def list_files(session_id: str):
    """List all available files for a session."""
    files = _load_files_index(session_id)
    if files is None:
        raise HTTPException(status_code=404, detail="Session not found")
    available = {k: os.path.basename(v) for k, v in files.items() if v and os.path.exists(v)}
    return {"session_id": session_id, "files": available}


if __name__ == "__main__":
    port = int(os.getenv("PORT", 8000))   # Railway injects $PORT automatically
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=port,
        reload=False,           # Never use reload=True in production
        log_level="info"
    )
