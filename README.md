# Digital Assets

A data pipeline application for collecting and analyzing digital asset data.

## Project Structure

```
digital-assets/
├── data/                    # 📊 Data storage (never committed to git)
│
├── process/                # ⚙️ Automated daily processes (triggered by GitHub Actions)
│
├── src/                    # 🔧 Reusable Python modules
│   ├── clients/            # API & database clients
│   ├── models/             # Data models & schemas
│   ├── utils/              # Utility functions
│   └── __init__.py
│
├── research/               # 📓 Research & analysis notebooks
│   ├── notebooks/          # Jupyter notebooks for exploration
│   └── scripts/            # Analysis scripts
│
├── .github/workflows/      # 🤖 GitHub Actions automation
│   └── deploy.yml          # Daily process scheduler
│
├── pyproject.toml          # Python project configuration
├── requirements.txt        # Python dependencies
├── Dockerfile              # Container configuration
└── .gitignore              # Git ignore rules (data/ never committed)
```

## Key Directories

### `data/` 
- Local data storage for raw and processed data
- **Never committed to git** (configured in `.gitignore`)
- Data flows in from processes and is consumed by research

### `process/`
- Automated tasks triggered daily by GitHub Actions (see `.github/workflows/deploy.yml`)
- Includes data importing and data processing tasks
- Examples: `dummy.py`, `email_dummy.py`
- Can be run manually with `python -m process.dummy`

### `src/`
- Core Python modules shared across processes and research
- **Clients**: Database connectors, API clients (ccxt, GCP, etc.)
- **Models**: Data schemas and structures
- **Utils**: Shared utilities like logging, helpers

### `research/`
- Jupyter notebooks for exploratory analysis and research
- Python scripts for standalone analysis
- Isolated environment for experimentation

## Running

### Daily Automation
Processes run automatically via GitHub Actions at 7:30 AM UTC (configurable in `.github/workflows/deploy.yml`)

### Manual Execution
```bash
# Run a specific process
python -m process.dummy
python -m process.email_dummy

# Or directly
python jobs/dummy.py
```

### Development
```bash
# Install dependencies
pip install -r requirements.txt

# Use research notebooks
jupyter notebook research/notebooks/
```
