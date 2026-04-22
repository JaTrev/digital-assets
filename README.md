# Digital Assets

A data pipeline repository for collecting and analyzing digital asset data.

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

### `scripts/`
- Includes data importing and data processing tasks
- Can be run manually with `python -m scripts.file_name`

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


### Development
```bash
# Install dependencies
pip install -r requirements.txt

# Use research notebooks
jupyter notebook research/notebooks/
```
