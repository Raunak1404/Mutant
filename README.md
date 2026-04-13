# Mutant — Agentic Excel Processor

An AI-powered desktop application for intelligent Excel data transformation, powered by a dual-LLM architecture with Claude and Azure OpenAI support.

## Architecture

- **Backend**: Python FastAPI with async SQLite, WebSocket progress streaming
- **Frontend**: React + TypeScript (Vite), served as static files by the backend
- **Desktop**: PyInstaller bundle with pywebview for native window experience
- **AI Core**: Multi-agent pipeline (orchestrator → codegen → worker → review) with 6-step processing

## Quick Start (Development)

### Prerequisites

- Python 3.11+
- Node.js 18+ (only for frontend development)

### 1. Clone & Install

```bash
git clone https://github.com/Raunak1404/Mutant.git
cd Mutant
```

### 2. Create Virtual Environment

**macOS / Linux:**
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip setuptools wheel
pip install -e '.[desktop]'
```

**Windows (PowerShell):**
```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install --upgrade pip setuptools wheel
pip install -e ".[desktop]"
```

**Windows (Command Prompt):**
```cmd
python -m venv .venv
.venv\Scripts\activate.bat
pip install --upgrade pip setuptools wheel
pip install -e ".[desktop]"
```

### 3. Configure Environment

Create a `.env` file in the project root (copy from `.env.example` if available):

```env
# Required — pick one LLM provider
LLM_PROVIDER=claude
ANTHROPIC_API_KEY=your-api-key-here
ANTHROPIC_MODEL=claude-sonnet-4-6

# Leave these as defaults for local desktop mode
JOB_RUNNER=local
USE_REDIS=false
EXECUTION_SERVICE_URL=embedded://local
API_HOST=127.0.0.1
API_PORT=8000
DEBUG=false
STORAGE_BACKEND=local
```

### 4. Build the Frontend (first time only)

```bash
cd frontend
npm install
npm run build
cd ..
```

This builds the React app into the `static/` directory.

### 5. Run the Desktop App

```bash
python desktop_app.py
```

This starts an embedded server and opens a native window. No separate backend or dev server needed!

### 6. Alternative — Run as Dev Server

```bash
# Terminal 1: Backend
python main.py

# Terminal 2: Frontend (with hot reload)
cd frontend && npm run dev
```

---

## Build Standalone Executable (Windows .exe / macOS .app)

### Prerequisites

```bash
pip install -e ".[desktop]"
```

### Build

```bash
pyinstaller weisiong-desktop.spec
```

The output will be in `dist/Mutant/`. Double-click `Mutant.exe` (Windows) or `Mutant` (macOS) to launch.

> **Note:** The `.env` file must be placed next to the executable, or in `~/.weisiong/.env`.

---

## Project Structure

```
├── api/                  # FastAPI routes, WebSocket, schemas
├── cache/                # Multi-tier caching (memory, disk, Redis)
├── chat/                 # Chat handler and models
├── config/               # Pydantic settings
├── core/                 # Agent pipeline (orchestrator, workers, review)
├── db/                   # SQLAlchemy models, migrations, rule/code seeding
├── desktop_app.py        # Desktop launcher (pywebview + embedded server)
├── excel/                # Excel reader/writer
├── execution_service/    # Sandboxed code execution
├── feedback/             # Feedback loop (questions, code/rule updates)
├── frontend/             # React + TypeScript source
├── llm/                  # LLM providers (Claude, Azure OpenAI)
├── main.py               # FastAPI app factory
├── models/               # Shared data models and enums
├── runtime/              # Bootstrap, paths, job runner
├── static/               # Pre-built frontend (served by backend)
├── steps/                # Native step logic + rule definitions
├── storage/              # File storage backends (local, S3, Azure Blob)
├── tasks/                # Task queue (taskiq broker)
├── tests/                # Test suite
├── utils/                # Logging, retry utilities
└── weisiong-desktop.spec # PyInstaller build spec
```

## License

Private / Internal Use
