# Dais Agenda

A tool to help plan attendance to the Databricks Data + AI Summit. This tool allows you to:
- Scrape session data from the Databricks website
- Manage and organize sessions
- Rate and tag sessions
- Get personalized recommendations

## Project Structure

```
dais-agenda/
├── src/
│   └── dais_agenda/
│       ├── __init__.py
│       ├── scraper.py      # Web scraper for session data using Selenium
│       ├── session_manager.py  # Session data management
│       └── cli.py         # Command-line interface
├── data/
│   └── sessions_.jsonl    # All session data in a single file
├── pyproject.toml         # Project configuration
└── README.md             # This file
```

## Installation

1. Clone the repository:
   ```bash
   git clone https://github.com/pmogren/dais-agenda.git
   cd dais-agenda
   ```

2. Create and activate a virtual environment:
   ```bash
   python -m venv .venv
   source .venv/bin/activate  # On Windows: .venv\Scripts\activate
   ```

3. Install the package in development mode:
   ```bash
   pip install -e .
   ```

## Usage

The tool provides a command-line interface for managing session data. Here are the available commands:

### Scraping Sessions

To scrape the latest session data from the Databricks website:
```bash
python -m dais_agenda.scraper
```

You can also run in preview mode to process only a few sessions (useful for testing):
```bash
python -m dais_agenda.scraper --preview
```

Or specify a custom number of sessions to process in preview mode:
```bash
python -m dais_agenda.scraper --preview --preview-count 5
```

This will:
- Fetch session data from the Databricks website using Selenium
- Save all sessions in a single JSONL file at `data/sessions_.jsonl` as well as in track-specific files
- Each session entry includes:
  - Session ID, title, and description
  - Track, level, and type
  - Industry and category information
  - Speaker details
  - Schedule information (day, room, start/end times in both local and PST)
  - Duration and path information

### Managing Sessions

To list all sessions:
```bash
python -m dais_agenda.cli list
```

To list sessions by track:
```bash
python -m dais_agenda.cli list --track "Data Engineering and Streaming"
```

To rate a session:
```bash
python -m dais_agenda.cli rate --session-id "10-reasons-use-databricks-delta-live-tables-your-next-data-processing" --rating 5
```

To add tags to a session:
```bash
python -m dais_agenda.cli tag --session-id "10-reasons-use-databricks-delta-live-tables-your-next-data-processing" --tags "spark,streaming,etl"
```

To get recommendations based on your ratings and tags:
```bash
python -m dais_agenda.cli recommend
```

## Development

The project uses a `src` layout for better organization and to avoid import issues. The main components are:

- `scraper.py`: Handles fetching and parsing session data from the Databricks website using Selenium
- `session_manager.py`: Manages session data storage and retrieval
- `cli.py`: Provides the command-line interface for user interaction

To run tests:
```bash
pytest
```

## License

This project is licensed under the MIT License - see the LICENSE file for details. 