"""
Deprecated wrapper.

This repository previously used `check_last_backup.py` as an entrypoint, but the
current pipeline runs `combined_backup_report.py` which:
- reads ServiceNow `tickets.json`
- validates snapshots (last 24h)
- triggers on-demand backups when stale (unless already running)
- writes `L2Backup/combined_backup_report.json` for `update_servicenow.py`
"""

from __future__ import annotations

from L2Backup.combined_backup_report import main


if __name__ == "__main__":
    main()
