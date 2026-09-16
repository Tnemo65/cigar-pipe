import subprocess
import sys


def test_run_cd_job_help_parses_without_duplicate_options():
    result = subprocess.run(
        [sys.executable, "scripts/run_cd_job.py", "--help"],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0
    assert result.stderr == ""
