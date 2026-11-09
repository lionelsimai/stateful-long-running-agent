"""Benchmark: kill -9 resume test — run the crash test and report."""
import subprocess
import sys
from pathlib import Path


def main() -> int:
    """Run examples/crash_test.py as the benchmark."""
    root = Path(__file__).resolve().parents[1]
    proc = subprocess.run([sys.executable, str(root / "examples" / "crash_test.py")],
                          capture_output=True, text=True, check=False)
    print(proc.stdout)
    print(proc.stderr, file=sys.stderr)
    print("target: task resumes cleanly after kill -9 -> "
          f"{'PASS' if proc.returncode == 0 else 'FAIL'}")
    return proc.returncode


if __name__ == "__main__":
    raise SystemExit(main())
