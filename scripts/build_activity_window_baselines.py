import argparse
import json
import subprocess
import sys
from pathlib import Path


def _run_index_summary(
    python_bin: str,
    index_summary_path: Path,
    repo_root: Path,
    start: str,
    end: str,
    csv_out: Path,
) -> None:
    cmd = [
        python_bin,
        str(index_summary_path),
        "--start",
        start,
        "--end",
        end,
        "--csv-out",
        str(csv_out),
    ]
    p = subprocess.run(cmd, cwd=repo_root, capture_output=True, text=True)
    if p.returncode != 0:
        raise RuntimeError(
            f"index_summary 执行失败: start={start}, end={end}\nSTDERR:\n{p.stderr}\nSTDOUT:\n{p.stdout}"
        )
    return None


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--business-definition",
        default=str(Path(__file__).resolve().parents[1] / "schema" / "business_definition.json"),
    )
    parser.add_argument(
        "--index-summary",
        default=str(Path(__file__).resolve().parents[0] / "index_summary.py"),
    )
    parser.add_argument(
        "--output-dir",
        default=str(Path(__file__).resolve().parents[1] / "schema" / "activity_window_samples"),
    )
    parser.add_argument("--python-bin", default=sys.executable)
    args = parser.parse_args()

    repo_root = Path(__file__).resolve().parents[1]
    business_definition_path = Path(args.business_definition).resolve()
    index_summary_path = Path(args.index_summary).resolve()
    output_dir = Path(args.output_dir).resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    bd = json.loads(business_definition_path.read_text(encoding="utf-8"))
    periods = bd.get("time_periods") or {}

    sample_count = 0
    for model_code, win in periods.items():
        if not isinstance(win, dict):
            continue
        start = str(win.get("start") or "").strip()
        end = str(win.get("end") or "").strip()
        finish = str(win.get("finish") or "").strip()
        if not (start and end and finish):
            continue

        windows = [("presale", start, end), ("launch", end, finish)]

        for stage_key, s, e in windows:
            sample_id = f"{model_code}_{stage_key}"
            csv_out = output_dir / f"{sample_id}.csv"
            _run_index_summary(
                python_bin=str(args.python_bin),
                index_summary_path=index_summary_path,
                repo_root=repo_root,
                start=s,
                end=e,
                csv_out=csv_out,
            )
            sample_count += 1
            print(csv_out)
    print(f"sample_count={sample_count}")


if __name__ == "__main__":
    main()
