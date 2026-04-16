import argparse
import json
from datetime import date
from pathlib import Path


EVENT_TYPES = [
    "static_review",
    "dynamic_review",
    "presale_release",
    "test_drive_reservation",
    "delivery_start",
    "launch_release",
    "demo_car_arrival",
]


def _load_series_models(competitors_path: Path) -> dict[str, list[str]]:
    with competitors_path.open("r", encoding="utf-8") as f:
        raw = json.load(f)

    series_models: dict[str, list[str]] = {}
    if isinstance(raw, dict):
        for k, v in raw.items():
            if isinstance(v, list):
                series_models[str(k)] = [str(x) for x in v]
        return series_models

    if isinstance(raw, list):
        for item in raw:
            if isinstance(item, dict):
                for k, v in item.items():
                    if isinstance(v, list):
                        series_models[str(k)] = [str(x) for x in v]
        return series_models

    raise ValueError("Unsupported competitors.json format")


def build_events_json(models: list[str]) -> dict:
    return {
        "schema": "competitor_model_events",
        "schema_version": 1,
        "generated_at": date.today().isoformat(),
        "event_types": EVENT_TYPES,
        "models": {
            model: {event_type: [] for event_type in EVENT_TYPES} for model in models
        },
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--series", required=True)
    parser.add_argument(
        "--competitors",
        default=str(Path(__file__).resolve().parents[1].joinpath("schema/competitors.json")),
    )
    parser.add_argument(
        "--output",
        default=str(Path(__file__).resolve().parents[1].joinpath("schema/events.json")),
    )
    args = parser.parse_args()

    series_models = _load_series_models(Path(args.competitors))
    if args.series not in series_models:
        raise SystemExit(
            f"Series '{args.series}' not found in {args.competitors}. Available: {', '.join(sorted(series_models.keys()))}"
        )

    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    data = build_events_json(series_models[args.series])
    with output_path.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
        f.write("\n")


if __name__ == "__main__":
    main()
