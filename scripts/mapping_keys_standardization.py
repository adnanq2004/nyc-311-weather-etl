import json
from pathlib import Path
import polars as pl

def clean_value(x: str, make_title=True) -> str:
    s = (
        pl.Series([str(x)])
        .str.strip_chars()
        .str.replace_all(r"\s+", " ")
        .item()
    )
    return s.title() if make_title else s

def clean_mapping_keys_and_values(mapping_dir = "mappings"):
    mapping_dir = Path(mapping_dir)

    for file in mapping_dir.glob("*.json"):
        print(f"Cleaning: {file.name}")

        with open(file, "r") as f:
            data = json.load(f)

        if isinstance(data, dict):
            cleaned = {clean_value(k): clean_value(v) for k, v in data.items()}

        elif isinstance(data, list):
            cleaned = [clean_value(item) for item in data]

        else:
            print(f"Skipping {file.name} (unsupported format)")
            continue

        # Overwrite the file
        with open(file, "w") as f:
            json.dump(cleaned, f, indent=4)

        print(f"Saved cleaned file: {file.name}\n")

    print("All mapping files processed.\n")

if __name__ == "__main__":
    clean_mapping_keys_and_values()
