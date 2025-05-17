import re
import json
from pathlib import Path
from datetime import datetime

def create_structured_json(dx_path, output_path, log_path="logs/create_json.log"):
    log_lines = []

    def log(msg):
        timestamped = f"{datetime.now().isoformat()} - {msg}"
        print(timestamped)
        log_lines.append(timestamped)

    log(f"📂 Reading DX file from: {dx_path}")
    with open(dx_path, "r", encoding="utf-8") as f:
        content = f.read()

    blocks = re.split(r"\n(?=##TITLE=)", content)
    log(f"🔍 Found {len(blocks)} raw blocks")

    def parse_block(block_text):
        block = {}
        lines = block_text.strip().splitlines()
        data_lines = []
        in_data = False

        for line in lines:
            if line.startswith("##"):
                match = re.match(r"##(.+?)=(.*)", line)
                if match:
                    key, value = match.groups()
                    key = key.strip().upper()
                    value = value.strip()
                    if key == "XYDATA":
                        in_data = True
                    block[key] = value
            elif in_data:
                data_lines.append(line.strip())

        title = block.get("TITLE")
        if title == "DATA" or block.get("DATA TYPE", "").upper() == "LINK":
            log(f"⏩ Skipping block with title={title} or type=LINK")
            return None

        xy_data = []
        for line in data_lines:
            tokens = line.strip().split()
            if tokens:
                x = float(tokens[0])
                y_values = [float(val) for val in tokens[1:]]
                xy_data.append({"X": x, "Y": y_values})
        if xy_data:
            block["XY"] = xy_data
            log(f"📊 Parsed block: title={title}, XY points={len(xy_data)}")
        else:
            log(f"⚠️ Block {title} has no XY data")

        # Add source file name
        block["SOURCE_FILE"] = Path(dx_path).name

        return block

    parsed = [parse_block(b) for b in blocks if b.strip()]
    parsed = [p for p in parsed if p is not None]
    log(f"✅ Parsed {len(parsed)} blocks successfully")

    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(parsed, f, indent=2)
    log(f"💾 Structured JSON saved to {output_path}")

    Path(log_path).parent.mkdir(parents=True, exist_ok=True)
    with open(log_path, "w", encoding="utf-8") as f:
        f.write("\n".join(log_lines))
    log(f"📝 Log written to {log_path}")
