# create_json.py

import re
import json
from pathlib import Path

def create_structured_json(dx_path, output_path):
    with open(dx_path, "r", encoding="utf-8") as f:
        content = f.read()

    blocks = re.split(r"\n(?=##TITLE=)", content)

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

        if block.get("TITLE") == "DATA" or block.get("DATA TYPE") == "LINK":
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
        return block

    parsed = [parse_block(b) for b in blocks if b.strip()]
    parsed = [p for p in parsed if p is not None]

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(parsed, f, indent=2)

    print(f"✅ Structured JSON saved to {output_path}")
