# src/tools/fix_geojson.py
from pathlib import Path
import json, ast, gzip, sys

SRC = Path("src/assets/prefectures.geojson")
DST = Path("src/assets/prefectures.fixed.geojson")

def try_json(txt: str):
    json.loads(txt)  # 例外が出なければOK
    return True

def main():
    if not SRC.exists():
        print(f"not found: {SRC}"); sys.exit(1)

    raw_bytes = SRC.read_bytes()

    # 1) まず素直にテキストとして試す
    try:
        txt = raw_bytes.decode("utf-8")
        if try_json(txt):
            DST.write_text(txt, encoding="utf-8")
            print(f"saved fixed: {DST} (plain utf-8)")
            return
    except Exception:
        pass

    # 2) バイト列リテラル b'...'
    try:
        s = raw_bytes.decode("utf-8", errors="ignore").strip()
        if s.startswith(("b'", 'b"')) and s.endswith(("'", '"')):
            b = ast.literal_eval(s)       # -> bytes
            txt = b.decode("utf-8")
            if try_json(txt):
                DST.write_text(txt, encoding="utf-8")
                print(f"saved fixed: {DST} (bytes-literal)")
                return
    except Exception:
        pass

    # 3) gzip圧縮を疑う
    try:
        if raw_bytes[:2] == b"\x1f\x8b":
            txt = gzip.decompress(raw_bytes).decode("utf-8")
            if try_json(txt):
                DST.write_text(txt, encoding="utf-8")
                print(f"saved fixed: {DST} (gunzipped)")
                return
    except Exception:
        pass

    print("Failed to recognize as JSON / bytes-literal / gzip. Please re-download a GeoJSON file.")

if __name__ == "__main__":
    main()

