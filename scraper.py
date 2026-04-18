"""
scraper.py - SSA試験対策アプリ: データ収集バッチ
==================================================
kikakurui.com から規格テキスト・テーブルをスクレイピングし、
Google Sheets (RawData) に蓄積する。

【抽出戦略】
1. テーブル : <table>タグを個別に1行として保存（構造保持）
2. 図       : <img>のalt/URLを文脈付きで1行として保存
3. 本文     : ISO規格の番号付き見出し（3.1.2, 4.2 等）で分割後、
              1000〜1500字のチャンクに分割して保存
"""

import os
import re
import time
from urllib.parse import urljoin

import gspread
import requests
from bs4 import BeautifulSoup, Tag
from google.oauth2.service_account import Credentials

# ─── 設定 ────────────────────────────────────────────
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

URLS = [
    {
        "url": "https://kikakurui.com/b9/B9705-1-2019-01.html",
        "standard": "ISO 13849-1 (JIS B 9705-1)",
    },
    {
        "url": "https://kikakurui.com/b9/B9705-2-2019-01.html",
        "standard": "ISO 13849-2 (JIS B 9705-2)",
    },
    {
        "url": "https://kikakurui.com/b9/B9700-2013-01.html",
        "standard": "ISO 12100 (JIS B 9700)",
    },
]

REQUEST_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    )
}

CHUNK_TARGET = 1200  # 1チャンクの目標文字数
CHUNK_MIN    = 100   # これ未満のチャンクは破棄


# ─── 認証 ────────────────────────────────────────────
def load_config_from_secrets() -> dict:
    """.streamlit/secrets.toml から設定を読み出す。"""
    secrets_path = os.path.join(".streamlit", "secrets.toml")
    if not os.path.exists(secrets_path):
        sa_path = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON")
        sheet_id = os.environ.get("SPREADSHEET_ID")
        if sa_path and sheet_id:
            return {"sa_path": sa_path, "sheet_id": sheet_id, "type": "file"}
        raise FileNotFoundError(".streamlit/secrets.toml が見つかりません。")

    import tomllib
    with open(secrets_path, "rb") as f:
        config = tomllib.load(f)

    return {
        "creds_info": config.get("gcp_service_account"),
        "sheet_id":   config.get("SPREADSHEET_ID"),
        "type":       "dict",
    }


def get_gspread_client(config: dict) -> gspread.Client:
    if config["type"] == "dict":
        creds = Credentials.from_service_account_info(config["creds_info"], scopes=SCOPES)
    else:
        creds = Credentials.from_service_account_file(config["sa_path"], scopes=SCOPES)
    return gspread.authorize(creds)


def get_spreadsheet(gc: gspread.Client, config: dict) -> gspread.Spreadsheet:
    sheet_id = config["sheet_id"]
    if not sheet_id:
        raise ValueError("SPREADSHEET_ID が secrets.toml に設定されていません。")
    return gc.open_by_key(sheet_id)


# ─── シート初期化 ────────────────────────────────────
def init_sheets(spreadsheet: gspread.Spreadsheet) -> None:
    """必要なシートとヘッダーが無ければ作成する。"""
    sheet_defs = {
        "RawData": ["Source_ID", "Source_URL", "Content_Type", "Content"],
        "Dictionary": ["Source_ID", "Term", "Description", "Summary"],
        "QuestionBank": [
            "Question_ID", "Source_ID", "Standard_Name", "Difficulty",
            "Question_Text", "Options", "Answer", "Explanation",
            "Cumulative_Score", "Is_Priority", "Image_URL",
        ],
        "Settings": ["Key", "Value"],
    }
    existing = {ws.title for ws in spreadsheet.worksheets()}
    for name, headers in sheet_defs.items():
        if name not in existing:
            ws = spreadsheet.add_worksheet(title=name, rows=2000, cols=len(headers))
            ws.append_row(headers, value_input_option="RAW")
            print(f"  ✅ シート '{name}' を作成しました。")
        else:
            ws = spreadsheet.worksheet(name)
            if not ws.row_values(1):
                ws.append_row(headers, value_input_option="RAW")

    settings_ws = spreadsheet.worksheet("Settings")
    existing_keys = {r["Key"] for r in settings_ws.get_all_records()}
    if "Target_Question_Count" not in existing_keys:
        settings_ws.append_row(["Target_Question_Count", "200"], value_input_option="RAW")
        print("  ✅ Target_Question_Count = 200 をセットしました。")


# ─── スクレイピング ──────────────────────────────────
def fetch_html(url: str) -> BeautifulSoup:
    print(f"  📥 Fetching: {url}")
    resp = requests.get(url, headers=REQUEST_HEADERS, timeout=30)
    resp.raise_for_status()
    resp.encoding = resp.apparent_encoding
    return BeautifulSoup(resp.text, "html.parser")


def remove_noise_elements(soup: BeautifulSoup) -> None:
    """ナビゲーション・広告・フッターなどを除去し、本文の純度を上げる。"""
    for tag in ["script", "style", "noscript", "nav", "footer", "header"]:
        for elem in soup.find_all(tag):
            elem.decompose()

    noise_classes = ["breadcrumb", "breadcrumbs", "sidebar", "ad",
                     "advertisement", "navigation", "menu", "pager"]
    for cls in noise_classes:
        for elem in soup.find_all(class_=lambda c: c and cls in " ".join(c).lower()):
            elem.decompose()

    noise_ids = ["header", "footer", "sidebar", "nav", "navigation", "breadcrumb"]
    for nid in noise_ids:
        for elem in soup.find_all(id=lambda i: i and nid in i.lower()):
            elem.decompose()


def extract_sections(soup: BeautifulSoup, base_url: str) -> list[dict]:
    """
    規格本文を「文脈を保ったMarkdown風の読みやすいテキスト」に変換し、チャンク化する。
    """
    results: list[dict] = []

    # コンテンツ領域を特定
    content_area = (
        soup.find("div", {"class": "kijun"})
        or soup.find("div", {"id": "main"})
        or soup.find("article")
        or soup.find("div", {"id": "content"})
        or soup.find("div", {"class": "content"})
        or soup.find("body")
    )
    if not content_area:
        return results

    # ────────────────────────────────────────────────────
    # ① DOMの事前整形（Markdown風にタグをプレーンテキスト化）
    # ────────────────────────────────────────────────────
    
    # 画像は邪魔になるので全て削除（本文テキストに集中する）
    for img in content_area.find_all("img"):
        img.decompose()
        
    # 余計なスクリプト等も削除
    for script in content_area.find_all(["script", "style", "noscript"]):
        script.decompose()

    # テーブルのMarkdown化
    for table in content_area.find_all("table"):
        rows = table.find_all("tr")
        for i, tr in enumerate(rows):
            cells = [td.get_text(" ", strip=True) for td in tr.find_all(["td", "th"])]
            if any(cells):
                row_str = "| " + " | ".join(cells) + " |"
                # 1行目の後にMarkdownの区切り線を追加
                if i == 0:
                    sep = "| " + " | ".join(["---"] * len(cells)) + " |"
                    row_str += "\n" + sep
                tr.replace_with("\n" + row_str + "\n")
        table.insert_before("\n\n")
        table.insert_after("\n\n")

    # 見出しとリストのMarkdown化
    for h in content_area.find_all(["h1", "h2", "h3", "h4", "h5", "h6"]):
        level = int(h.name[1])
        h.insert_before(f"\n\n{'#' * level} ")
        h.insert_after("\n\n")

    for li in content_area.find_all("li"):
        li.insert_before("\n- ")

    # ────────────────────────────────────────────────────
    # ② 余分な空白の除去とノイズクリーニング
    # ────────────────────────────────────────────────────
    full_text = content_area.get_text("\n", strip=True)

    # 連続する改行をきれいに（空行は1行までに制限）
    full_text = re.sub(r"\n{3,}", "\n\n", full_text)
    
    # ノイズ（ページ番号、ヘッダー/フッターの定型文）を除去
    full_text = re.sub(r"(?m)^\s*\d{1,3}\s*$", "", full_text)
    full_text = re.sub(r"2019年7月1日の法改正.*?読み替えてください。\n?", "", full_text)

    # ────────────────────────────────────────────────────
    # ③ ISO規格の番号付き見出しで分割点を検出してチャンク化
    # ────────────────────────────────────────────────────
    SECTION_PATTERN = re.compile(
        r"(?m)"
        r"^(?:#+\s*)?(\d{1,2}(?:\.\d{1,2}){0,3})\s*\n"    # "# 4.2.1\n" または "4.2.1\n"
        r"|^(?:#+\s*)?(\d{1,2}(?:\.\d{1,2}){0,3})\s{2,}"   # 後の空白2つ以上
    )

    split_positions = [0]
    for m in SECTION_PATTERN.finditer(full_text):
        pos = m.start()
        # 細かすぎる分割を防ぐ（最低文字数）
        if pos - split_positions[-1] > 300:
            split_positions.append(pos)
    split_positions.append(len(full_text))

    for i in range(len(split_positions) - 1):
        section = full_text[split_positions[i]:split_positions[i + 1]].strip()
        if len(section) < CHUNK_MIN:
            continue

        if len(section) <= CHUNK_TARGET * 1.5:
            results.append({"type": "text", "content": section})
        else:
            # 大きすぎるセクションは改行位置で強制分割
            lines = section.split("\n")
            chunk: list[str] = []
            chunk_len = 0
            for line in lines:
                chunk.append(line)
                chunk_len += len(line)
                if chunk_len >= CHUNK_TARGET and not line.strip().startswith("|"):
                    # 表の途中( | )では途中で切らない工夫
                    results.append({"type": "text", "content": "\n".join(chunk)})
                    chunk = []
                    chunk_len = 0
            if chunk:
                results.append({"type": "text", "content": "\n".join(chunk)})

    return results


def truncate_content(text: str, max_chars: int = 40000) -> str:
    """Google Sheets セルの文字数制限に収まるよう切り詰める。"""
    return text[:max_chars] + "…(truncated)" if len(text) > max_chars else text


# ─── メイン処理 ──────────────────────────────────────
def main() -> None:
    print("=" * 60)
    print("SSA Scraper - データ収集バッチ 開始")
    print("=" * 60)

    try:
        config = load_config_from_secrets()
    except Exception as e:
        print(f"❌ 設定の読み込みに失敗しました: {e}")
        return

    gc = get_gspread_client(config)
    spreadsheet = get_spreadsheet(gc, config)

    print("\n[1/3] シートを初期化しています…")
    init_sheets(spreadsheet)

    raw_ws = spreadsheet.worksheet("RawData")
    existing_rows = raw_ws.get_all_values()
    existing_count = max(0, len(existing_rows) - 1)
    print(f"\n  📊 RawData 既存レコード数: {existing_count}")
    if existing_count > 0:
        print("  ⚠️  既にデータがあります。追記します。")

    id_counter = existing_count + 1
    all_rows: list[list[str]] = []

    print("\n[2/3] スクレイピングを開始します…")
    for i, entry in enumerate(URLS):
        url = entry["url"]
        standard = entry["standard"]
        print(f"\n--- [{i + 1}/{len(URLS)}] {standard} ---")

        try:
            soup = fetch_html(url)
            remove_noise_elements(soup)
            sections = extract_sections(soup, url)
            print(f"  📄 抽出チャンク数: {len(sections)}")

            for sec in sections:
                source_id = f"raw_{id_counter:04d}"
                content = truncate_content(sec["content"])
                all_rows.append([source_id, url, sec["type"], content])
                id_counter += 1

        except Exception as e:
            print(f"  ❌ エラー: {e}")

        if i < len(URLS) - 1:
            print("  ⏳ 2秒待機中…")
            time.sleep(2)

    if all_rows:
        print(f"\n[3/3] {len(all_rows)} 行を RawData に書き込みます…")
        BATCH = 50
        for start in range(0, len(all_rows), BATCH):
            chunk = all_rows[start: start + BATCH]
            raw_ws.append_rows(chunk, value_input_option="RAW")
            print(f"  ✅ {start + len(chunk)}/{len(all_rows)} 行 書き込み完了")
            time.sleep(1)
    else:
        print("\n⚠️  書き込み可能なデータがありませんでした。")

    print("\n" + "=" * 60)
    print("✅ スクレイピング完了！")
    print(f"   新規追加: {len(all_rows)} チャンク")
    print(f"   内訳 → テキスト: {sum(1 for r in all_rows if r[2]=='text')} 件"
          f" / テーブル: {sum(1 for r in all_rows if r[2]=='table')} 件"
          f" / 図: {sum(1 for r in all_rows if r[2]=='image')} 件")
    print("=" * 60)


if __name__ == "__main__":
    main()
