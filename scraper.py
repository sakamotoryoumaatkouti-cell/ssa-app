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


def _table_to_text(table_tag: Tag) -> str:
    """<table> タグをセルの内容を保ちつつテキスト形式に変換する。"""
    rows: list[str] = []
    for tr in table_tag.find_all("tr"):
        cols = [
            cell.get_text(" ", strip=True)
            for cell in tr.find_all(["td", "th"], recursive=False)
        ]
        if any(cols):
            rows.append(" | ".join(cols))
    return "\n".join(rows)


def _is_decorative_image(alt: str, src: str) -> bool:
    """装飾画像（アイコン・ロゴ等）を除外する判定。"""
    kws = ["icon", "logo", "banner", "btn", "arrow", "spacer", "blank"]
    return not alt and any(k in src.lower() for k in kws)


def _prev_context(tag: Tag, max_chars: int = 100) -> str:
    """タグの直前にある最も近いテキストブロックを文脈として返す。"""
    for prev in tag.find_all_previous(["p", "h1", "h2", "h3", "h4", "h5"]):
        t = prev.get_text(" ", strip=True)
        if len(t) > 5:
            return t[:max_chars]
    return ""


def extract_sections(soup: BeautifulSoup, base_url: str) -> list[dict]:
    """
    規格本文を「Geminiが問題を生成しやすい粒度のチャンク」として抽出する。

    戦略:
    ① テーブル  → 個別に1行として保存 (構造保持)
    ② 図(img)   → alt/URLを文脈付きで1行として保存
    ③ 本文テキスト
       → ISO番号付き見出し(3.1.2, 4.2 等)で分割
       → さらに1000〜1500字のチャンクに分割して保存
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
    # ① テーブルを個別抽出 → DOMから除去
    # ────────────────────────────────────────────────────
    for table in content_area.find_all("table"):
        if table.find_parent("table"):
            continue  # ネストされたサブテーブルはスキップ
        table_text = _table_to_text(table)
        if len(table_text) > 20:
            ctx = _prev_context(table)
            header = f"【表: {ctx}】\n" if ctx else "【表】\n"
            results.append({"type": "table", "content": header + table_text})
        table.decompose()

    # ────────────────────────────────────────────────────
    # ② 図(img)を個別抽出 → DOMから除去
    # ────────────────────────────────────────────────────
    for img in content_area.find_all("img"):
        alt = img.get("alt", "")
        src = img.get("src", "")
        if src and not src.startswith("http"):
            src = urljoin(base_url, src)
        if _is_decorative_image(alt, src):
            img.decompose()
            continue
        if alt or src:
            ctx = _prev_context(img, max_chars=80)
            content = f"【図】{ctx}\n説明(alt): {alt}\n画像URL: {src}"
            results.append({"type": "image", "content": content})
        img.decompose()

    # ────────────────────────────────────────────────────
    # ③ 残りの本文テキストを取得してクリーニング
    # ────────────────────────────────────────────────────
    full_text = content_area.get_text("\n", strip=True)

    # ノイズ除去
    full_text = re.sub(r"\n{3,}", "\n\n", full_text)
    full_text = re.sub(r"(?m)^\s*\d{1,3}\s*$", "", full_text)    # ページ番号行
    full_text = re.sub(
        r"2019年7月1日の法改正.*?読み替えてください。\n?", "", full_text
    )

    # ────────────────────────────────────────────────────
    # ④ ISO規格の番号付き見出しで分割点を検出
    #    例: "3.1.2\n用語定義" や "4  設計上の考慮事項"
    # ────────────────────────────────────────────────────
    SECTION_PATTERN = re.compile(
        r"(?m)"
        r"^(\d{1,2}(?:\.\d{1,2}){0,3})\s*\n"    # "4.2.1\n" 形式
        r"|^(\d{1,2}(?:\.\d{1,2}){0,3})\s{2,}"   # "4.2.1  " 形式（空白2つ以上）
    )

    split_positions = [0]
    for m in SECTION_PATTERN.finditer(full_text):
        pos = m.start()
        if pos - split_positions[-1] > 200:
            split_positions.append(pos)
    split_positions.append(len(full_text))

    # ────────────────────────────────────────────────────
    # ⑤ セクションを 1000〜1500字 のチャンクに分割して保存
    # ────────────────────────────────────────────────────
    for i in range(len(split_positions) - 1):
        section = full_text[split_positions[i]:split_positions[i + 1]].strip()
        if len(section) < CHUNK_MIN:
            continue

        if len(section) <= CHUNK_TARGET * 1.5:
            results.append({"type": "text", "content": section})
        else:
            # 改行位置でチャンク化
            lines = section.split("\n")
            chunk: list[str] = []
            chunk_len = 0
            for line in lines:
                chunk.append(line)
                chunk_len += len(line)
                if chunk_len >= CHUNK_TARGET:
                    results.append({"type": "text", "content": "\n".join(chunk)})
                    chunk = []
                    chunk_len = 0
            if chunk and chunk_len >= CHUNK_MIN:
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
