"""
generator.py - SSA試験対策アプリ: 問題自動生成ループ (Google GenAI 最新版)
====================================================
RawData / Dictionary から未使用素材をピックアップし、
Gemini API で問題を生成して QuestionBank に蓄積する。
"""

import json
import os
import random
import re
import time

import gspread
from google import genai
from google.oauth2.service_account import Credentials

# ─── 設定 ────────────────────────────────────────────
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

BATCH_SIZE = 5          # 1回のAPI呼び出しで生成する問題数
SLEEP_BETWEEN = 60      # 各バッチ間の待機秒数（無料枠レート制限回避）

# 1.5-flash または 2.0-flash を使用可能
MODEL_ID = "gemini-2.5-flash" 

SYSTEM_PROMPT = """\
あなたは機械安全分野の最高峰であるセーフティシニアアセッサ（SSA）を育成するための試験作成委員です。
提供された規格データに基づき、アセッサとしての実務的な視点を試す高度な四択試験問題を作成してください。

【重要】毎回同じような「～について最も適切なものを選べ」という問題にならないよう、以下の【出題パターン】からランダムに1つを選んで問題文を構成してください。

【出題パターン】
パターンA（実務シナリオ型）:
架空の機械設計や現場のトラブル場面を提示し、規格に照らし合わせてどの対応が正しいかを問う。
(例) 「ロボットセル内で作業者がメンテナンスを行う設計において、HAZOP分析をした結果... この場合ISO12100に基づく正しい措置はどれか。」

パターンB（誤り指摘型）:
4つの選択肢の中に、実務者が陥りやすい「もっともらしいが規格上は間違っている」設計方針や解釈を1つ（または3つ）混ぜ、正しいもの（または誤っているもの）を指摘させる。

パターンC（理由・根拠型）:
「なぜその措置・数値が必要なのか」という規格の背景にある考え方や理由を問う。

パターンD（定義・穴埋め型）:
規格特有の厳密な用語や数値基準について、正しく理解しているかを問う。

【ルール】
- 難易度(1〜3)を適切に設定すること。1: 基本語彙、2: 規格解釈、3: 現場での複合的な実務判断
- 誤選択肢には、「昔の規格なら正解だった考え方」や「安全側への倒しすぎで非現実的なもの」など、プロでも迷う巧妙なものを混ぜてください。
- 提供される素材ごとに1問ずつ、合計{batch_size}問を生成してください。

出力は **必ず** 以下のJSON配列形式とすること（JSON以外は一切含めないこと）:
[
  {{
    "source_id": "提供されたSource_ID",
    "difficulty": 1,
    "question": "問題文",
    "options": ["A: 選択肢1", "B: 選択肢2", "C: 選択肢3", "D: 選択肢4"],
    "answer": "A: 選択肢1",
    "explanation": "解説"
  }},
  ...
]
"""

def load_config() -> dict:
    secrets_path = os.path.join(".streamlit", "secrets.toml")
    
    # ローカル実行時 (.streamlit/secrets.toml がある場合)
    if os.path.exists(secrets_path):
        import tomllib
        with open(secrets_path, "rb") as f:
            config = tomllib.load(f)
        return {
            "creds_info": config.get("gcp_service_account"),
            "sheet_id": config.get("SPREADSHEET_ID"),
            "gemini_api_key": config.get("GEMINI_API_KEY"),
        }
    
    # GitHub Actions等の環境変数から実行される場合
    gcp_json_str = os.environ.get("GCP_SERVICE_ACCOUNT")
    if not gcp_json_str:
        raise ValueError("secrets.toml または環境変数 GCP_SERVICE_ACCOUNT が見つかりません。")
        
    return {
        "creds_info": json.loads(gcp_json_str),
        "sheet_id": os.environ.get("SPREADSHEET_ID"),
        "gemini_api_key": os.environ.get("GEMINI_API_KEY"),
    }

def get_gspread_client(creds_info: dict) -> gspread.Client:
    creds = Credentials.from_service_account_info(creds_info, scopes=SCOPES)
    return gspread.authorize(creds)

def url_to_standard_name(url: str) -> str:
    if "B9705-1" in url: return "ISO 13849-1"
    if "B9705-2" in url: return "ISO 13849-2"
    if "B9700" in url: return "ISO 12100"
    return "Unknown"

def extract_image_url(content: str) -> str:
    match = re.search(r"画像URL:\s*(https?://\S+)", content)
    return match.group(1) if match else ""

def get_unused_sources(spreadsheet: gspread.Spreadsheet, used_ids: set[str], count: int = BATCH_SIZE) -> list[dict]:
    candidates: list[dict] = []
    try:
        raw_ws = spreadsheet.worksheet("RawData")
        for r in raw_ws.get_all_records():
            sid = str(r.get("Source_ID", ""))
            if sid and sid not in used_ids:
                c_type = r.get("Content_Type", "text")
                if c_type == "image":
                    continue
                candidates.append({
                    "source_id": sid,
                    "source_url": r.get("Source_URL", ""),
                    "content_type": c_type,
                    "content": str(r.get("Content", ""))[:3000],
                })
    except Exception as e: print(f"  ⚠️ RawDataエラー: {e}")
    random.shuffle(candidates)
    return candidates[:count]

def main() -> None:
    print("=" * 60)
    print("SSA Generator (Latest SDK) - 開始")
    print("=" * 60)

    config = load_config()
    client = genai.Client(api_key=config["gemini_api_key"])
    gc = get_gspread_client(config["creds_info"])
    spreadsheet = gc.open_by_key(config["sheet_id"])

    settings_ws = spreadsheet.worksheet("Settings")
    settings = {r["Key"]: r["Value"] for r in settings_ws.get_all_records()}
    target_count = int(settings.get("Target_Question_Count", 200))

    qb_ws = spreadsheet.worksheet("QuestionBank")
    qb_records = qb_ws.get_all_records()
    current_count = len(qb_records)
    print(f"  🎯 目標: {target_count} | 現状: {current_count}")

    if current_count >= target_count:
        print("✅ 目標達成済みです。")
        return

    used_source_ids = {str(r.get("Source_ID", "")) for r in qb_records if r.get("Source_ID")}
    id_counter = current_count + 1

    while current_count < target_count:
        remaining = target_count - current_count
        batch_count = min(BATCH_SIZE, remaining)
        sources = get_unused_sources(spreadsheet, used_source_ids, count=batch_count)
        
        if not sources:
            print("⚠️ 未使用素材が終了しました。")
            break

        # プロンプト組み立て
        prompt_parts = [f"--- 素材 {i+1} ---\nSource_ID: {s['source_id']}\n種別: {s['content_type']}\n内容: {s['content']}" for i, s in enumerate(sources)]
        user_prompt = "以下の素材に基づいて問題を生成してください:\n\n" + "\n\n".join(prompt_parts)

        print(f"  📤 Gemini {MODEL_ID} にリクエスト中...")
        try:
            response = client.models.generate_content(
                model=MODEL_ID,
                contents=user_prompt,
                config={
                    "system_instruction": SYSTEM_PROMPT.format(batch_size=len(sources)),
                    "response_mime_type": "application/json",
                }
            )
            
            # response.text が直接 JSON 配列になる（response_mime_type指定のおかげ）
            questions = json.loads(response.text)
            
            rows = []
            for q in questions:
                sid = q.get("source_id", "unknown")
                s_url = next((s["source_url"] for s in sources if s["source_id"] == sid), "")
                img_url = next((extract_image_url(s["content"]) for s in sources if s["source_id"] == sid and s["content_type"] == "image"), "")
                
                rows.append([
                    f"q_{id_counter:04d}", 
                    sid, 
                    url_to_standard_name(s_url),
                    int(q.get("difficulty", 1)),
                    q.get("question", ""),
                    json.dumps(q.get("options", []), ensure_ascii=False),
                    q.get("answer", ""),
                    q.get("explanation", ""),
                    0, "False", img_url
                ])
                id_counter += 1
                current_count += 1
                used_source_ids.add(sid)

            if rows:
                qb_ws.append_rows(rows, value_input_option="RAW")
                print(f"  💾 {len(rows)}問を保存しました。({current_count}/{target_count})")
            
        except Exception as e:
            print(f"  ❌ エラー: {e}")
            time.sleep(10) # 短い待機でリトライ

        # GitHub Actions環境では1バッチで終了（毎時のcronに任せる）
        if os.environ.get("GITHUB_ACTIONS") == "true":
            print("  ℹ️ GitHub Actions環境のため、1バッチで正常終了します。")
            break

        time.sleep(SLEEP_BETWEEN)

    print("=" * 60)
    print("✅ 完了")

if __name__ == "__main__":
    main()
