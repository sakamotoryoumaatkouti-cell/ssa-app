"""
app.py - SSA試験対策アプリ: Streamlit Web UI
=============================================
スマホ最適化・高コントラスト設計の学習アプリケーション。
QuestionBank から事前蓄積済みの問題を高速に出題する。
"""

import json
import random
import hashlib

import gspread
import streamlit as st
from google.oauth2.service_account import Credentials

# ─── Streamlit ページ設定 ────────────────────────────
st.set_page_config(
    page_title="SSA試験対策",
    page_icon="🛡️",
    layout="centered",
    initial_sidebar_state="collapsed",
)

# ─── カスタムCSS（スマホ最適化・高コントラスト） ─────
st.markdown("""
<style>
    /* ── 全体のベース ── */
    .stApp {
        background-color: #0B0F19 !important;
    }
    section[data-testid="stSidebar"] {
        background-color: #111827 !important;
    }

    /* ── Streamlitの不要なUI（DeployリボンやFork/Githubアイコン等）を完全に隠す ── */
    header[data-testid="stHeader"] {
        display: none !important;
        visibility: hidden !important;
    }
    .stApp > header {
        display: none !important;
    }
    div[data-testid="stToolbar"] {
        display: none !important;
    }
    div[data-testid="stDecoration"] {
        display: none !important;
    }
    
    /* ── 右下のGitHub・Streamlitバッジ等も隠す ── */
    #MainMenu, footer {
        visibility: hidden !important;
    }
    .viewerBadge_container__1QSob, .streamlit-viewer-badge {
        display: none !important;
    }
    [data-testid="stStatusWidget"] {
        display: none !important;
    }

    /* ── フォント ── */
    @import url('https://fonts.googleapis.com/css2?family=Noto+Sans+JP:wght@400;700;900&display=swap');
    
    body {
        font-family: 'Noto Sans JP', sans-serif;
        color: #F3F4F6;
    }
    .stMarkdown, .stText, p, li, div[data-testid="stMarkdownContainer"] {
        font-family: 'Noto Sans JP', sans-serif !important;
        color: #F3F4F6 !important;
    }

    /* ── 見出し ── */
    h1, h2, h3 {
        color: #FFFFFF !important;
        font-weight: 900 !important;
        letter-spacing: 0.05em !important;
    }

    /* ── ボタン ── */
    .stButton > button {
        background: linear-gradient(135deg, #2563EB, #1D4ED8) !important;
        color: #FFFFFF !important;
        font-weight: 700 !important;
        font-size: 1.1rem !important;
        border: 1px solid #3B82F6 !important;
        border-radius: 8px !important;
        padding: 0.8rem 2rem !important;
        width: 100% !important;
        transition: all 0.2s ease !important;
        box-shadow: 0 4px 12px rgba(37, 99, 235, 0.25) !important;
    }
    .stButton > button:hover {
        transform: translateY(-2px) !important;
        box-shadow: 0 6px 16px rgba(37, 99, 235, 0.4) !important;
        background: linear-gradient(135deg, #3B82F6, #2563EB) !important;
    }
    .stButton > button:active {
        transform: translateY(0) !important;
    }

    /* ── 正解・不正解カード ── */
    .correct-card {
        background: linear-gradient(135deg, #064E3B, #022C22);
        border-left: 5px solid #10B981;
        border-radius: 8px;
        padding: 1.2rem;
        margin: 1rem 0;
        box-shadow: 0 4px 15px rgba(0,0,0,0.3);
    }
    .wrong-card {
        background: linear-gradient(135deg, #4C1D95, #2E1065);
        border-left: 5px solid #8B5CF6;
        border-radius: 8px;
        padding: 1.2rem;
        margin: 1rem 0;
        box-shadow: 0 4px 15px rgba(0,0,0,0.3);
    }
    .explanation-card {
        background: #1F2937;
        border-left: 5px solid #3B82F6;
        border-radius: 8px;
        padding: 1.2rem;
        margin: 1rem 0;
    }

    /* ── 進捗バー ── */
    .progress-bar-bg {
        background: #374151;
        border-radius: 8px;
        height: 24px;
        overflow: hidden;
        margin: 0.5rem 0;
    }
    .progress-bar-fill {
        background: linear-gradient(90deg, #3B82F6, #60A5FA);
        height: 100%;
        border-radius: 8px;
        display: flex;
        align-items: center;
        justify-content: center;
        color: #111827;
        font-weight: 900;
        font-size: 0.85rem;
        min-width: 40px;
        transition: width 0.5s ease;
    }

    /* ── 難易度バッジ ── */
    .difficulty-badge {
        display: inline-block;
        padding: 4px 12px;
        border-radius: 12px;
        font-weight: 700;
        font-size: 0.85rem;
        margin-bottom: 0.5rem;
        border: 1px solid rgba(255,255,255,0.1);
    }
    .diff-1 { background: #1E3A8A; color: #BFDBFE; }
    .diff-2 { background: #701A75; color: #F5D0FE; }
    .diff-3 { background: #991B1B; color: #FECACA; }

    /* ── クイズ選択肢ボタン ── */
    .quiz-option-btn > button {
        background: #1F2937 !important;
        color: #F3F4F6 !important;
        border: 1px solid #4B5563 !important;
        border-radius: 8px !important;
        text-align: left !important;
        font-size: 1rem !important;
        padding: 1.2rem !important;
    }
    .quiz-option-btn > button:hover {
        border-color: #60A5FA !important;
        background: #1E3A8A !important;
    }

    /* ── モバイル最適化 ── */
    @media (max-width: 768px) {
        .block-container {
            padding: 1rem 0.8rem !important;
        }
        h1 { font-size: 1.6rem !important; }
        h2 { font-size: 1.3rem !important; }
    }

    /* ── メトリクスカード ── */
    .metric-card {
        background: #1F2937;
        border-radius: 12px;
        padding: 1.2rem;
        text-align: center;
        border: 1px solid #374151;
        box-shadow: 0 4px 12px rgba(0,0,0,0.5);
    }
    .metric-value {
        font-size: 2.5rem;
        font-weight: 900;
        color: #60A5FA;
    }
    .metric-label {
        font-size: 0.85rem;
        color: #9CA3AF;
        margin-top: 0.3rem;
    }
</style>
""", unsafe_allow_html=True)


SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]


# ─── 認証 & データ取得（キャッシュ） ────────────────
@st.cache_resource(show_spinner=False)
def get_gspread_client() -> gspread.Client:
    """st.secrets からサービスアカウント情報を読み込む。"""
    creds_info = st.secrets["gcp_service_account"]
    creds = Credentials.from_service_account_info(dict(creds_info), scopes=SCOPES)
    return gspread.authorize(creds)


@st.cache_resource(show_spinner=False)
def get_spreadsheet(_gc: gspread.Client) -> gspread.Spreadsheet:
    return _gc.open_by_key(st.secrets["SPREADSHEET_ID"])


@st.cache_data(ttl=120, show_spinner=False)
def load_question_bank(_spreadsheet_id: str) -> list[dict]:
    """QuestionBank のデータを読み込む。ttl=120秒でキャッシュ。"""
    gc = get_gspread_client()
    ss = get_spreadsheet(gc)
    ws = ss.worksheet("QuestionBank")
    records = ws.get_all_records()
    # 行番号を付与（スプレッドシート上の実際の行: ヘッダー=1行目）
    for i, r in enumerate(records):
        r["_row_number"] = i + 2
    return records


@st.cache_data(ttl=300, show_spinner=False)
def load_raw_data(_spreadsheet_id: str) -> list[dict]:
    gc = get_gspread_client()
    ss = get_spreadsheet(gc)
    ws = ss.worksheet("RawData")
    return ws.get_all_records()


@st.cache_data(ttl=300, show_spinner=False)
def load_dictionary(_spreadsheet_id: str) -> list[dict]:
    gc = get_gspread_client()
    ss = get_spreadsheet(gc)
    try:
        ws = ss.worksheet("Dictionary")
        return ws.get_all_records()
    except gspread.exceptions.WorksheetNotFound:
        return []


@st.cache_data(ttl=300, show_spinner=False)
def load_settings(_spreadsheet_id: str) -> dict:
    gc = get_gspread_client()
    ss = get_spreadsheet(gc)
    ws = ss.worksheet("Settings")
    return {r["Key"]: r["Value"] for r in ws.get_all_records()}


def update_question_score(row_number: int, new_score: int, is_priority: bool) -> None:
    """QuestionBank の特定行のスコアとフラグを更新する。"""
    gc = get_gspread_client()
    ss = get_spreadsheet(gc)
    ws = ss.worksheet("QuestionBank")
    # Cumulative_Score は 9列目, Is_Priority は 10列目
    ws.update_cell(row_number, 9, new_score)
    ws.update_cell(row_number, 10, str(is_priority))


def update_setting(key: str, value: str) -> None:
    """Settings シートの値を更新する。"""
    gc = get_gspread_client()
    ss = get_spreadsheet(gc)
    ws = ss.worksheet("Settings")
    records = ws.get_all_records()
    for i, r in enumerate(records):
        if r["Key"] == key:
            ws.update_cell(i + 2, 2, value)  # Value列は2列目
            return
    # 存在しない場合は追記
    ws.append_row([key, value], value_input_option="RAW")


# ─── 認証ゲート ──────────────────────────────────────
def check_password() -> bool:
    """パスワード認証。通過済みならTrue。"""
    # スマホ等のリロード対策：URLパラメータで認証状態を維持する
    if "auth_token" in st.query_params and st.query_params["auth_token"] == "success":
        st.session_state["authenticated"] = True
        return True

    if st.session_state.get("authenticated"):
        return True

    st.markdown("## 🔐 SSA試験対策アプリ")
    st.markdown("アクセスするにはパスワードを入力してください。")
    password = st.text_input("パスワード", type="password", key="password_input")

    if st.button("ログイン", key="login_btn"):
        if password == st.secrets.get("APP_PASSWORD", ""):
            st.session_state["authenticated"] = True
            st.query_params["auth_token"] = "success"
            st.rerun()
        else:
            st.error("❌ パスワードが正しくありません。")

    return False


# ─── ページ: ホーム ──────────────────────────────────
def page_home() -> None:
    st.markdown("# 🛡️ SSA試験対策")
    st.markdown("### セーフティシニアアセッサ")

    sheet_id = st.secrets["SPREADSHEET_ID"]
    questions = load_question_bank(sheet_id)
    settings = load_settings(sheet_id)

    total = len(questions)
    target = int(settings.get("Target_Question_Count", 200))
    mastered = sum(
        1 for q in questions if int(q.get("Cumulative_Score", 0)) >= 3
    )
    in_progress = sum(
        1 for q in questions
        if 0 < int(q.get("Cumulative_Score", 0)) < 3
    )
    priority = sum(
        1 for q in questions if str(q.get("Is_Priority", "False")).lower() == "true"
    )

    # ─── ダッシュボード ───
    st.markdown("---")
    col1, col2, col3 = st.columns(3)
    with col1:
        st.markdown(f"""
        <div class="metric-card">
            <div class="metric-value">{mastered}</div>
            <div class="metric-label">✅ マスター済</div>
        </div>
        """, unsafe_allow_html=True)
    with col2:
        st.markdown(f"""
        <div class="metric-card">
            <div class="metric-value">{total}</div>
            <div class="metric-label">📚 生成済み</div>
        </div>
        """, unsafe_allow_html=True)
    with col3:
        st.markdown(f"""
        <div class="metric-card">
            <div class="metric-value">{target}</div>
            <div class="metric-label">🎯 目標</div>
        </div>
        """, unsafe_allow_html=True)

    # 進捗バー
    if total > 0:
        pct = min(100, int(mastered / total * 100))
    else:
        pct = 0
    st.markdown(f"""
    <div style="margin-top: 1rem;">
        <div style="font-weight: 700; margin-bottom: 4px;">
            学習進捗: {mastered}/{total} 問マスター ({pct}%)
        </div>
        <div class="progress-bar-bg">
            <div class="progress-bar-fill" style="width: {max(pct, 2)}%;">
                {pct}%
            </div>
        </div>
    </div>
    """, unsafe_allow_html=True)

    # 学習状況の詳細
    st.markdown("")
    col_a, col_b = st.columns(2)
    with col_a:
        st.markdown(f"🔄 **学習中**: {in_progress} 問")
    with col_b:
        st.markdown(f"🔥 **要復習**: {priority} 問")

    st.markdown("---")

    # 学習開始ボタン
    if total > 0:
        if st.button("🚀 学習を開始する", key="start_quiz_btn", use_container_width=True):
            st.session_state["page"] = "クイズ"
            st.session_state["quiz_index"] = 0
            st.session_state["answered"] = False
            st.session_state["selected_answer"] = None
            st.session_state["quiz_seed"] = random.randint(0, 999999)
            load_question_bank.clear()
            st.rerun()
    else:
        st.warning("⚠️ 問題がまだ生成されていません。generator.py を実行してください。")

    st.markdown("")
    if st.button("📖 用語・規格を閲覧する", key="go_input_btn", use_container_width=True):
        st.session_state["page"] = "インプット"
        st.rerun()


# ─── ページ: インプット ──────────────────────────────
def page_input() -> None:
    # 戻るボタンを上部に配置
    if st.button("🏠 ホームに戻る", key="back_home_btn", use_container_width=True):
        st.session_state["page"] = "ホーム"
        st.rerun()

    st.markdown("# 📖 用語・規格データ閲覧")
    sheet_id = st.secrets["SPREADSHEET_ID"]

    tab1, tab2 = st.tabs(["規格データ (RawData)", "用語辞書 (Dictionary)"])

    with tab1:
        st.info("規格原文は以下のリンクから外部サイト（kikakurui.com）にて直接閲覧してください。")
        st.markdown("""
        - 📘 [ISO 13849-1 (JIS B 9705-1)](https://kikakurui.com/b9/B9705-1-2019-01.html)
        - 📘 [ISO 13849-2 (JIS B 9705-2)](https://kikakurui.com/b9/B9705-2-2019-01.html)
        - 📘 [ISO 12100 (JIS B 9700)](https://kikakurui.com/b9/B9700-2013-01.html)
        """)

    with tab2:
        dict_data = load_dictionary(sheet_id)
        if not dict_data:
            st.info("用語データがありません。")
        else:
            search_d = st.text_input(
                "🔍 用語検索", key="dict_search",
                placeholder="用語名で検索…"
            )
            filtered_d = dict_data
            if search_d:
                s_lower = search_d.lower()
                filtered_d = [
                    r for r in dict_data
                    if s_lower in str(r.get("Term", "")).lower()
                    or s_lower in str(r.get("Description", "")).lower()
                ]
            st.caption(f"表示: {len(filtered_d)} / {len(dict_data)} 件")

            for item in filtered_d[:100]:
                with st.expander(f"📗 {item.get('Term', '(不明)')}"):
                    st.markdown(f"**説明**: {item.get('Description', '')}")
                    st.markdown(f"**要約**: {item.get('Summary', '')}")


# ─── ページ: クイズ ──────────────────────────────────
def page_quiz() -> None:
    st.markdown("# ❓ クイズ")

    sheet_id = st.secrets["SPREADSHEET_ID"]
    questions = load_question_bank(sheet_id)

    if not questions:
        st.warning("問題がありません。generator.py を実行してください。")
        return

    # ─── 出題リスト構築 ───
    if "quiz_seed" not in st.session_state:
        st.session_state["quiz_seed"] = random.randint(0, 999999)
    seed = st.session_state["quiz_seed"]

    priority_qs = [
        q for q in questions
        if str(q.get("Is_Priority", "False")).lower() == "true"
        and int(q.get("Cumulative_Score", 0)) < 3
    ]
    normal_qs = [
        q for q in questions
        if str(q.get("Is_Priority", "False")).lower() != "true"
        and int(q.get("Cumulative_Score", 0)) < 3
    ]
    
    def get_sort_key(q):
        return hashlib.md5(f"{seed}_{q.get('_row_number')}".encode()).hexdigest()
        
    normal_qs.sort(key=get_sort_key)
    quiz_pool = priority_qs + normal_qs

    if not quiz_pool:
        st.success("🎉 全問マスターしました！おめでとうございます！")
        mastered = sum(1 for q in questions if int(q.get("Cumulative_Score", 0)) >= 3)
        st.metric("マスター済み", f"{mastered}/{len(questions)}")
        return

    # ─── 現在の問題インデックス ───
    if "quiz_index" not in st.session_state:
        st.session_state["quiz_index"] = 0
    if "answered" not in st.session_state:
        st.session_state["answered"] = False
    if "selected_answer" not in st.session_state:
        st.session_state["selected_answer"] = None

    idx = st.session_state["quiz_index"]
    if idx >= len(quiz_pool):
        st.session_state["quiz_index"] = 0
        idx = 0

    q = quiz_pool[idx]

    # ─── 問題表示 ───
    remaining_count = len(quiz_pool)
    difficulty = int(q.get("Difficulty", 1))
    stars = "★" * difficulty + "☆" * (3 - difficulty)
    diff_class = f"diff-{difficulty}"
    diff_labels = {1: "基礎", 2: "応用", 3: "実務"}

    st.markdown(f"""
    <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 0.5rem;">
        <span class="difficulty-badge {diff_class}">{stars} {diff_labels.get(difficulty, '')}</span>
        <span style="color: #9E9E9E; font-size: 0.85rem;">残り {remaining_count} 問</span>
    </div>
    """, unsafe_allow_html=True)

    standard = q.get("Standard_Name", "")
    if standard:
        st.caption(f"📋 {standard}")

    st.markdown(f"### {q.get('Question_Text', '(問題文なし)')}")

    # 選択肢をパース
    options_raw = q.get("Options", "[]")
    if isinstance(options_raw, str):
        try:
            options = json.loads(options_raw)
        except json.JSONDecodeError:
            options = [options_raw]
    else:
        options = list(options_raw) if options_raw else []

    answer = q.get("Answer", "")
    row_num = q.get("_row_number", -1)
    current_score = int(q.get("Cumulative_Score", 0))

    # ─── 未回答時: 選択肢表示 ───
    if not st.session_state["answered"]:
        st.markdown("")
        for i, opt in enumerate(options):
            if st.button(
                opt,
                key=f"opt_{idx}_{i}",
                use_container_width=True,
            ):
                st.session_state["selected_answer"] = opt
                st.session_state["answered"] = True
                st.rerun()
    else:
        # ─── 回答済み: 正誤判定 ───
        selected = st.session_state["selected_answer"]
        is_correct = (selected == answer)

        if is_correct:
            new_score = current_score + 1
            is_priority = False
            st.markdown(f"""
            <div class="correct-card">
                <div style="font-size: 1.5rem; font-weight: 900;">⭕ 正解！</div>
                <div style="margin-top: 0.5rem;">あなたの回答: {selected}</div>
            </div>
            """, unsafe_allow_html=True)
        else:
            new_score = max(0, current_score - 1)
            is_priority = True
            st.markdown(f"""
            <div class="wrong-card">
                <div style="font-size: 1.5rem; font-weight: 900;">❌ 不正解</div>
                <div style="margin-top: 0.5rem;">あなたの回答: {selected}</div>
                <div style="font-weight: 700; margin-top: 0.3rem;">正解: {answer}</div>
            </div>
            """, unsafe_allow_html=True)

        # スコア更新（スプレッドシートに反映）
        if row_num > 0:
            try:
                update_question_score(row_num, new_score, is_priority)
            except Exception as e:
                st.toast(f"⚠️ スコア保存エラー: {e}")

        # 解説表示
        explanation = q.get("Explanation", "")
        if explanation:
            st.markdown(f"""
            <div class="explanation-card">
                <div style="font-weight: 700; margin-bottom: 0.5rem; color: #60A5FA;">
                    📘 アセッサ視点の解説
                </div>
                <div style="color: #D1D5DB;">{explanation}</div>
            </div>
            """, unsafe_allow_html=True)

        # 次の問題へ
        st.markdown("")
        if st.button("➡️ 次の問題へ", key="next_q_btn", use_container_width=True):
            st.session_state["quiz_index"] = idx + 1
            st.session_state["answered"] = False
            st.session_state["selected_answer"] = None
            # キャッシュをクリアして最新スコアを反映
            load_question_bank.clear()
            st.rerun()

    # ─── 中断ボタン（未回答・回答済み問わず常に最下部に表示） ───
    st.markdown("---")
    if st.button("🏠 中断してホームに戻る", key="interrupt_quiz_btn"):
        st.session_state["page"] = "ホーム"
        st.session_state["quiz_index"] = 0
        st.session_state["answered"] = False
        st.session_state["selected_answer"] = None
        st.rerun()


# ─── ページ: 設定 ────────────────────────────────────
def page_settings() -> None:
    st.markdown("# ⚙️ 設定 (Admin)")

    sheet_id = st.secrets["SPREADSHEET_ID"]
    settings = load_settings(sheet_id)

    current_target = int(settings.get("Target_Question_Count", 200))

    st.markdown("### 問題生成 目標数")
    st.markdown(f"現在の設定値: **{current_target}** 問")

    new_target = st.number_input(
        "新しい目標数",
        min_value=10,
        max_value=1000,
        value=current_target,
        step=10,
        key="target_input",
    )

    if st.button("💾 保存する", key="save_settings_btn"):
        try:
            update_setting("Target_Question_Count", str(int(new_target)))
            load_settings.clear()
            st.success(f"✅ 目標数を {int(new_target)} に変更しました。")
        except Exception as e:
            st.error(f"❌ 保存エラー: {e}")

    st.markdown("---")
    st.markdown("### キャッシュ管理")
    if st.button("🔄 全キャッシュをクリア", key="clear_cache_btn"):
        st.cache_data.clear()
        st.cache_resource.clear()
        st.success("✅ キャッシュをクリアしました。")
        st.rerun()

    st.markdown("---")
    st.markdown("### データ概要")
    try:
        questions = load_question_bank(sheet_id)
        raw_data = load_raw_data(sheet_id)
        dict_data = load_dictionary(sheet_id)

        col1, col2, col3 = st.columns(3)
        col1.metric("RawData", f"{len(raw_data)} 件")
        col2.metric("Dictionary", f"{len(dict_data)} 件")
        col3.metric("QuestionBank", f"{len(questions)} 件")

        # 難易度分布
        if questions:
            diff_counts = {1: 0, 2: 0, 3: 0}
            for q in questions:
                d = int(q.get("Difficulty", 1))
                diff_counts[d] = diff_counts.get(d, 0) + 1
            st.markdown("#### 難易度分布")
            st.markdown(
                f"⭐ 基礎: {diff_counts[1]} 問 / "
                f"⭐⭐ 応用: {diff_counts[2]} 問 / "
                f"⭐⭐⭐ 実務: {diff_counts[3]} 問"
            )
    except Exception as e:
        st.error(f"データ取得エラー: {e}")


# ─── メイン ──────────────────────────────────────────
def main() -> None:
    # 認証チェック
    if not check_password():
        st.stop()
        return

    # サイドバーナビゲーション
    with st.sidebar:
        st.markdown("## 🛡️ SSA試験対策")
        st.markdown("---")

        pages = ["ホーム", "インプット", "クイズ", "設定"]
        icons = ["🏠", "📖", "❓", "⚙️"]

        for page, icon in zip(pages, icons):
            if st.button(
                f"{icon} {page}",
                key=f"nav_{page}",
                use_container_width=True,
            ):
                st.session_state["page"] = page
                # クイズに切り替えたら状態リセット
                if page == "クイズ":
                    st.session_state["quiz_index"] = 0
                    st.session_state["answered"] = False
                    st.session_state["selected_answer"] = None
                    st.session_state["quiz_seed"] = random.randint(0, 999999)
                    load_question_bank.clear()
                st.rerun()

        st.markdown("---")
        st.caption("v1.0 | SSA Exam Prep")

    # ページルーティング
    current_page = st.session_state.get("page", "ホーム")

    if current_page == "ホーム":
        page_home()
    elif current_page == "インプット":
        page_input()
    elif current_page == "クイズ":
        page_quiz()
    elif current_page == "設定":
        page_settings()


if __name__ == "__main__":
    main()
