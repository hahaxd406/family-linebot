import sys
sys.stdout.reconfigure(line_buffering=True)

import os
import re
import json
import hmac
import base64
import hashlib
import datetime
import calendar
import threading
import psycopg
from psycopg.rows import dict_row
import secrets
from urllib.parse import quote

import requests as req
import gspread
from flask import Flask, request
from google.oauth2.service_account import Credentials as ServiceAccountCredentials
from openai import OpenAI

app = Flask(__name__)

# ===== 環境變數：請在 Render 設定，不要把金鑰寫進 GitHub =====
LINE_CHANNEL_SECRET = os.environ.get("LINE_CHANNEL_SECRET", "")
LINE_CHANNEL_ACCESS_TOKEN = os.environ.get("LINE_CHANNEL_ACCESS_TOKEN", "")
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY", "")
GOOGLE_SHEET_ID = os.environ.get("GOOGLE_SHEET_ID", "")
GOOGLE_CREDENTIALS_JSON = os.environ.get("GOOGLE_CREDENTIALS_JSON", "")
OWNER_LINE_USER_ID = os.environ.get("OWNER_LINE_USER_ID", "").strip()
DATABASE_URL = os.environ.get("DATABASE_URL", "")
GOOGLE_SCOPES = [
    "https://www.googleapis.com/auth/drive.file",
    "https://www.googleapis.com/auth/spreadsheets",
]

client = OpenAI(api_key=OPENAI_API_KEY) if OPENAI_API_KEY else None
TW = datetime.timezone(datetime.timedelta(hours=8))

# ===== 大項目與小項目 =====
# 支出與刷卡使用相同的小項目；繳卡費屬於實際支出。
EXPENSE_CATEGORIES = [
    "餐飲", "交通", "生活", "醫療健康", "美容保養", "服飾",
    "訂閱服務", "娛樂休閒", "運動健身", "旅遊", "教育", "保險",
    "稅費", "捐款公益", "其他", "繳卡費", "繳貸款"
]
CARD_CATEGORIES = EXPENSE_CATEGORIES.copy()
INCOME_CATEGORIES = [
    "薪資", "獎金", "投資收入", "利息", "退款", "紅包",
    "補助", "兼職收入", "其他"
]
MAIN_TYPES = ["支出", "收入", "刷卡"]

HEADERS = ["日期", "時間", "傳送者", "收支", "項目", "內容", "金額", "備註"]


def now_tw():
    return datetime.datetime.now(TW)


class DBConnection:
    """Small compatibility wrapper so the existing app can use PostgreSQL."""
    def __init__(self):
        if not DATABASE_URL:
            raise RuntimeError("Render 尚未設定 DATABASE_URL（PostgreSQL 連線網址）")
        self.conn = psycopg.connect(DATABASE_URL, row_factory=dict_row, connect_timeout=15)

    def execute(self, sql, params=()):
        # Existing app uses SQLite-style ? placeholders; convert them for psycopg.
        return self.conn.execute(sql.replace("?", "%s"), params)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        try:
            if exc_type is None:
                self.conn.commit()
            else:
                self.conn.rollback()
        finally:
            self.conn.close()
        return False


def db_connect():
    return DBConnection()


def init_db():
    with db_connect() as conn:
        conn.execute("""CREATE TABLE IF NOT EXISTS user_sheets (
            line_user_id TEXT PRIMARY KEY,
            display_name TEXT NOT NULL,
            worksheet_title TEXT NOT NULL,
            created_at TEXT NOT NULL
        )""")
        conn.execute("""CREATE TABLE IF NOT EXISTS oauth_sessions (
            state TEXT PRIMARY KEY,
            line_user_id TEXT NOT NULL,
            created_at INTEGER NOT NULL
        )""")
        conn.execute("""CREATE TABLE IF NOT EXISTS link_tokens (
            token TEXT PRIMARY KEY,
            line_user_id TEXT NOT NULL,
            expires_at INTEGER NOT NULL,
            used INTEGER NOT NULL DEFAULT 0
        )""")
        conn.execute("""CREATE TABLE IF NOT EXISTS user_settings (
            line_user_id TEXT PRIMARY KEY,
            wallet_balance INTEGER NOT NULL DEFAULT 0,
            account_balance INTEGER NOT NULL DEFAULT 0,
            statement_day INTEGER NOT NULL DEFAULT 15,
            wallet_initialized INTEGER NOT NULL DEFAULT 0,
            account_initialized INTEGER NOT NULL DEFAULT 0
        )""")
        conn.execute("ALTER TABLE user_settings ADD COLUMN IF NOT EXISTS wallet_initialized INTEGER NOT NULL DEFAULT 0")
        conn.execute("ALTER TABLE user_settings ADD COLUMN IF NOT EXISTS account_initialized INTEGER NOT NULL DEFAULT 0")
        conn.execute("""CREATE TABLE IF NOT EXISTS transaction_meta (
            line_user_id TEXT NOT NULL,
            sheet_row INTEGER NOT NULL,
            payment_method TEXT NOT NULL DEFAULT 'bank',
            PRIMARY KEY(line_user_id, sheet_row)
        )""")


def service_account_credentials():
    """Use the owner's Google service account to access one shared spreadsheet."""
    if not GOOGLE_CREDENTIALS_JSON:
        raise RuntimeError("Render 尚未設定 GOOGLE_CREDENTIALS_JSON（Google 服務帳戶 JSON）")
    try:
        info = json.loads(GOOGLE_CREDENTIALS_JSON)
    except json.JSONDecodeError as exc:
        raise RuntimeError("GOOGLE_CREDENTIALS_JSON 不是有效的 JSON") from exc
    return ServiceAccountCredentials.from_service_account_info(
        info,
        scopes=["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    )


def get_user_row(line_user_id):
    with db_connect() as conn:
        return conn.execute("SELECT * FROM user_sheets WHERE line_user_id=?", (line_user_id,)).fetchone()


def user_is_linked(line_user_id):
    # A user is ready when the shared spreadsheet and service account are configured.
    return bool(GOOGLE_SHEET_ID and GOOGLE_CREDENTIALS_JSON and line_user_id)


def safe_worksheet_title(name):
    name = re.sub(r"[\[\]:*?/\\]", "", (name or "LINE 使用者")).strip()
    name = name[:90] or "LINE 使用者"
    return name


def get_sheet(line_user_id):
    """Open the shared spreadsheet and only this LINE user's worksheet."""
    if not GOOGLE_SHEET_ID:
        raise RuntimeError("Render 尚未設定 GOOGLE_SHEET_ID（共用試算表 ID）")
    gc = gspread.authorize(service_account_credentials())
    sh = gc.open_by_key(GOOGLE_SHEET_ID)
    row = get_user_row(line_user_id)
    if not row:
        raise RuntimeError("尚未建立個人工作表，請重新傳送一次指令。")
    title = row["worksheet_title"]
    try:
        ws = sh.worksheet(title)
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title=title, rows="3000", cols="8")
        ws.update("A1", [HEADERS])
    values = ws.get_all_values()
    if not values:
        ws.update("A1", [HEADERS])
    elif values[0] != HEADERS:
        raise RuntimeError(f"工作表「{title}」欄位與程式不一致，請先備份後再處理。")
    return ws


def ensure_user_sheet(line_user_id, display_name):
    """Map stable LINE user ID to a worksheet named after their LINE display name."""
    display_name = (display_name or "LINE 使用者").strip()[:80] or "LINE 使用者"
    existing = get_user_row(line_user_id)
    if existing:
        if existing["display_name"] != display_name:
            # Keep worksheet identity stable even if display name changes; update label only.
            with db_connect() as conn:
                conn.execute("UPDATE user_sheets SET display_name=? WHERE line_user_id=?", (display_name, line_user_id))
        return existing["worksheet_title"]
    gc = gspread.authorize(service_account_credentials())
    sh = gc.open_by_key(GOOGLE_SHEET_ID)
    base = safe_worksheet_title(display_name)
    title = base
    suffix = 2
    existing_titles = {w.title for w in sh.worksheets()}
    while title in existing_titles:
        title = safe_worksheet_title(f"{base[:85]} ({suffix})")
        suffix += 1
    with db_connect() as conn:
        conn.execute("INSERT INTO user_sheets(line_user_id,display_name,worksheet_title,created_at) VALUES(?,?,?,?)",
                     (line_user_id, display_name, title, now_tw().isoformat()))
    ws = sh.add_worksheet(title=title, rows="3000", cols="8")
    ws.update("A1", [HEADERS])
    return title


def parse_date_hint(text):
    """先用明確規則辨識今天、昨天、前天及常見日期格式。"""
    today = now_tw().date()
    cleaned = text

    if "大前天" in cleaned:
        target = today - datetime.timedelta(days=3)
        cleaned = cleaned.replace("大前天", " ")
    elif "前天" in cleaned:
        target = today - datetime.timedelta(days=2)
        cleaned = cleaned.replace("前天", " ")
    elif "昨天" in cleaned or "昨日" in cleaned:
        target = today - datetime.timedelta(days=1)
        cleaned = cleaned.replace("昨天", " ").replace("昨日", " ")
    elif "今天" in cleaned or "今日" in cleaned:
        target = today
        cleaned = cleaned.replace("今天", " ").replace("今日", " ")
    else:
        target = today

    # 支援 2026/10/15、2026-10-15、2026.10.15
    match = re.search(r"(?<!\d)(20\d{2})[./-](\d{1,2})[./-](\d{1,2})(?!\d)", cleaned)
    if match:
        try:
            target = datetime.date(int(match.group(1)), int(match.group(2)), int(match.group(3)))
            cleaned = cleaned[:match.start()] + " " + cleaned[match.end():]
        except ValueError:
            pass
    else:
        # 支援 10/15（使用今年；若日期看起來是未來日期，則不擅自改成年份）
        match = re.search(r"(?<!\d)(\d{1,2})/(\d{1,2})(?!\d)", cleaned)
        if match:
            try:
                target = datetime.date(today.year, int(match.group(1)), int(match.group(2)))
                cleaned = cleaned[:match.start()] + " " + cleaned[match.end():]
            except ValueError:
                pass

    # Time is metadata, not a category prefix. Remove it even when it follows a date.
    cleaned = re.sub(r"(?<!\d)\d{1,2}:\d{2}(?!\d)", " ", cleaned)
    cleaned = re.sub(r"^\s*(的|那天|當天)+", " ", cleaned)
    cleaned = re.sub(r"\s+", " ", cleaned).strip(" ，,。:：")
    return target, cleaned


def infer_main_type(text):
    """優先辨識使用者明確輸入的大項目；沒寫時再依常見詞彙推斷。"""
    s = text.strip()
    if re.search(r"^(刷卡|信用卡|卡刷)(?:\s|[:：,，|｜-]|$)", s):
        return "刷卡"
    if re.search(r"^(收入|入帳|進帳)(?:\s|[:：,，|｜-]|$)", s):
        return "收入"
    if re.search(r"^(支出|花費|付款)(?:\s|[:：,，|｜-]|$)", s):
        return "支出"

    # 明確收入項目
    if re.search(r"(薪資|薪水|工資|獎金|利息收入|投資收入|兼職收入|收到紅包|收入|退款入帳)", s):
        return "收入"
    # 「繳卡費」是支出：代表實際把款項付出去。
    if "交卡費" in s or "繳卡費" in s or "繳信用卡" in s:
        return "支出"
    # 未指定時，預設支出。
    return "支出"


def clean_main_prefix(text, main_type):
    s = text.strip()
    prefixes = {
        "刷卡": r"^(刷卡|信用卡|卡刷)\s*[:：,，|｜-]?\s*",
        "收入": r"^(收入|入帳|進帳)\s*[:：,，|｜-]?\s*",
        "支出": r"^(支出|花費|付款)\s*[:：,，|｜-]?\s*",
    }
    s = re.sub(prefixes[main_type], "", s)
    return s.strip()


def default_category(item, main_type):
    s = item.lower()
    if main_type == "收入":
        if any(k in s for k in ["薪水", "薪資", "工資", "月薪"]):
            return "薪資"
        if "獎金" in s:
            return "獎金"
        if "紅包" in s:
            return "紅包"
        if "退款" in s or "退費" in s:
            return "退款"
        if "利息" in s:
            return "利息"
        if "投資" in s or "股息" in s or "配息" in s:
            return "投資收入"
        return "其他"

    if "交卡費" in s or "繳卡費" in s or "繳信用卡" in s:
        return "繳卡費"
    if "繳貸款" in s or "貸款還款" in s:
        return "繳貸款"
    if any(k in s for k in ["食材", "食物", "食品", "生鮮", "蔬菜", "水果", "肉類",
                             "海鮮", "零食", "點心", "飲料", "早餐", "午餐", "晚餐",
                             "宵夜", "飯", "麵", "鍋", "滷味", "咖啡", "便當", "餐"]):
        return "餐飲"
    if any(k in s for k in ["規費", "手續費", "辦理費", "地政", "戶政", "登記費", "代辦", "公證", "稅", "罰款"]):
        return "稅費"
    if any(k in s for k in ["停車", "加油", "捷運", "公車", "火車", "高鐵", "計程車", "車資", "交通"]):
        return "交通"
    if any(k in s for k in ["醫院", "診所", "藥", "看醫生", "掛號", "復健"]):
        return "醫療健康"
    if any(k in s for k in ["保險"]):
        return "保險"
    if any(k in s for k in ["訂閱", "會員", "月費", "netflix", "spotify", "claude", "chatgpt"]):
        return "訂閱服務"
    return "其他"


def fallback_parse(text):
    """OpenAI 無法使用時的基本備援：抓最後一個金額，避免完全不能記帳。"""
    # Remove a leading clock time (e.g. 18:05) before classifying the transaction.
    time_cleaned = re.sub(r"^\s*\d{1,2}:\d{2}\s+", "", text)
    target_date, cleaned = parse_date_hint(time_cleaned)
    main_type = infer_main_type(cleaned)
    cleaned = clean_main_prefix(cleaned, main_type)
    match = re.search(r"^(.*?)[\s$＄,，:：|｜-]*(\d[\d,]*)\s*(元|塊|塊錢)?$", cleaned)
    if not match:
        return None
    item = match.group(1).strip(" ，,。:：|｜-")
    amount_text = match.group(2).replace(",", "")
    if not amount_text.isdigit() or not item:
        return None
    amount = int(amount_text)
    if amount < 0:
        return None
    return {
        "date": target_date.isoformat(),
        "main_type": main_type,
        "item": item,
        "amount": amount,
        "category": default_category(item, main_type),
    }


def parse_record(text):
    """用 AI 理解自然語言；日期先由程式規則處理，AI 只協助判斷項目與類別。"""
    # Remove a leading clock time (e.g. 18:05) before classifying the transaction.
    time_cleaned = re.sub(r"^\s*\d{1,2}:\d{2}\s+", "", text)
    target_date, cleaned = parse_date_hint(time_cleaned)
    main_type = infer_main_type(cleaned)
    cleaned = clean_main_prefix(cleaned, main_type)

    if not client:
        return fallback_parse(text)

    categories = INCOME_CATEGORIES if main_type == "收入" else EXPENSE_CATEGORIES
    prompt = f"""
你是繁體中文記帳助手。請把使用者訊息解析成一筆記帳資料。
使用者原始訊息：{text}
移除日期和大項目提示後的文字：{cleaned}
已由程式判定的大項目：{main_type}
記帳日期（已由程式依台灣時間判定）：{target_date.isoformat()}

只能回傳一個 JSON 物件，不要 Markdown 或其他文字，格式：
{{"item":"項目名稱","amount":100,"category":"餐飲"}}

規則：
- item 保留主要項目名稱，不要把日期詞或「支出／收入／刷卡」當成項目名稱。
- amount 是金額整數。忽略金額中的逗號，例如 1,200 是 1200。
- category 必須從這些類別選一個：{", ".join(categories)}
- 支出與刷卡沿用相同小項目。
- 含食材、食物、食品、生鮮、蔬菜、水果、肉類、海鮮、零食、點心、飲料、早餐、午餐、晚餐、宵夜、飯、麵、鍋、滷味、便當、咖啡等詞，歸「餐飲」。
- 含規費、手續費、辦理費、地政、戶政、登記費、代辦、公證、稅、罰款等詞，歸「稅費」。
- 停車、捷運、公車、加油、車資等交通項目，歸「交通」。
- 「繳卡費／繳信用卡」若大項目是支出，類別選「繳卡費」。
- 不要把刷卡消費當成支出；如果訊息開頭明確是「刷卡」，大項目已是刷卡。
- 若沒有金額或不是記帳內容，回傳 {{"item":"","amount":0,"category":"其他"}}。
"""
    try:
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": prompt}],
            max_tokens=250,
            temperature=0
        )
        raw = (response.choices[0].message.content or "").strip()
        raw = raw.replace("```json", "").replace("```", "").strip()
        data = json.loads(raw)
        item = str(data.get("item", "")).strip()
        amount = int(data.get("amount", 0))
        category = str(data.get("category", "其他")).strip()
        allowed = INCOME_CATEGORIES if main_type == "收入" else EXPENSE_CATEGORIES
        if not item or amount <= 0 or category not in allowed:
            return fallback_parse(text)
        return {
            "date": target_date.isoformat(),
            "main_type": main_type,
            "item": item,
            "amount": amount,
            "category": category,
        }
    except Exception as e:
        sys.stdout.write(f"[PARSE] OpenAI 解析失敗，改用備援：{e}\n")
        return fallback_parse(text)


# LINE 使用者名稱快取；試算表顯示 LINE 名稱，不顯示 U 開頭的 userId。
_SENDER_NAME_CACHE = {}

def get_sender_name(user_id, source):
    if not user_id:
        return "未知使用者"
    if user_id in _SENDER_NAME_CACHE:
        return _SENDER_NAME_CACHE[user_id]
    if not LINE_CHANNEL_ACCESS_TOKEN:
        return "未知使用者"

    headers = {"Authorization": f"Bearer {LINE_CHANNEL_ACCESS_TOKEN}"}
    source_type = (source or {}).get("type", "")
    group_id = (source or {}).get("groupId", "")
    room_id = (source or {}).get("roomId", "")
    urls = []
    if source_type == "group" and group_id:
        urls.append(f"https://api.line.me/v2/bot/group/{group_id}/member/{user_id}")
    elif source_type == "room" and room_id:
        urls.append(f"https://api.line.me/v2/bot/room/{room_id}/member/{user_id}")
    urls.append(f"https://api.line.me/v2/bot/profile/{user_id}")

    for url in urls:
        try:
            response = req.get(url, headers=headers, timeout=10)
            if response.status_code == 200:
                display_name = str(response.json().get("displayName", "")).strip()
                if display_name:
                    _SENDER_NAME_CACHE[user_id] = display_name
                    return display_name
        except Exception as e:
            sys.stdout.write(f"[LINE PROFILE] 取得名稱失敗：{e}\n")

    # 若 LINE API 無法取得顯示名稱，不把 Uxxxx ID 寫進表格。
    return "未知使用者"


def get_records(line_user_id):
    ws = get_sheet(line_user_id)
    rows = ws.get_all_records()
    with db_connect() as conn:
        meta_rows = conn.execute("SELECT sheet_row,payment_method FROM transaction_meta WHERE line_user_id=?", (line_user_id,)).fetchall()
    methods = {int(x["sheet_row"]): x["payment_method"] for x in meta_rows}
    result = []
    for sheet_row, row in enumerate(rows, start=2):
        try:
            amount = int(str(row.get("金額", 0)).replace(",", ""))
            date_str = str(row.get("日期", "")).strip()
            main_type = str(row.get("收支", "支出")).strip() or "支出"
            if main_type not in MAIN_TYPES:
                main_type = "支出"
            result.append({"日期": date_str, "時間": str(row.get("時間", "")),
                "傳送者": str(row.get("傳送者", "未知使用者")), "大項目": main_type,
                "項目": str(row.get("內容", "")), "金額": amount,
                "小項目": str(row.get("項目", "其他")) or "其他",
                "備註": str(row.get("備註", "")), "_sheet_row": sheet_row,
                "付款方式": methods.get(sheet_row, "bank")})
        except (ValueError, TypeError):
            continue
    return result


def monthly_total(line_user_id, main_type, today=None):
    today = today or now_tw().date()
    ym = today.strftime("%Y-%m")
    return sum(r["金額"] for r in get_records(line_user_id)
               if r["大項目"] == main_type and r["日期"].startswith(ym))


def cumulative_label(main_type):
    return {"支出": "本月支出累積", "收入": "本月收入累積", "刷卡": "本月刷卡累積"}[main_type]


def save_record(line_user_id, sender, record, payment_method="bank"):
    ws = get_sheet(line_user_id)
    sheet_row = len(ws.get_all_values()) + 1
    ws.append_row([record["date"], now_tw().strftime("%H:%M"), sender, record["main_type"],
                   record["category"], record["item"], record["amount"], ""], value_input_option="USER_ENTERED")
    with db_connect() as conn:
        conn.execute("""INSERT INTO transaction_meta(line_user_id,sheet_row,payment_method) VALUES(?,?,?)
                     ON CONFLICT(line_user_id,sheet_row) DO UPDATE SET payment_method=EXCLUDED.payment_method""",
                     (line_user_id, sheet_row, payment_method))


def add_initial_balance(line_user_id, sender, balance_type, amount):
    """Write each initial balance as a one-time income row; excluded from monthly totals by date label."""
    ws = get_sheet(line_user_id)
    col = "wallet_initialized" if balance_type == "wallet_balance" else "account_initialized"
    with db_connect() as conn:
        conn.execute("INSERT INTO user_settings(line_user_id) VALUES(?) ON CONFLICT(line_user_id) DO NOTHING", (line_user_id,))
        setting = conn.execute(f"SELECT {col} FROM user_settings WHERE line_user_id=?", (line_user_id,)).fetchone()
        if setting and int(setting[col]) == 1:
            return False
        sheet_row = len(ws.get_all_values()) + 1
        label = "初始錢包餘額" if balance_type == "wallet_balance" else "初始帳戶餘額"
        ws.append_row(["初始設定", now_tw().strftime("%H:%M"), sender, "收入", "其他", label, amount, "初始餘額"], value_input_option="USER_ENTERED")
        conn.execute("UPDATE user_settings SET " + col + "=1 WHERE line_user_id=?", (line_user_id,))
        conn.execute("INSERT INTO transaction_meta(line_user_id,sheet_row,payment_method) VALUES(?,?,?) ON CONFLICT(line_user_id,sheet_row) DO UPDATE SET payment_method=EXCLUDED.payment_method",
                     (line_user_id, sheet_row, "initial"))
    return True


def query_stats(line_user_id, period):
    """今日／本月明細：列出每筆明細、分類加總，以及三大項目合計。"""
    try:
        records = get_records(line_user_id)
    except Exception as e:
        return f"📭 無法連接你的試算表：{str(e)[:180]}"
    today = now_tw().date()
    if period == "today":
        filtered = [r for r in records if r["日期"] == today.isoformat()]
        title, empty_text = f"📅 {today.strftime('%Y/%m/%d')} 今日記帳總覽", "今天還沒有記帳資料。"
    else:
        ym = today.strftime("%Y-%m")
        filtered = [r for r in records if r["日期"].startswith(ym)]
        title, empty_text = f"📊 {today.strftime('%Y年%m月')} 本月記帳總覽", "本月還沒有記帳資料。"
    if not filtered:
        return f"{title}\n\n{empty_text}\n\n💸 支出：$0\n💵 收入：$0\n💳 刷卡：$0"
    icons = {"支出": "💸", "收入": "💵", "刷卡": "💳"}
    lines = [title, ""]
    for main_type in MAIN_TYPES:
        type_rows = [r for r in filtered if r["大項目"] == main_type]
        total = sum(r["金額"] for r in type_rows)
        lines.append(f"{icons[main_type]} {main_type}明細")
        if not type_rows:
            lines.extend(["  （沒有資料）", "  合計：$0", ""])
            continue
        type_rows.sort(key=lambda r: (r["日期"], r["時間"], r["項目"]))
        for r in type_rows:
            date_label = r["日期"][5:] if len(r["日期"]) >= 10 else r["日期"]
            lines.append(f"  • {date_label} {r['項目']} ${r['金額']:,}（{r['小項目'] or '其他'}）")
        category_totals = {}
        for r in type_rows:
            cat = r["小項目"] or "其他"
            category_totals[cat] = category_totals.get(cat, 0) + r["金額"]
        lines.append("  分類小計：")
        for cat, amount in sorted(category_totals.items(), key=lambda x: (-x[1], x[0])):
            lines.append(f"    └ {cat}：${amount:,}")
        lines.extend([f"  **{main_type}合計：${total:,}**", ""])
    return "\n".join(lines)


def delete_latest_record(line_user_id):
    ws = get_sheet(line_user_id)
    rows = ws.get_all_records()
    if not rows:
        return "目前找不到你新增的記帳資料。"
    # Delete only a transaction created by this user; skip initial balance rows.
    for idx in range(len(rows)-1, -1, -1):
        row = rows[idx]
        if str(row.get("日期", "")) == "初始設定" or str(row.get("備註", "")) == "初始餘額":
            continue
        sheet_row_number = idx + 2
        item = str(row.get("內容", "未命名項目"))
        amount = str(row.get("金額", "0"))
        main_type = str(row.get("收支", "支出"))
        ws.delete_rows(sheet_row_number)
        with db_connect() as conn:
            conn.execute("DELETE FROM transaction_meta WHERE line_user_id=? AND sheet_row=?", (line_user_id, sheet_row_number))
            conn.execute("UPDATE transaction_meta SET sheet_row=sheet_row-1 WHERE line_user_id=? AND sheet_row>?", (line_user_id, sheet_row_number))
        return f"🗑️ 已刪除上一筆：{main_type}｜{item} ${amount}"
    return "目前找不到可刪除的記帳資料。"


def get_settings(line_user_id):
    with db_connect() as conn:
        conn.execute("INSERT INTO user_settings(line_user_id) VALUES(?) ON CONFLICT(line_user_id) DO NOTHING", (line_user_id,))
        row = conn.execute("SELECT * FROM user_settings WHERE line_user_id=?", (line_user_id,)).fetchone()
        return dict(row)


def set_setting(line_user_id, field, value):
    allowed = {"wallet_balance", "account_balance", "statement_day"}
    if field not in allowed:
        raise ValueError("不支援的設定")
    with db_connect() as conn:
        conn.execute("INSERT INTO user_settings(line_user_id) VALUES(?) ON CONFLICT(line_user_id) DO NOTHING", (line_user_id,))
        conn.execute(f"UPDATE user_settings SET {field}=? WHERE line_user_id=?", (value, line_user_id))


def cycle_start(today, statement_day):
    import calendar
    this_month_last = calendar.monthrange(today.year, today.month)[1]
    statement_day = min(max(1, statement_day), this_month_last)
    if today.day > statement_day:
        start_day = min(statement_day + 1, this_month_last)
        return datetime.date(today.year, today.month, start_day)
    # 上個月結算日隔天為本期起日
    if today.month == 1:
        year, month = today.year - 1, 12
    else:
        year, month = today.year, today.month - 1
    last = calendar.monthrange(year, month)[1]
    prev_statement = min(statement_day, last)
    if prev_statement >= last:
        # 上個月的結算日已是該月最後一天，本期從本月 1 日開始。
        return datetime.date(today.year, today.month, 1)
    return datetime.date(year, month, prev_statement + 1)


def query_people_stats(line_user_id):
    try:
        records = get_records(line_user_id)
        settings = get_settings(line_user_id)
    except Exception as e:
        return f"📭 無法連接你的試算表：{str(e)[:180]}"
    total_income = sum(r["金額"] for r in records if r["大項目"] == "收入")
    total_expense = sum(r["金額"] for r in records if r["大項目"] == "支出")
    today = now_tw().date()
    start = cycle_start(today, int(settings["statement_day"]))
    card_total = sum(r["金額"] for r in records if r["大項目"] == "刷卡"
                     and len(r["日期"]) >= 10 and start.isoformat() <= r["日期"] <= today.isoformat())
    balance = total_income - total_expense
    return (f"📊 個人財務統計\n\n"
            f"💵 累積收入：${total_income:,}\n"
            f"💸 累積支出：${total_expense:,}\n"
            f"💰 餘額：${balance:,}\n"
            f"💳 本期已刷：${card_total:,}\n"
            f"📅 信用卡結算日：每月 {int(settings['statement_day'])} 日")


def flex_button(label, action):
    return {"type": "button", "style": "primary", "color": "#2E8B78", "action": action, "height": "sm"}


def reply_messages(reply_token, messages):
    if not reply_token or not LINE_CHANNEL_ACCESS_TOKEN:
        sys.stdout.write("[LINE] 缺少 replyToken 或 LINE_CHANNEL_ACCESS_TOKEN，無法回覆\n")
        return
    try:
        response = req.post(
            "https://api.line.me/v2/bot/message/reply",
            headers={"Content-Type": "application/json", "Authorization": f"Bearer {LINE_CHANNEL_ACCESS_TOKEN}"},
            json={"replyToken": reply_token, "messages": messages}, timeout=15
        )
        if response.status_code >= 400:
            sys.stdout.write(f"[LINE] 回覆失敗 {response.status_code}: {response.text}\n")
    except Exception as e:
        sys.stdout.write(f"[LINE] 回覆錯誤：{e}\n")


def reply_message(reply_token, text):
    reply_messages(reply_token, [{"type": "text", "text": str(text)[:4900]}])


def link_google_flex_message(link_url):
    return {"type": "flex", "altText": "連結你的 Google 帳號", "contents": {
        "type": "bubble", "body": {"type": "box", "layout": "vertical", "spacing": "md", "contents": [
            {"type": "text", "text": "連結 Google 帳號", "weight": "bold", "size": "lg"},
            {"type": "text", "text": "點擊下方按鈕，授權自己的 Google 帳號，建立只屬於你的記帳試算表。", "wrap": True, "size": "sm"}
        ]},
        "footer": {"type": "box", "layout": "vertical", "contents": [
            flex_button("連結 Google 帳號", {"type": "uri", "label": "連結 Google 帳號", "uri": link_url})
        ]}
    }}


WELCOME_MESSAGE = """🎉 歡迎使用記帳小工具！

先設定：
• 錢包餘額 1500
• 帳戶餘額 25000
• 信用卡結算日 15

記帳範例：
• 午餐 100
• 刷卡 買衣服 890
• 收入 薪水 28000
• 繳貸款 5704
• 繳卡費 5000

查詢：今日、本月、統計、帳本
補記：昨天 午餐 100 或 2026/10/16 18:05 刷卡 寶雅 495
刪除：刪除
只有記帳指令會回覆，一般聊天不會插話。"""

HELP_MESSAGE = WELCOME_MESSAGE


def welcome_flex():
    return {"type": "text", "text": WELCOME_MESSAGE}


def handle_event(event):
    event_type = event.get("type")
    reply_token = event.get("replyToken", "")
    source = event.get("source", {})
    line_user_id = source.get("userId", "")

    if event_type == "join":
        reply_messages(reply_token, [welcome_flex()])
        return
    if event_type != "message":
        return
    msg = event.get("message", {})
    if msg.get("type") != "text" or not line_user_id:
        return
    text = msg.get("text", "").strip()
    sender = get_sender_name(line_user_id, source)
    sys.stdout.write(f"[MSG] {text!r} from {sender} ({line_user_id})\n")

    # Only the configured owner can trigger the reminder.
    if text == "我的LINE ID":
        reply_message(reply_token, f"你的 LINE 使用者 ID：\n{line_user_id}")
        return

    if text == "提醒":
        if OWNER_LINE_USER_ID and line_user_id == OWNER_LINE_USER_ID:
            reply_message(reply_token, "兄弟！該記帳了")
        return

    if text in ["說明", "/說明", "help", "/help"]:
        reply_message(reply_token, HELP_MESSAGE)
        return

    if not user_is_linked(line_user_id):
        # Only respond to commands that clearly intend to use the bookkeeping bot.
        if text in {"今日", "今天", "本月", "統計", "帳本", "刪除", "刪除上一筆", "刪除上一筆記帳"} or re.match(r"^(錢包餘額|帳戶餘額|信用卡結算日)\s*[:：]?\s*\d+$", text):
            reply_message(reply_token, "⚠️ 記帳機器人尚未完成試算表設定，請檢查管理員的 Google 服務帳戶與 GOOGLE_SHEET_ID 設定。")
        return

    # Create a worksheet per LINE user ID; worksheet label uses LINE display name.
    try:
        ensure_user_sheet(line_user_id, sender)
    except Exception as e:
        sys.stdout.write(f"[SHEETS INIT] {e}\n")
        if text in {"今日", "今天", "本月", "統計", "帳本", "刪除", "刪除上一筆", "刪除上一筆記帳"} or re.search(r"\d", text):
            reply_message(reply_token, f"⚠️ 試算表尚未就緒，請檢查設定。")
        return

    setting_match = re.fullmatch(r"(錢包餘額|帳戶餘額|信用卡結算日)\s*[:：]?\s*(\d+)", text)
    if setting_match:
        setting_name, value_text = setting_match.groups()
        value = int(value_text)
        if setting_name == "信用卡結算日":
            if not 1 <= value <= 31:
                reply_message(reply_token, "信用卡結算日請輸入 1～31 的數字。")
                return
            set_setting(line_user_id, "statement_day", value)
            reply_message(reply_token, f"✅ 信用卡結算日：每月 {value} 日。")
        elif setting_name == "錢包餘額":
            set_setting(line_user_id, "wallet_balance", value)
            try:
                add_initial_balance(line_user_id, sender, "wallet_balance", value)
            except Exception as e:
                sys.stdout.write(f"[INITIAL BALANCE] {e}\n")
                reply_message(reply_token, "⚠️ 錢包餘額設定失敗，請稍後再試。")
                return
            reply_message(reply_token, f"✅ 錢包餘額 ${value:,}。")
        else:
            set_setting(line_user_id, "account_balance", value)
            try:
                add_initial_balance(line_user_id, sender, "account_balance", value)
            except Exception as e:
                sys.stdout.write(f"[INITIAL BALANCE] {e}\n")
                reply_message(reply_token, "⚠️ 帳戶餘額設定失敗，請稍後再試。")
                return
            reply_message(reply_token, f"✅ 帳戶餘額 ${value:,}。")
        return

    commands = {
        "今日": lambda: query_stats(line_user_id, "today"),
        "今天": lambda: query_stats(line_user_id, "today"),
        "/今日": lambda: query_stats(line_user_id, "today"),
        "/today": lambda: query_stats(line_user_id, "today"),
        "本月": lambda: query_stats(line_user_id, "month"),
        "/本月": lambda: query_stats(line_user_id, "month"),
        "/month": lambda: query_stats(line_user_id, "month"),
        "統計": lambda: query_people_stats(line_user_id),
        "/統計": lambda: query_people_stats(line_user_id),
        "/stats": lambda: query_people_stats(line_user_id),
    }
    if text == "帳本":
        try:
            sh_url = f"https://docs.google.com/spreadsheets/d/{GOOGLE_SHEET_ID}/edit"
            reply_message(reply_token, f"📒 你的工作表：{sender}\n{sh_url}")
        except Exception:
            reply_message(reply_token, "⚠️ 暫時無法取得帳本連結。")
        return
    if text in commands:
        reply_message(reply_token, commands[text]())
        return
    if text in ["刪除", "刪除上一筆", "刪除上一筆記帳"]:
        try:
            reply_message(reply_token, delete_latest_record(line_user_id))
        except Exception as e:
            sys.stdout.write(f"[SHEETS] 刪除失敗：{e}\n")
            reply_message(reply_token, "⚠️ 刪除失敗，請稍後再試。")
        return

    payment_method = "bank"
    parse_text = text
    if re.match(r"^現金(?:\s|[:：])", parse_text):
        payment_method = "cash"
        parse_text = re.sub(r"^現金\s*[:：]?\s*", "", parse_text)
    elif re.match(r"^(金融卡|帳戶|銀行帳戶)(?:\s|[:：])", parse_text):
        payment_method = "bank"
        parse_text = re.sub(r"^(金融卡|帳戶|銀行帳戶)\s*[:：]?\s*", "", parse_text)

    # Avoid responding to casual chat: only parse messages that look like a transaction.
    has_amount = bool(re.search(r"\d[\d,]*\s*(元|塊|塊錢)?\s*$", parse_text))
    looks_like_record = has_amount and (bool(re.search(r"^(刷卡|信用卡|卡刷|收入|入帳|進帳|支出|花費|付款|現金|金融卡|帳戶|銀行帳戶)\b", text)) or len(parse_text.split()) >= 2 or bool(re.search(r"(午餐|早餐|晚餐|薪水|薪資|寶雅|買|繳|交通|咖啡|便當|衣服|訂閱|醫療|車資)", parse_text)))
    if not looks_like_record:
        return
    record = parse_record(parse_text)
    if not record:
        reply_message(reply_token, "⚠️ 這筆記帳格式我看不懂，請試試：午餐 100 或 刷卡 寶雅 495。")
        return
    try:
        save_record(line_user_id, sender, record, payment_method)
        total = monthly_total(line_user_id, record["main_type"])
        label = cumulative_label(record["main_type"])
        saved_date = datetime.date.fromisoformat(record["date"])
        date_note = f"\n📅 記錄日期：{saved_date.strftime('%Y/%m/%d')}" if saved_date != now_tw().date() else ""
        response_text = (f"✅ {record['main_type']}｜{record['item']} ${record['amount']:,}"
                         f"（{record['category']}）{date_note}\n📊 {label}：${total:,}")
        reply_message(reply_token, response_text)
        sys.stdout.write(f"[SHEETS] 寫入成功：{record}\n")
    except Exception as e:
        sys.stdout.write(f"[SHEETS] 寫入失敗：{e}\n")
        reply_message(reply_token, "⚠️ 這筆記帳沒有成功寫入，請稍後再試。")


init_db()

@app.route("/auth/start", methods=["GET"])
def auth_start():
    return "Google 個別授權流程已停用；請由管理員設定共用試算表。", 410


@app.route("/callback", methods=["POST"])
def callback():
    body = request.get_data(as_text=True)
    if not LINE_CHANNEL_SECRET:
        sys.stdout.write("[WEBHOOK] 沒設定 LINE_CHANNEL_SECRET，拒收\n")
        return "LINE_CHANNEL_SECRET not set", 500

    expected = base64.b64encode(
        hmac.new(LINE_CHANNEL_SECRET.encode(), body.encode(), hashlib.sha256).digest()
    ).decode()
    received = request.headers.get("X-Line-Signature", "")
    if not hmac.compare_digest(expected, received):
        sys.stdout.write("[WEBHOOK] 簽章不對，拒收\n")
        return "bad signature", 403

    try:
        data = json.loads(body)
    except json.JSONDecodeError:
        return "OK"

    for event in data.get("events", []):
        thread = threading.Thread(target=handle_event, args=(event,), daemon=True)
        thread.start()
    return "OK"


@app.route("/", methods=["GET"])
def health():
    return "✅ 記帳小工具運作中"


if __name__ == "__main__":
    init_db()
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)
