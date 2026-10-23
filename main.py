import sys
sys.stdout.reconfigure(line_buffering=True)

import os
import re
import json
import hmac
import base64
import hashlib
import datetime
import threading
import psycopg
from psycopg.rows import dict_row

import requests as req
import gspread
from flask import Flask, request
from openai import OpenAI

app = Flask(__name__)

# ===== 環境變數：請在 Render 設定，不要把金鑰寫進 GitHub =====
LINE_CHANNEL_SECRET = os.environ.get("LINE_CHANNEL_SECRET", "")
LINE_CHANNEL_ACCESS_TOKEN = os.environ.get("LINE_CHANNEL_ACCESS_TOKEN", "")
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY", "")
GOOGLE_SHEET_ID = os.environ.get("GOOGLE_SHEET_ID", "")  # 舊設定保留；個人分帳以 USER_SPREADSHEET_MAP 為準
USER_SPREADSHEET_MAP_JSON = os.environ.get("USER_SPREADSHEET_MAP", "{}")

# ===== 個人試算表設定：之後要新增使用者／工作表，統一改這個區塊 =====
# spreadsheet_id：該使用者自己的 Google 試算表 ID
# main_worksheet：主要記帳表（支出／收入／刷卡）
# worksheets.stock：股票功能使用的次要工作表
# worksheets.foreign_currency：外幣支出功能使用的次要工作表
# 注意：這裡只放 ID 與工作表名稱；LINE／Google／OpenAI 金鑰仍放 Render 環境變數。
PERSONAL_SHEETS = {
    "U0a81f4b57639d3d599aced33392c506d": {
        "spreadsheet_id": "1-uumCTYnKuPsftwg6Lbriht5Y6vARJDIPnPuDcAmVes",
        "main_worksheet": "帳本",
        "legacy_main_worksheets": ["Bu_506d"],
        "worksheets": {
            "stock": "股票",
            "foreign_currency": "外幣",
        },
    },
    "U1b8bc5162bdf85af9b8bed34cd38abc4": {
        "spreadsheet_id": "1MeJRJ8RpixqJmpvO-0KeJAcmQfOJL62C_WmCIe2-Z7Q",
        "main_worksheet": "帳本",
        "legacy_main_worksheets": ["Syuan_abc4"],
        "worksheets": {
            "stock": "股票",
            "foreign_currency": "外幣",
        },
    },
    # 第三位朋友：主要工作表未指定時，程式會依 LINE 顯示名稱自動建立／取得。
    "U5ff9728b5d981070890d63b610a7b834": {
        "spreadsheet_id": "1JegFHHQfZ5nV3WlL0dRrVAwbVlP8-t9mFFLn2a0WR20",
        "main_worksheet": "帳本",
        "legacy_main_worksheets": [],
        "worksheets": {
            "stock": "股票",
            "foreign_currency": "外幣",
        },
    },
}
GOOGLE_CREDENTIALS_JSON = os.environ.get("GOOGLE_CREDENTIALS_JSON", "")  # Google 服務帳戶 JSON（完整 JSON 文字）
DATABASE_URL = os.environ.get("DATABASE_URL", "")

client = OpenAI(api_key=OPENAI_API_KEY) if OPENAI_API_KEY else None
TW = datetime.timezone(datetime.timedelta(hours=8))

# ============================================================
# ★★★ 小分類編輯區：要新增／刪除「餐飲、交通、生活」等項目，改這裡！★★★
# 不用改程式其他地方：在對應清單的引號中新增名稱，項目之間用逗號隔開；
# 要刪除就把該名稱移除。請保留方括號 [ ]。
#
# 【一般支出】例如：現金／金融卡支付的餐飲、交通、生活等
EXPENSE_CATEGORIES = [
    "餐飲", "交通", "生活", "醫療健康", "美容保養", "服飾",
    "訂閱服務", "娛樂休閒", "運動健身", "旅遊", "教育", "保險",
    "稅費", "捐款公益", "其他", "繳卡費", "繳貸款"
]

# 【刷卡】信用卡消費的小分類；這份清單獨立修改，不會連動一般支出
CARD_CATEGORIES = [
    "餐飲", "交通", "生活", "醫療健康", "美容保養", "服飾",
    "訂閱服務", "娛樂休閒", "運動健身", "旅遊", "教育", "保險",
    "稅費", "捐款公益", "其他", "繳卡費", "繳貸款"
]

# 【收入】例如：薪資、獎金、退款等收入的小分類
INCOME_CATEGORIES = [
    "薪資", "獎金", "投資收入", "利息", "退款", "紅包",
    "補助", "兼職收入", "其他"
]

# 大分類名稱（一般使用者通常不需要修改這行）
MAIN_TYPES = ["支出", "收入", "刷卡"]
# ======================== 小分類編輯區結束 ========================

HEADERS = ["日期", "時間", "記錄人", "收支", "項目", "內容", "金額", "備註"]
STOCK_HEADERS = ["日期", "股票代號", "買賣", "張/股數", "成交單價", "成交成本", "備註"]
FOREIGN_HEADERS = ["日期", "時間", "記錄人", "收支", "項目", "內容", "幣別", "金額", "備註"]


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
        conn.execute("""CREATE TABLE IF NOT EXISTS user_settings (
            line_user_id TEXT PRIMARY KEY,
            wallet_balance INTEGER NOT NULL DEFAULT 0,
            account_balance INTEGER NOT NULL DEFAULT 0,
            statement_day INTEGER NOT NULL DEFAULT 15,
            currency_mode TEXT NOT NULL DEFAULT 'TWD'
        )""")
        conn.execute("""CREATE TABLE IF NOT EXISTS transaction_meta (
            line_user_id TEXT NOT NULL,
            sheet_row INTEGER NOT NULL,
            payment_method TEXT NOT NULL DEFAULT 'bank',
            PRIMARY KEY(line_user_id, sheet_row)
        )""")
        conn.execute("""CREATE TABLE IF NOT EXISTS user_profiles (
            line_user_id TEXT PRIMARY KEY,
            display_name TEXT NOT NULL,
            worksheet_title TEXT NOT NULL,
            created_at TEXT NOT NULL
        )""")
        # 舊資料庫相容：使用 PostgreSQL 原生 IF NOT EXISTS，避免重複欄位錯誤
        # 造成交易進入 aborted 狀態，後續 SQL 全部失敗。
        conn.execute("ALTER TABLE user_settings ADD COLUMN IF NOT EXISTS wallet_initial_recorded INTEGER NOT NULL DEFAULT 0")
        conn.execute("ALTER TABLE user_settings ADD COLUMN IF NOT EXISTS account_initial_recorded INTEGER NOT NULL DEFAULT 0")
        conn.execute("ALTER TABLE user_settings ADD COLUMN IF NOT EXISTS currency_mode TEXT NOT NULL DEFAULT 'TWD'")




def get_personal_sheet_config(line_user_id):
    """取得 LINE 使用者的個人試算表設定；環境變數設定可額外覆蓋／新增。"""
    if not line_user_id:
        return {}
    mapping = dict(PERSONAL_SHEETS)
    try:
        env_mapping = json.loads(USER_SPREADSHEET_MAP_JSON or "{}")
    except json.JSONDecodeError as e:
        raise RuntimeError("Render 的 USER_SPREADSHEET_MAP 不是有效 JSON") from e
    if not isinstance(env_mapping, dict):
        raise RuntimeError("USER_SPREADSHEET_MAP 必須是 LINE userId 對應設定的 JSON 物件")
    mapping.update(env_mapping)
    config = mapping.get(line_user_id, {})
    # 相容舊格式：userId: "spreadsheet_id"；這種格式會沿用自動命名工作表。
    if isinstance(config, str):
        config = {"spreadsheet_id": config}
    return config if isinstance(config, dict) else {}


def get_user_spreadsheet_id(line_user_id):
    config = get_personal_sheet_config(line_user_id)
    return str(config.get("spreadsheet_id", "")).strip()


def get_user_worksheet_title(line_user_id):
    config = get_personal_sheet_config(line_user_id)
    # 新設定優先；保留舊版 worksheet_title 相容性。
    return str(config.get("main_worksheet") or config.get("worksheet_title", "")).strip()


def get_user_feature_worksheet_title(line_user_id, feature, default_title):
    """讀取某個功能的次要工作表名稱；未設定時使用安全的預設名稱。"""
    config = get_personal_sheet_config(line_user_id)
    worksheets = config.get("worksheets", {})
    if isinstance(worksheets, dict):
        title = str(worksheets.get(feature, "")).strip()
        if title:
            return title[:100]
    return default_title[:100]

def user_is_linked(line_user_id):
    # 每位 LINE 使用者必須有自己的試算表對應，避免誤寫入別人的帳本。
    return bool(get_user_spreadsheet_id(line_user_id) and GOOGLE_CREDENTIALS_JSON and DATABASE_URL)


def shared_google_client():
    if not GOOGLE_CREDENTIALS_JSON:
        raise RuntimeError("Render 尚未設定 GOOGLE_CREDENTIALS_JSON（Google 服務帳戶 JSON）")
    try:
        info = json.loads(GOOGLE_CREDENTIALS_JSON)
    except json.JSONDecodeError as e:
        raise RuntimeError("GOOGLE_CREDENTIALS_JSON 不是有效的 JSON，請貼上完整服務帳戶 JSON") from e
    # service_account_from_dict 已直接回傳 gspread.Client；不可再傳入 authorize。
    return gspread.service_account_from_dict(info)


def safe_worksheet_title(display_name, user_id):
    name = re.sub(r"[\[\]:*?/\\]", " ", (display_name or "LINE使用者")).strip().strip("'")
    name = re.sub(r"\s+", " ", name)[:70] or "LINE使用者"
    # 同名者用 ID 後四碼區分，避免兩人共用同一張工作表。
    suffix = str(user_id)[-4:]
    return f"{name}_{suffix}"[:100]


def get_or_rename_worksheet(spreadsheet, title, legacy_titles, rows, cols, headers):
    """Use the canonical tab name, renaming a legacy tab in place to preserve its data."""
    try:
        ws = spreadsheet.worksheet(title)
    except gspread.WorksheetNotFound:
        ws = None
        for old_title in legacy_titles or []:
            if not old_title or old_title == title:
                continue
            try:
                ws = spreadsheet.worksheet(old_title)
                ws.update_title(title)
                sys.stdout.write(f"[SHEETS] 已將工作表「{old_title}」改名為「{title}」\n")
                break
            except gspread.WorksheetNotFound:
                continue
        if ws is None:
            ws = spreadsheet.add_worksheet(title=title, rows=str(rows), cols=str(cols))
            ws.update("A1", [headers])
    values = ws.get_all_values()
    if not values:
        ws.update("A1", [headers])
    elif values[0] != headers:
        raise RuntimeError(f"工作表「{title}」欄位與程式不一致，為避免資料遺失未自動覆蓋。")
    return ws


def get_sheet(line_user_id):
    """開啟該 LINE 使用者設定的個人試算表與記帳工作表。"""
    if not line_user_id:
        raise RuntimeError("無法辨識 LINE 使用者 ID")
    spreadsheet_id = get_user_spreadsheet_id(line_user_id)
    if not spreadsheet_id:
        raise RuntimeError("尚未設定你的個人試算表。請管理員在 Render 的 USER_SPREADSHEET_MAP 加入你的 LINE userId 與試算表 ID。")
    gc = shared_google_client()
    sh = gc.open_by_key(spreadsheet_id)

    # 若個人設定指定了既有工作表名稱，就直接使用它；否則沿用自動命名方式。
    with db_connect() as conn:
        profile = conn.execute(
            "SELECT display_name,worksheet_title FROM user_profiles WHERE line_user_id=?",
            (line_user_id,)
        ).fetchone()

    cached_name = _SENDER_NAME_CACHE.get(line_user_id)
    if cached_name:
        display_name = cached_name
    elif profile and profile["display_name"] and profile["display_name"] != "LINE使用者":
        display_name = profile["display_name"]
    else:
        display_name = get_sender_name(line_user_id, {})
    if not display_name:
        display_name = "LINE使用者"

    configured_title = get_user_worksheet_title(line_user_id)
    title = configured_title or safe_worksheet_title(display_name, line_user_id)
    with db_connect() as conn:
        conn.execute(
            """INSERT INTO user_profiles(line_user_id,display_name,worksheet_title,created_at)
               VALUES(?,?,?,?)
               ON CONFLICT(line_user_id) DO UPDATE SET
                 display_name=EXCLUDED.display_name,
                 worksheet_title=EXCLUDED.worksheet_title""",
            (line_user_id, display_name, title, now_tw().isoformat())
        )

    config = get_personal_sheet_config(line_user_id)
    legacy_titles = list(config.get("legacy_main_worksheets", []))
    if profile and profile["worksheet_title"] and profile["worksheet_title"] != title:
        legacy_titles.append(profile["worksheet_title"])
    # 相容第三位使用者的舊版自動命名分頁。
    legacy_titles.extend([safe_worksheet_title(display_name, line_user_id)])
    try:
        ws = sh.worksheet(title)
    except gspread.WorksheetNotFound:
        ws = None
        for old_title in dict.fromkeys(legacy_titles):
            if not old_title or old_title == title:
                continue
            try:
                ws = sh.worksheet(old_title)
                ws.update_title(title)
                sys.stdout.write(f"[SHEETS] 已將工作表「{old_title}」改名為「{title}」\n")
                break
            except gspread.WorksheetNotFound:
                continue
        if ws is None:
            ws = sh.add_worksheet(title=title, rows="2000", cols=str(len(HEADERS)))
            ws.update("A1", [HEADERS])

    values = ws.get_all_values()
    if not values:
        ws.update("A1", [HEADERS])
    elif values[0] != HEADERS:
        if values[0][0:2] == ["日期", "時間"] and "傳送者" in values[0]:
            idx = values[0].index("傳送者")
            ws.update_cell(1, idx + 1, "記錄人")
        else:
            raise RuntimeError(f"工作表「{title}」欄位與程式不一致，請先備份後再處理。")
    return ws



def get_currency_mode(line_user_id):
    """取得使用者目前記帳幣別；每位使用者獨立保存於 PostgreSQL。"""
    with db_connect() as conn:
        row = conn.execute("SELECT currency_mode FROM user_settings WHERE line_user_id=?", (line_user_id,)).fetchone()
    mode = str(row["currency_mode"] if row else "TWD").upper() if row else "TWD"
    return mode if mode in CURRENCY_ALIASES else "TWD"


def set_currency_mode(line_user_id, currency):
    """更新個人目前記帳幣別。"""
    if currency not in CURRENCY_ALIASES:
        currency = "TWD"
    with db_connect() as conn:
        conn.execute("""INSERT INTO user_settings(line_user_id, currency_mode) VALUES(?, ?)
                     ON CONFLICT(line_user_id) DO UPDATE SET currency_mode=EXCLUDED.currency_mode""",
                     (line_user_id, currency))


def currency_command(text):
    """只有單獨輸入幣別名稱（或『切換至人民幣』）才視為模式切換。"""
    raw = re.sub(r"^[\s:：,，]+|[\s:：,，。]+$", "", str(text or "")).strip()
    raw = re.sub(r"^(切換(?:到|至)?|改成|使用|記錄|記帳|外幣(?:支出|記帳)?)[\s:：,，-]*", "", raw, flags=re.I).strip()
    code, alias = detect_currency(raw)
    if not code:
        return None
    # 避免「人民幣奶茶 20」被當成單純切換指令；剩下的文字必須正好等於別名。
    if raw.casefold() in {a.casefold() for a in CURRENCY_ALIASES[code]}:
        return code
    return None


def classify_expense_category(item):
    """先用明確規則；規則無法判斷時請 AI 依消費語意判斷，盡量不落入其他。"""
    category = default_category(item, "支出")
    if category != "其他":
        return category
    if not client:
        return "其他"
    try:
        prompt = f"""你是繁體中文個人記帳分類助手。請只判斷這筆消費最合理的分類，回傳單一 JSON：{{\"category\":\"分類名稱\"}}。
消費描述：{item}
只能從以下選一個：{', '.join(EXPENSE_CATEGORIES)}
判斷原則：
- 先看使用者明確指定的用途；明確用途優先於品牌、角色、藝人名字。
- 飲料、餐點、餐廳、手搖飲、咖啡、便當、零食歸餐飲。
- 衛生用品、衛生棉、衛生紙、日常消耗品、家庭用品歸生活。
- 盲盒、盲抽、扭蛋、抽賞、玩具、玩偶、娃娃、公仔、動漫／卡通角色商品、收藏品、專輯、小卡、偶像／藝人／團體相關消費、演唱會、遊戲娛樂，若沒有明確其他用途，一律歸娛樂休閒。陌生角色名或藝人名不可只因不認識就歸其他；結合「買、抽、專輯、玩具、周邊」等情境判斷。
- 明確店家名稱可依店家類型判斷；超市、百貨、藥妝、五金、生活用品通路預設生活，餐飲與飲料品牌歸餐飲。
- 訂閱、會員、影音／音樂服務、雲端、AI 服務、軟體月費歸訂閱服務。
- 交通、醫療健康、美容保養、服飾、運動健身、旅遊、教育、保險、稅費、捐款公益依用途分類。
- 只有在完全沒有足夠線索時才選其他。"""
        response = client.chat.completions.create(model="gpt-4o-mini", messages=[{"role":"user","content":prompt}], max_tokens=80, temperature=0)
        raw = (response.choices[0].message.content or "").strip().replace("```json", "").replace("```", "").strip()
        data = json.loads(raw)
        result = str(data.get("category", "其他")).strip()
        return result if result in EXPENSE_CATEGORIES else "其他"
    except Exception as e:
        sys.stdout.write(f"[CATEGORY AI] 分類失敗：{e}\n")
        return "其他"


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
    match = re.search(r"(?<!\d)(20\d{2})[/.\-](\d{1,2})[/.\-](\d{1,2})(?!\d)", cleaned)
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

    # 日期旁的時間只作為時間提示，不得卡在「刷卡／收入」前面影響大項目判斷。
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
    """以固定關鍵字優先分類，減少常見項目呼叫 AI 後仍被歸到其他。"""
    s = item.lower().replace("＋", "+")
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
        if any(k in s for k in ["投資", "股息", "配息"]):
            return "投資收入"
        if any(k in s for k in ["補助", "津貼"]):
            return "補助"
        if any(k in s for k in ["兼職", "接案", "外快"]):
            return "兼職收入"
        return "其他"

    if any(k in s for k in ["交卡費", "繳卡費", "繳信用卡"]):
        return "繳卡費"
    if any(k in s for k in ["繳貸款", "貸款還款"]):
        return "繳貸款"

    # 明確寫出生活用途時，優先於角色／品牌名稱分類。
    if any(k in s for k in ["生活用品", "日用品", "家用品", "居家用品", "衛生棉", "好自在", "夜用", "護墊", "衛生紙", "面紙", "清潔用品"]):
        return "生活"

    # 常見連鎖餐飲／飲料品牌：即使只輸入店名，也直接歸「餐飲」。
    # 先判斷餐飲品牌，避免店名被後面的生活用品店規則搶先分類。
    dining_brands = [
        # 速食、漢堡、炸雞、披薩與常見連鎖餐廳
        "麥當勞", "mcdonald", "肯德基", "kfc", "摩斯漢堡", "mos burger", "漢堡王", "burger king",
        "頂呱呱", "拿坡里", "必勝客", "pizza hut", "達美樂", "domino's", "dominos",
        "subway", "賽百味", "四海遊龍", "八方雲集", "三商巧福", "鬍鬚張", "梁社漢", "悟饕",
        "爭鮮", "壽司郎", "藏壽司", "くら寿司", "築間", "肉多多", "石二鍋", "涮乃葉",
        "錢都", "大戶屋", "定食8", "定食８", "春水堂", "鼎泰豐", "添好運", "瓦城",
        "大心", "路易莎", "louisa", "星巴克", "starbucks", " cama", " cama cafe", " cama café",
        "85度c", "85度Ｃ", " cama", " cama cafe", " cama café", " cama",
        # 台灣常見手搖飲、茶飲與咖啡連鎖
        "可不可", "可不可熟成茶行", "50嵐", "五十嵐", "清心福全", "清心", "大苑子", "麻古茶坊", "麻古",
        "迷客夏", "coco都可", "co co都可", "coco", "茶之魔手", "茶の魔手", "鮮茶道", "得正",
        "一沐日", "龜記", "龜記茗品", "萬波", "五桐號", "八曜和茶", "烏弄", "水巷茶弄",
        "茶湯會", "喫茶趣", "先喝道", "大茗", "大明", "大苑子", "珍煮丹", "老賴茶棧",
        "迷客夏", "再睡5分鐘", "再睡五分鐘", "日出茶太", "茶太", "comebuy", "貢茶", "春陽茶事",
        "一手私藏", "得正", "上宇林", "叮哥", "茶聚", "歇腳亭", "ug樂己", "樂己", "milksha",
        "teas原味", "tea's原味", "teas 原味", "tea's 原味", "幸福堂", "老虎堂", "鶴茶樓", "康青龍",
        "星巴克", "路易莎", " cama", " cama cafe", " cama café", " cama",
        # 直接描述餐飲店型的詞
        "素食店", "素食餐廳", "自助餐店", "便當店", "早餐店", "飲料店", "手搖飲", "咖啡店", "餐廳"
    ]
    if any(k.strip() in s for k in dining_brands if k.strip()):
        return "餐飲"

    # 常見生活百貨、超市、藥妝與五金通路：只輸入店名也先歸「生活」。
    lifestyle_stores = [
        "全聯", "大全聯", "px mart", "大潤發", "愛買", "a.mart", "amart", "家樂福", "carrefour",
        "寶雅", "poya", "小北百貨", "小北", "振宇五金", "特力屋", "h&d", "生活工場",
        "大創", "daiso", "無印良品", "muji", "宜得利", "nitori", "美廉社", "simple mart",
        "全家便利商店", "全家", "7-eleven", "7-11", "7－11", "統一超商", "萊爾富", "hi-life",
        "ok超商", "okmart", "ok mart", "康是美", "cosmed", "屈臣氏", "watsons", "丁丁藥局",
        "大樹藥局", "寶家", "特力和樂", "hola", "宜家家居", "ikea", "五金行", "五金百貨"
    ]
    if any(k in s for k in lifestyle_stores):
        return "生活"

    if any(k in s for k in ["食材", "食物", "食品", "生鮮", "蔬菜", "水果", "肉類",
                             "海鮮", "零食", "點心", "飲料", "早餐", "午餐", "晚餐",
                             "宵夜", "飯", "麵", "鍋", "滷味", "咖啡", "便當", "珍珠奶茶", "奶茶", "烏龍茶", "茶飲", "手搖", "餐"]):
        return "餐飲"
    if any(k in s for k in ["規費", "手續費", "辦理費", "地政", "戶政", "登記費", "代辦", "公證", "稅", "罰款"]):
        return "稅費"
    if any(k in s for k in ["悠遊卡", "一卡通", "icash", "停車", "加油", "捷運", "公車", "火車", "高鐵", "計程車", "車資", "交通"]):
        return "交通"
    if any(k in s for k in ["醫院", "診所", "藥", "看醫生", "掛號", "復健"]):
        return "醫療健康"
    if "保險" in s:
        return "保險"
    if any(k in s for k in ["保養品", "化妝品", "化妝水", "乳液", "口紅", "美容", "美甲"]):
        return "美容保養"
    if any(k in s for k in ["衣服", "褲子", "裙子", "鞋子", "襪子", "包包", "帽子", "服飾"]):
        return "服飾"
    if any(k in s for k in ["健身房", "健身", "瑜伽", "瑜珈", "運動課", "運動用品"]):
        return "運動健身"
    if any(k in s for k in ["機票", "住宿", "飯店", "旅館", "旅行", "旅遊", "民宿"]):
        return "旅遊"
    if any(k in s for k in ["課程", "學費", "教材", "補習", "教科書", "線上課"]):
        return "教育"

    # 訂閱服務：串流影音、雲端空間、AI 會員、軟體與網站主機服務。
    if any(k in s for k in [
        "訂閱", "會員", "月費", "年費", "續費", "方案費", "premium", "plus方案", "go方案",
        "netflix", "disney+", "disney plus", "spotify", "apple icloud", "icloud", "icloud+",
        "youtube premium", "youtube music", "愛奇藝", "奇藝", "優酷", "youku", "騰訊視頻",
        "騰訊视频", "騰訊會員", "芒果tv", "芒果tv", "芒果會員", "抖音會員",
        "chatgpt", "gpt go", "gptgo", "openai", "claude", "gemini advanced", "copilot pro",
        "render", "cloudflare", "vercel", "heroku", "hosting", "hosting fee", "subscription",
        "雲端空間", "雲端容量", "儲存空間升級", "軟體訂閱", "軟體會員", "影音會員", "串流會員"
    ]):
        return "訂閱服務"

    # 生活：使用者說「工具」時，預設指實體小工具與生活耗材，不是軟體。
    if any(k in s for k in [
        "工具", "刷子", "螺絲", "螺絲釘", "螺絲起子", "螺絲批", "扳手", "鉗子", "槌子", "錘子",
        "尺", "剪刀", "美工刀", "膠帶", "膠水", "電池", "延長線", "充電線", "插頭", "插座",
        "燈泡", "收納盒", "收納袋", "收納用品", "日用品", "生活用品", "家用品", "清潔用品",
        "衛生紙", "面紙", "抽取式衛生紙", "衛生棉", "好自在", "護墊", "垃圾袋", "洗衣精", "洗衣液", "洗碗精", "抹布",
        "曬衣架", "衣架", "海綿", "菜瓜布", "拖把", "掃把", "垃圾桶", "保鮮膜", "鋁箔紙",
        "夾鏈袋", "紙巾", "濕紙巾", "棉花棒", "牙籤", "牙刷", "牙膏", "洗手乳", "洗手液"
    ]):
        return "生活"

    # 娛樂休閒：追星、收藏、玩偶玩具、門票與遊戲消費。
    if any(k in s for k in [
        "盲盒", "盲抽", "抽盲盒", "扭蛋", "抽賞", "一番賞", "玩偶", "玩具", "娃娃", "公仔", "模型", "絨毛", "毛絨", "布偶", "抱枕玩偶", "烏薩奇",
        "周邊", "周邊商品", "應援物", "專輯", "唱片", "小卡", "卡冊", "寫真", "追星",
        "演唱會", "門票", "音樂祭", "粉絲見面會", "見面會", "電影票", "電影", "戲票",
        "展覽票", "遊樂園", "桌遊", "電玩", "遊戲點數", "遊戲儲值", "直播打賞", "抖幣", "抖币"
    ]):
        return "娛樂休閒"
    return "其他"


def fallback_parse(text):
    """OpenAI 無法使用時的基本備援：抓最後一個金額，避免完全不能記帳。"""
    target_date, cleaned = parse_date_hint(text)
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
    target_date, cleaned = parse_date_hint(text)
    main_type = infer_main_type(cleaned)
    cleaned = clean_main_prefix(cleaned, main_type)

    if not client:
        return fallback_parse(text)

    categories = INCOME_CATEGORIES if main_type == "收入" else (CARD_CATEGORIES if main_type == "刷卡" else EXPENSE_CATEGORIES)
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
- 悠遊卡（即使只輸入「悠遊卡」，也視為悠遊卡儲值／交通支出）、一卡通、icash、停車、捷運、公車、加油、車資等交通項目，歸「交通」。
- 娛樂休閒包含追星、收藏及現場活動：盲盒、盲抽、扭蛋、抽賞、玩偶、玩具、娃娃、公仔、模型、周邊、應援物、專輯、唱片、小卡、卡冊、寫真、演唱會、門票、粉絲見面會、電影票、展覽票、遊樂園、桌遊、電玩、遊戲儲值、直播打賞等，統一歸「娛樂休閒」，不要歸「其他」。卡通／動漫角色名稱、玩具系列名稱、偶像／藝人／團體名稱，若是在購物記帳情境且沒有明確指定其他用途，優先判為娛樂休閒；不要要求使用者一定要寫出「周邊」或「專輯」。如果使用者明確寫出「生活用品、衛生用品」等實際用途，則用途優先。
- 訂閱服務包含國內外影音／音樂會員與數位服務：Netflix、Disney+、Apple iCloud／iCloud+、YouTube Premium、Spotify、愛奇藝、優酷、騰訊視頻、芒果TV、ChatGPT Plus／Go、OpenAI、Claude、Render、Cloudflare、Vercel 等；雲端空間升級、會員、月費、年費、軟體訂閱、網站部署／主機服務都歸「訂閱服務」。不要因品牌來自哪個國家而改分類。
- 生活包含一般實體工具與日常耗材：工具、刷子、螺絲／螺絲釘、電池、剪刀、膠帶、插頭、燈泡、面紙、衛生紙、垃圾袋、清潔用品、收納用品、牙刷、牙膏等，統一歸「生活」。使用者只說「工具」時，預設是實體工具，不要當成軟體工具。
- 若使用者明確說出軟體／平台名稱，依其品牌關鍵字判斷，不要只因有「工具」二字就歸生活。
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
        allowed = INCOME_CATEGORIES if main_type == "收入" else (CARD_CATEGORIES if main_type == "刷卡" else EXPENSE_CATEGORIES)
        if not item or amount <= 0:
            return fallback_parse(text)
        # 明確關鍵字分類優先於 AI 判斷，避免訂閱服務、生活用品與娛樂收藏被誤判為「其他」。
        if main_type != "收入":
            forced_category = default_category(item, main_type)
            # default_category 是唯一的明確關鍵字規則來源，避免兩份清單不同步。
            if forced_category != "其他":
                category = forced_category
        if category not in allowed:
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
                    try:
                        with db_connect() as conn:
                            conn.execute("INSERT INTO user_profiles(line_user_id,display_name,worksheet_title,created_at) VALUES(?,?,?,?) ON CONFLICT(line_user_id) DO UPDATE SET display_name=EXCLUDED.display_name",
                                         (user_id, display_name, safe_worksheet_title(display_name, user_id), now_tw().isoformat()))
                    except Exception as db_error:
                        sys.stdout.write(f"[PROFILE DB] 更新名稱失敗：{db_error}\n")
                    return display_name
        except Exception as e:
            sys.stdout.write(f"[LINE PROFILE] 取得名稱失敗：{e}\n")

    # API 暫時取不到名稱時先用中性名稱，之後再次成功取得 Profile 時會更新。
    return "LINE使用者"


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
            result.append({
                "日期": date_str,
                "時間": str(row.get("時間", "")),
                "記錄人": str(row.get("記錄人", row.get("傳送者", "未知使用者"))),
                "大項目": main_type,
                "項目": str(row.get("內容", "")),
                "金額": amount,
                "小項目": str(row.get("項目", "其他")) or "其他",
                "備註": str(row.get("備註", "")),
                "_sheet_row": sheet_row,
                "付款方式": methods.get(sheet_row, "bank"),
            })
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
    ws.append_row([
        record["date"], now_tw().strftime("%H:%M"), sender, record["main_type"],
        record["category"], record["item"], record["amount"], ""
    ], value_input_option="USER_ENTERED")
    with db_connect() as conn:
        conn.execute("""INSERT INTO transaction_meta(line_user_id,sheet_row,payment_method) VALUES(?,?,?)
                     ON CONFLICT(line_user_id,sheet_row) DO UPDATE SET payment_method=EXCLUDED.payment_method""",
                     (line_user_id, sheet_row, payment_method))


# ===== 外幣支出：只記錄原幣，不換算新台幣 =====
CURRENCY_ALIASES = {
    "USD": ["美元", "美金", "美國幣", "USD", "US dollar", "US dollars", "dollar", "dollars"],
    "CNY": ["人民幣", "人民币", "RMB", "CNY", "中國幣", "人仔"],
    "JPY": ["日幣", "日币", "日圓", "日元", "円", "JPY", "日幣"] ,
    "KRW": ["韓幣", "韩币", "韓元", "韩元", "KRW", "韓國幣"],
    "HKD": ["港幣", "港币", "港元", "HKD"],
    "EUR": ["歐元", "欧元", "EUR"],
    "GBP": ["英鎊", "英镑", "GBP", "英鎊"],
    "SGD": ["新加坡幣", "新加坡币", "新幣", "新币", "SGD"],
    "THB": ["泰銖", "泰铢", "THB"],
    "AUD": ["澳幣", "澳币", "澳元", "AUD"],
    "CAD": ["加幣", "加币", "加元", "CAD"],
    "VND": ["越南盾", "VND"],
    "PHP": ["披索", "菲律賓披索", "菲律宾比索", "PHP"],
    "TWD": ["新台幣", "台幣", "台币", "TWD"],
}


def detect_currency(text):
    """從常見中英文叫法辨識幣別；回傳 ISO 幣別碼與實際匹配文字。"""
    s = str(text or "")
    candidates = []
    for code, aliases in CURRENCY_ALIASES.items():
        for alias in aliases:
            candidates.append((alias, code))
    # 長字串先比對，避免「港幣」等被較短詞搶先命中。
    for alias, code in sorted(candidates, key=lambda x: len(x[0]), reverse=True):
        if re.search(rf"(?<![A-Za-z]){re.escape(alias)}(?![A-Za-z])" if alias.isascii() else re.escape(alias), s, re.I):
            return code, alias
    return None, None


def parse_foreign_record(text, active_currency=None):
    """解析外幣支出或收入；使用者切換幣別後，持續沿用該幣別。"""
    target_date, cleaned = parse_date_hint(text)
    detected_currency, _alias = detect_currency(cleaned)
    # 明確提到新台幣時交給一般記帳流程處理。
    if detected_currency == "TWD":
        return None
    currency = detected_currency if detected_currency else active_currency
    if not currency or currency == "TWD":
        return None

    item_text = cleaned
    # 移除訊息內的幣別名稱，避免被當成消費項目。
    if detected_currency and detected_currency != "TWD":
        for aliases in CURRENCY_ALIASES.values():
            for word in sorted(aliases, key=len, reverse=True):
                item_text = re.sub(rf"(?<![A-Za-z]){re.escape(word)}(?![A-Za-z])" if word.isascii() else re.escape(word), " ", item_text, flags=re.I)
    item_text = re.sub(r"外幣(?:支出|收入|記帳)?", " ", item_text, flags=re.I)
    item_text = re.sub(r"^[\s:：,，$＄]+|[\s:：,，$＄]+$", "", item_text)

    # 沿用台幣記帳的收支判斷與前綴：例如「收入 退款 100」或「早餐100」。
    main_type = infer_main_type(item_text)
    item_text = clean_main_prefix(item_text, main_type)
    match = re.search(r"^(.*?)[\s$＄,，:：|｜-]*(\d[\d,]*(?:\.\d+)?)\s*(?:元|塊|塊錢)?$", item_text)
    if not match:
        return None
    item = match.group(1).strip(" ，,。:：|｜-")
    amount_text = match.group(2).replace(",", "")
    if not item:
        return None
    try:
        amount = float(amount_text)
    except ValueError:
        return None
    if amount <= 0:
        return None

    if main_type == "收入":
        category = default_category(item, "收入")
        if category == "其他" and client:
            try:
                prompt = "你是繁體中文記帳分類助手。請只從以下收入分類選一個，回傳 JSON：{\"category\":\"分類名稱\"}。\n收入內容：" + item + "\n可選分類：" + ", ".join(INCOME_CATEGORIES)
                response = client.chat.completions.create(model="gpt-4o-mini", messages=[{"role": "user", "content": prompt}], max_tokens=80, temperature=0)
                raw = (response.choices[0].message.content or "{}").replace("```json", "").replace("```", "").strip()
                data = json.loads(raw)
                candidate = str(data.get("category", "其他")).strip()
                if candidate in INCOME_CATEGORIES:
                    category = candidate
            except Exception as e:
                sys.stdout.write(f"[FOREIGN INCOME CATEGORY] 分類備援：{e}\n")
    else:
        category = classify_expense_category(item)
    return {"date": target_date.isoformat(), "currency": currency, "main_type": main_type,
            "item": item, "amount": amount, "category": category}

def get_foreign_sheet(line_user_id):
    """在該使用者自己的試算表中建立／取得「外幣」工作表。"""
    if not line_user_id:
        raise RuntimeError("無法辨識 LINE 使用者 ID")
    spreadsheet_id = get_user_spreadsheet_id(line_user_id)
    if not spreadsheet_id:
        raise RuntimeError("尚未設定你的個人試算表。")
    sh = shared_google_client().open_by_key(spreadsheet_id)
    title = get_user_feature_worksheet_title(line_user_id, "foreign_currency", "外幣")
    config = get_personal_sheet_config(line_user_id)
    legacy_titles = ["外幣支出", "外幣"]
    try:
        ws = sh.worksheet(title)
    except gspread.WorksheetNotFound:
        ws = None
        for old_title in legacy_titles:
            if old_title == title:
                continue
            try:
                ws = sh.worksheet(old_title)
                ws.update_title(title)
                sys.stdout.write(f"[SHEETS] 已將工作表「{old_title}」改名為「{title}」\n")
                break
            except gspread.WorksheetNotFound:
                continue
        if ws is None:
            ws = sh.add_worksheet(title=title, rows="2000", cols=str(len(FOREIGN_HEADERS)))
            ws.update("A1", [FOREIGN_HEADERS])
    values = ws.get_all_values()
    if not values:
        ws.update("A1", [FOREIGN_HEADERS])
    elif values[0] != FOREIGN_HEADERS:
        # 若這張分頁只有一列表頭（例如舊版誤建成一般帳本表頭），可安全修正表頭；
        # 若已有任何資料列，絕不自動覆蓋，避免破壞使用者資料。
        if len(values) == 1:
            ws.update("A1", [FOREIGN_HEADERS])
            sys.stdout.write(f"[FOREIGN SHEETS] 已修正空白工作表「{title}」的表頭\n")
        else:
            raise RuntimeError(f"工作表「{title}」已有資料但欄位與外幣表不一致；為避免破壞資料，程式沒有覆蓋。請先備份並確認分頁。")
    return ws


def save_foreign_record(line_user_id, sender, record):
    ws = get_foreign_sheet(line_user_id)
    amount = record["amount"]
    amount_value = int(amount) if float(amount).is_integer() else amount
    main_type = record.get("main_type", "支出")
    # 欄位沿用原本記帳表的「收支／項目／內容」概念，僅在金額前新增幣別欄。
    ws.append_row([
        record["date"], now_tw().strftime("%H:%M"), sender, main_type,
        record["category"], record["item"], record["currency"], amount_value, ""
    ], value_input_option="USER_ENTERED")
    # 分開計算同幣別、同收支類型的本月累積；只做加總，不做匯率換算。
    rows = ws.get_all_records()
    total = 0.0
    current_month = now_tw().strftime("%Y-%m")
    for row in rows:
        if (str(row.get("幣別", "")).upper() == record["currency"]
                and str(row.get("收支", "支出")) == main_type
                and str(row.get("日期", "")).startswith(current_month)):
            try:
                total += float(str(row.get("金額", 0)).replace(",", ""))
            except (ValueError, TypeError):
                pass
    return total

def format_foreign_amount(amount):
    return f"{int(amount):,}" if float(amount).is_integer() else f"{amount:,.2f}".rstrip("0").rstrip(".")


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
    elif period == "week":
        week_start = today - datetime.timedelta(days=today.weekday())
        week_end = week_start + datetime.timedelta(days=6)
        filtered = [r for r in records if len(r["日期"]) >= 10 and week_start.isoformat() <= r["日期"][:10] <= week_end.isoformat()]
        title, empty_text = f"📊 本週記帳總覽（{week_start.strftime('%m/%d')}～{week_end.strftime('%m/%d')}）", "本週還沒有記帳資料。"
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
    """刪除目前模式對應工作表中自己的最後一筆記錄。"""
    mode = get_currency_mode(line_user_id)
    if mode != "TWD":
        ws = get_foreign_sheet(line_user_id)
        values = ws.get_all_values()
        if len(values) <= 1:
            return "外幣工作表目前沒有可刪除的記錄。"
        # 外幣欄位：日期、時間、記錄人、收支、項目、內容、幣別、金額、備註
        for row_num in range(len(values), 1, -1):
            row = values[row_num - 1]
            recorder = row[2].strip() if len(row) > 2 else ""
            if recorder == get_sender_name(line_user_id, {}) or not recorder:
                item = row[5] if len(row) > 5 else "未命名項目"
                amount = row[7] if len(row) > 7 else "0"
                ws.delete_rows(row_num)
                return f"🗑️ 已刪除外幣最後一筆：{item} {amount}（{mode}）"
        return "外幣工作表找不到你自己的記錄，沒有刪除任何資料。"

    ws = get_sheet(line_user_id)
    rows = ws.get_all_records()
    if not rows:
        return "目前找不到你新增的記帳資料。"
    row = rows[-1]
    sheet_row_number = len(rows) + 1
    item = str(row.get("內容", "未命名項目"))
    amount = str(row.get("金額", "0"))
    main_type = str(row.get("收支", "支出"))
    ws.delete_rows(sheet_row_number)
    with db_connect() as conn:
        conn.execute("DELETE FROM transaction_meta WHERE line_user_id=? AND sheet_row=?", (line_user_id, sheet_row_number))
    return f"🗑️ 已刪除上一筆：{main_type}｜{item} ${amount}"


def delete_latest_stock_record(line_user_id):
    """刪除股票工作表最後一筆交易；不碰一般收支或外幣帳本。"""
    ws = get_stock_sheet(line_user_id)
    values = ws.get_all_values()
    if len(values) <= 1:
        return "股票工作表目前沒有可刪除的交易。"
    for row_num in range(len(values), 1, -1):
        row = values[row_num - 1]
        if any(str(cell).strip() for cell in row):
            symbol = row[1] if len(row) > 1 else "未命名股票"
            direction = row[2] if len(row) > 2 else "交易"
            shares = row[3] if len(row) > 3 else ""
            ws.delete_rows(row_num)
            return f"🗑️ 已刪除股票最後一筆：{direction}｜{symbol}｜{shares} 股"
    return "股票工作表目前沒有可刪除的交易。"


def get_settings(line_user_id):
    with db_connect() as conn:
        conn.execute("INSERT INTO user_settings(line_user_id) VALUES(?) ON CONFLICT(line_user_id) DO NOTHING", (line_user_id,))
        row = conn.execute("SELECT * FROM user_settings WHERE line_user_id=?", (line_user_id,)).fetchone()
        return dict(row)


def record_initial_balance(line_user_id, sender, kind, amount):
    """將初始錢包／帳戶金額記入個人工作表的累積收入；使用特殊日期避免算入當月收入。"""
    field = "wallet_initial_recorded" if kind == "錢包" else "account_initial_recorded"
    settings = get_settings(line_user_id)
    if int(settings.get(field, 0)):
        return False
    record = {"date": "初始設定", "main_type": "收入", "item": f"初始{kind}餘額", "amount": int(amount), "category": "其他"}
    save_record(line_user_id, sender, record, "bank")
    with db_connect() as conn:
        conn.execute(f"UPDATE user_settings SET {field}=1 WHERE line_user_id=?", (line_user_id,))
    return True


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
    """顯示個人累積收支、現金／帳戶估算餘額與本期刷卡金額。"""
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
                     and r["日期"][:10] and start.isoformat() <= r["日期"][:10] <= today.isoformat())
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



WELCOME_MESSAGE = """歡迎使用記帳小工具，可以輸入說明看使用方式。"""

HELP_MESSAGE = """小工具可以做到的事🩵

・記支出：午餐 100、買衣服 890
・記收入：收入 薪水 28000
・記刷卡：刷卡 買衣服 890
・記每月還款：繳貸款 5704、繳卡費 5000
・切換外幣模式：先輸入「幣別名稱」後，開始像正常記帳一樣記錄；使用完輸入「新台幣」切回日常使用
・忘記沒關係可以補上日期：昨天 午餐 100、10/20 晚餐 150
・打錯了一般／外幣記錄：刪除（刪掉目前模式下自己最後一筆）
・刪錯股票交易：刪除股票（刪掉自己最後一筆股票交易）
・查詢收支：今日／本週／本月
・看個人帳務統計：統計
・打開試算表：帳本
・買進股票：買0050 100股 60.8
・賣出股票：賣0050 100股 60.8
・喚醒記帳小工具：醒醒（閒置後可能需要等待約 1～2 分鐘）
・提醒對方記帳：提醒
・看使用說明：說明

✨記帳小工具，讓記帳變簡單✨"""



def get_stock_sheet(line_user_id):
    """使用現有 LINE 使用者身分，建立／取得該使用者專屬股票分頁。"""
    get_sheet(line_user_id)  # 確認使用者帳本設定正確
    title = get_user_feature_worksheet_title(line_user_id, "stock", "股票")
    spreadsheet_id = get_user_spreadsheet_id(line_user_id)
    if not spreadsheet_id:
        raise RuntimeError("尚未設定你的個人試算表，請管理員檢查 USER_SPREADSHEET_MAP。")
    spreadsheet = shared_google_client().open_by_key(spreadsheet_id)
    legacy_stock_titles = ["股票_Bu_506d", "股票_Syuan_abc4"]
    # 舊版第三位使用者的股票分頁可能以顯示名稱命名；若「股票」不存在且只有一張舊股票分頁，就原地改名保留資料。
    try:
        spreadsheet.worksheet(title)
    except gspread.WorksheetNotFound:
        legacy_stock_titles.extend(
            tab.title for tab in spreadsheet.worksheets()
            if tab.title.startswith("股票_") and tab.title not in legacy_stock_titles
        )
    return get_or_rename_worksheet(
        spreadsheet, title, legacy_stock_titles,
        2000, len(STOCK_HEADERS), STOCK_HEADERS
    )


def parse_stock_command(text):
    """解析買／賣股票；股數可用股或張，1 張按 1000 股換算。"""
    target_date, cleaned = parse_date_hint(text)
    match = re.fullmatch(
        r"\s*(買|賣)\s*([^\s]+)\s+(\d+(?:,\d{3})*|\d+)\s*(股|張)\s+([\d,]+(?:\.\d+)?)\s*",
        cleaned,
    )
    if not match:
        return None
    action, symbol, quantity_text, unit, price_text = match.groups()
    quantity = int(quantity_text.replace(",", ""))
    if quantity <= 0:
        return None
    shares = quantity * (1000 if unit == "張" else 1)
    price = float(price_text.replace(",", ""))
    if price <= 0:
        return None
    amount = round(shares * price, 2)
    return {
        "date": target_date.isoformat(),
        "symbol": symbol,
        "direction": "買進" if action == "買" else "賣出",
        "shares": shares,
        "quantity_display": f"{quantity:,}{unit}",
        "price": price,
        "amount": amount,
    }


def save_stock_record(line_user_id, record):
    """明確寫入該使用者的 Google 試算表「股票」分頁，並驗證新增結果，避免假成功。"""
    spreadsheet_id = get_user_spreadsheet_id(line_user_id)
    if not spreadsheet_id:
        raise RuntimeError("找不到這位使用者對應的試算表 ID，股票沒有寫入。")

    # 直接開啟使用者自己的試算表，不依賴主帳本分頁名稱來推算股票分頁。
    spreadsheet = shared_google_client().open_by_key(spreadsheet_id)
    worksheet_title = get_user_feature_worksheet_title(line_user_id, "stock", "股票")
    legacy_stock_titles = ["股票_Bu_506d", "股票_Syuan_abc4"]
    try:
        spreadsheet.worksheet(worksheet_title)
    except gspread.WorksheetNotFound:
        legacy_stock_titles.extend(
            tab.title for tab in spreadsheet.worksheets()
            if tab.title.startswith("股票_") and tab.title not in legacy_stock_titles
        )
    ws = get_or_rename_worksheet(
        spreadsheet, worksheet_title, legacy_stock_titles,
        2000, len(STOCK_HEADERS), STOCK_HEADERS
    )

    row_to_write = [
        record["date"], record["symbol"], record["direction"], record.get("quantity_display", record["shares"]),
        record["price"], record["amount"], ""
    ]
    sys.stdout.write(
        f"[STOCK WRITE START] user={line_user_id} "
        f"spreadsheet_id={spreadsheet_id} spreadsheet_title={spreadsheet.title!r} "
        f"worksheet={ws.title!r} row={row_to_write!r}\n"
    )

    # 寫入 Google Sheets；任何 API 例外都會回到錯誤回覆，不會回報成功。
    result = ws.append_row(
        row_to_write,
        value_input_option="USER_ENTERED",
        insert_data_option="INSERT_ROWS",
        include_values_in_response=True,
    )
    updated_range = ((result or {}).get("updates") or {}).get("updatedRange")

    # 二次確認實際工作表最後一列，避免 API 沒回傳更新範圍時仍回覆「成功」。
    saved_values = ws.get_all_values()
    last_row = saved_values[-1] if len(saved_values) > 1 else []
    expected = [str(v) for v in row_to_write]
    actual = [str(v).strip() for v in last_row[:len(expected)]]
    # Google Sheets 可能把 60.0 顯示成 60、把 6080.0 顯示成 6080，數字欄位以數值比較。
    text_fields_match = len(actual) >= 4 and actual[:4] == expected[:4]
    numeric_fields_match = False
    try:
        numeric_fields_match = (
            len(actual) >= 6
            and abs(float(actual[4].replace(",", "")) - float(record["price"])) < 0.000001
            and abs(float(actual[5].replace(",", "")) - float(record["amount"])) < 0.01
        )
    except (ValueError, TypeError):
        numeric_fields_match = False
    if not (text_fields_match and numeric_fields_match):
        sys.stdout.write(
            f"[STOCK WRITE VERIFY FAILED] user={line_user_id} "
            f"spreadsheet_id={spreadsheet_id} worksheet={ws.title!r} "
            f"updated_range={updated_range!r} expected={expected!r} actual={actual!r}\n"
        )
        raise RuntimeError(
            f"Google 試算表未能確認新增資料（試算表：{spreadsheet.title}；工作表：{ws.title}）。"
        )

    sys.stdout.write(
        f"[STOCK WRITE VERIFIED] user={line_user_id} "
        f"spreadsheet_id={spreadsheet_id} spreadsheet_title={spreadsheet.title!r} "
        f"worksheet={ws.title!r} updated_range={updated_range!r}\n"
    )
    return {
        "spreadsheet_id": spreadsheet_id,
        "spreadsheet_title": spreadsheet.title,
        "worksheet": ws.title,
        "updated_range": updated_range or "已讀回驗證",
    }


def welcome_flex():
    return {"type": "flex", "altText": "歡迎使用記帳小工具", "contents": {
        "type": "bubble", "body": {"type": "box", "layout": "vertical", "spacing": "sm", "contents": [
            {"type": "text", "text": WELCOME_MESSAGE, "wrap": True, "size": "sm"}
        ]},
"footer": {"type": "box", "layout": "vertical", "contents": []}
    }}


def handle_event(event):
    event_type = event.get("type")
    reply_token = event.get("replyToken", "")
    source = event.get("source", {})
    line_user_id = source.get("userId", "")

    if event_type == "join":
        # 使用者已將小工具加入群組，不再自動傳送加入群組歡迎訊息。
        sys.stdout.write("[JOIN] 已略過自動歡迎訊息\n")
        return

    if event_type != "message":
        return
    msg = event.get("message", {})
    if msg.get("type") != "text":
        return
    text = msg.get("text", "").strip()
    if not line_user_id:
        reply_message(reply_token, "⚠️ 無法辨識你的 LINE 帳號，請重新傳送訊息。")
        return
    sender = get_sender_name(line_user_id, source)
    sys.stdout.write(f"[MSG] {text!r} from {sender} | LINE userId={line_user_id}\n")

    # 查詢自己的 LINE userId，方便管理員建立個人試算表對應。
    if text in {"我的ID", "我的ID是什麼", "/myid"}:
        if source.get("type") == "user":
            reply_message(reply_token, f"你的 LINE userId：\n{line_user_id}\n請私訊管理員提供這串 ID。")
        else:
            reply_message(reply_token, "為了避免在群組公開帳號識別碼，請私訊機器人輸入「我的ID」。")
        return

    # 精確輸入「醒醒」才會觸發，避免聊天時誤觸；用來確認機器人已啟動並可開始記帳。
    if text == "醒醒":
        reply_message(reply_token, "請開始記帳。")
        sys.stdout.write("[WAKE] 已回覆喚醒確認\n")
        return

    # 兩位使用者都可以手動觸發提醒。
    if text == "提醒":
        reply_message(reply_token, "兄弟！該記帳了")
        return

    if text == "帳本":
        try:
            spreadsheet_id = get_user_spreadsheet_id(line_user_id)
            if not spreadsheet_id:
                reply_message(reply_token, "⚠️ 尚未設定你的個人試算表，請私訊管理員確認帳本綁定。")
            else:
                ws = get_sheet(line_user_id)
                reply_message(reply_token, f"📒 這是你的帳本：\nhttps://docs.google.com/spreadsheets/d/{spreadsheet_id}/edit#gid={ws.id}")
        except Exception as e:
            reply_message(reply_token, f"⚠️ 暫時無法取得帳本連結：{str(e)[:150]}")
        return


    if text in ["說明", "/說明", "help", "/help"]:
        reply_message(reply_token, HELP_MESSAGE)
        return

    # 先辨識單獨輸入的幣別名稱。若帳號尚未綁定，明確告知原因，不要讓「美金」看起來沒反應。
    requested_currency = currency_command(text)
    if requested_currency and not user_is_linked(line_user_id):
        reply_message(reply_token, "⚠️ 這個 LINE 帳號尚未完成個人試算表／資料庫設定，幣別模式沒有切換。請先完成帳號綁定後再試。")
        return

    # 個人分帳模式：每個 LINE 使用者只能操作映射給自己的試算表。
    if not user_is_linked(line_user_id):
        # 一般閒聊不提示設定錯誤，避免群組洗版；但記帳格式必須明確告知原因，不能靜默忽略。
        setup_commands = {"今日", "今天", "/今日", "/today", "本週", "本周", "/本週", "本月", "/本月", "/month", "統計", "/統計", "/stats", "刪除", "刪除上一筆", "刪除上一筆記帳", "刪除股票", "刪除股票上一筆", "刪除上一筆股票", "帳本"}
        is_balance_setting = re.match(r"^(錢包餘額|帳戶餘額|信用卡結算日)\s*[:：]?\s*\d+$", text)
        is_stock_like = bool(re.search(r"(?:^|\s)(?:買|賣)\s*[^\s]+\s+\d+(?:,\d{3})*\s*(?:股|張)\s+[\d,.]+\s*$", text))
        is_record_like = bool(re.search(r"\d[\d,]*(?:\.\d+)?\s*(?:元|塊|塊錢)?\s*$", text)) or is_stock_like
        if text in setup_commands or is_balance_setting or is_record_like:
            sys.stdout.write("[CONFIG] 未記帳：個人試算表映射、GOOGLE_CREDENTIALS_JSON 或 DATABASE_URL 未完整設定\n")
            reply_message(reply_token, "⚠️ 這個 LINE 帳號還沒有綁定個人試算表，這筆尚未記錄。請私訊管理員提供「我的ID」，完成 USER_SPREADSHEET_MAP 設定後再試。")
        return

    # 幣別模式切換：狀態按 LINE 使用者分開保存，重啟服務後仍會延續。
    # 切換失敗時明確回覆錯誤，避免使用者以為已切換，後續卻誤記到新台幣帳本。
    if requested_currency:
        try:
            set_currency_mode(line_user_id, requested_currency)
            if requested_currency == "TWD":
                reply_message(reply_token, "✅ 已切回新台幣日常記帳模式。\n接下來的支出會寫入原本的記帳工作表。")
            else:
                reply_message(reply_token, f"✅ 已切換至 {requested_currency} 外幣記帳模式！\n現在可以開始記錄外幣支出或收入，例如：早餐 100、收入 退款 50。\n接下來會寫入「外幣」工作表，保留你選擇的幣別與原幣金額，不換算新台幣，也不併入台幣統計。\n要切回日常記帳，請單獨輸入「新台幣」。")
        except Exception as e:
            sys.stdout.write(f"[CURRENCY MODE] 切換失敗：{e}\n")
            reply_message(reply_token, f"⚠️ 幣別模式切換失敗，這次沒有切換成功，因此請先不要輸入消費。原因：{str(e)[:180]}")
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
            reply_message(reply_token, f"✅ 信用卡結算日已設定為每月 {value} 日。")
        elif setting_name == "錢包餘額":
            set_setting(line_user_id, "wallet_balance", value)
            record_initial_balance(line_user_id, sender, "錢包", value)
            reply_message(reply_token, f"✅ 錢包餘額 ${value:,}。")
        else:
            set_setting(line_user_id, "account_balance", value)
            record_initial_balance(line_user_id, sender, "帳戶", value)
            reply_message(reply_token, f"✅ 帳戶餘額 ${value:,}。")
        return

    stock_record = parse_stock_command(text)
    if stock_record:
        try:
            stock_destination = save_stock_record(line_user_id, stock_record)
            date_note = f"\n📅 記錄日期：{stock_record['date']}" if stock_record["date"] != now_tw().date().isoformat() else ""
            amount_label = f"${stock_record['amount']:,.2f}".rstrip("0").rstrip(".")
            price_label = f"${stock_record['price']:,.4f}".rstrip("0").rstrip(".")
            reply_message(reply_token,
                f"✅ 股票{stock_record['direction']}｜{stock_record['symbol']}｜{stock_record['shares']:,} 股\n"
                f"成交單價：{price_label}\n成交金額：{amount_label}{date_note}\n📒 已寫入「{stock_destination['worksheet']}」工作表（未含手續費與交易稅）")
        except Exception as e:
            sys.stdout.write(f"[STOCK SHEETS] 寫入失敗：{e}\n")
            reply_message(reply_token, f"⚠️ 股票交易沒有成功寫入，請稍後再試。原因：{str(e)[:180]}")
        return

    commands = {
        "今日": lambda: query_stats(line_user_id, "today"),
        "今天": lambda: query_stats(line_user_id, "today"),
        "/今日": lambda: query_stats(line_user_id, "today"),
        "/today": lambda: query_stats(line_user_id, "today"),
        "本週": lambda: query_stats(line_user_id, "week"),
        "本周": lambda: query_stats(line_user_id, "week"),
        "/本週": lambda: query_stats(line_user_id, "week"),
        "本月": lambda: query_stats(line_user_id, "month"),
        "/本月": lambda: query_stats(line_user_id, "month"),
        "/month": lambda: query_stats(line_user_id, "month"),
        "統計": lambda: query_people_stats(line_user_id),
        "/統計": lambda: query_people_stats(line_user_id),
        "/stats": lambda: query_people_stats(line_user_id),
    }
    if text in commands:
        reply_message(reply_token, commands[text]())
        return

    if text in ["刪除股票", "刪除股票上一筆", "刪除上一筆股票"]:
        try:
            reply_message(reply_token, delete_latest_stock_record(line_user_id))
        except Exception as e:
            sys.stdout.write(f"[STOCK SHEETS] 刪除失敗：{e}\n")
            reply_message(reply_token, f"⚠️ 股票刪除失敗，沒有刪除其他帳本資料。原因：{str(e)[:180]}")
        return

    if text in ["刪除", "刪除上一筆", "刪除上一筆記帳"]:
        try:
            reply_message(reply_token, delete_latest_record(line_user_id))
        except Exception as e:
            sys.stdout.write(f"[SHEETS] 刪除失敗：{e}\n")
            reply_message(reply_token, f"⚠️ 刪除失敗，請稍後再試。原因：{str(e)[:180]}")
        return

    # 明確提到非新台幣幣別時，獨立寫入「外幣」，不納入新台幣帳本統計。
    current_currency_mode = get_currency_mode(line_user_id)
    foreign_record = parse_foreign_record(text, active_currency=current_currency_mode if current_currency_mode != "TWD" else None)
    if foreign_record:
        try:
            foreign_total = save_foreign_record(line_user_id, sender, foreign_record)
            amount_label = format_foreign_amount(foreign_record["amount"])
            total_label = format_foreign_amount(foreign_total)
            currency = foreign_record["currency"]
            date_note = f"\n📅 記錄日期：{foreign_record['date']}" if foreign_record["date"] != now_tw().date().isoformat() else ""
            foreign_type = foreign_record.get("main_type", "支出")
            total_title = "本月外幣收入累積" if foreign_type == "收入" else "本月外幣支出累積"
            reply_message(reply_token,
                f"✅ 外幣{foreign_type}｜{foreign_record['item']} {amount_label} {currency}（{foreign_record['category']}）{date_note}\n"
                f"📊 {total_title}：{total_label} {currency}\n"
                "📒 已記錄到「外幣」工作表；只記錄原幣金額，不換算新台幣，也不計入新台幣統計。")
            sys.stdout.write(f"[FOREIGN SHEETS] 寫入成功：{foreign_record}\n")
        except Exception as e:
            sys.stdout.write(f"[FOREIGN SHEETS] 寫入失敗：{e}\n")
            reply_message(reply_token, f"⚠️ 外幣記錄沒有成功寫入，請稍後再試。原因：{str(e)[:180]}")
        return

    # 外幣模式下若有金額卻無法解析，禁止掉回新台幣帳本，避免誤記到一般支出表。
    explicit_currency_for_safety, _ = detect_currency(text)
    if current_currency_mode != "TWD" and explicit_currency_for_safety != "TWD" and re.search(r"\d[\d,]*(?:\.\d+)?\s*(?:元|塊|塊錢)?\s*$", text):
        reply_message(reply_token, "⚠️ 目前仍在外幣記帳模式，但這筆格式我無法辨識，所以沒有寫入任何工作表。請試試「早餐 100」或「昨天 拉麵 1200」；要記新台幣請先輸入「新台幣」。")
        return

    payment_method = "bank"
    parse_text = text
    # 使用者在外幣模式中明確寫出新台幣，也視為結束外幣模式；若同句有消費，照常記入新台幣帳本。
    explicit_currency, _explicit_alias = detect_currency(parse_text)
    if explicit_currency == "TWD":
        if current_currency_mode != "TWD":
            set_currency_mode(line_user_id, "TWD")
        for alias in sorted(CURRENCY_ALIASES["TWD"], key=len, reverse=True):
            parse_text = re.sub(rf"(?<![A-Za-z]){re.escape(alias)}(?![A-Za-z])" if alias.isascii() else re.escape(alias), " ", parse_text, flags=re.I)
        parse_text = re.sub(r"\s+", " ", parse_text).strip()
    if re.match(r"^現金(?:\s|[:：])", parse_text):
        payment_method = "cash"
        parse_text = re.sub(r"^現金\s*[:：]?\s*", "", parse_text)
    elif re.match(r"^(金融卡|帳戶|銀行帳戶)(?:\s|[:：])", parse_text):
        payment_method = "bank"
        parse_text = re.sub(r"^(金融卡|帳戶|銀行帳戶)\s*[:：]?\s*", "", parse_text)
    # 只有含有金額、且看起來像記帳的文字才進入解析；閒聊靜默忽略。
    if not re.search(r"\d[\d,]*\s*(?:元|塊|塊錢)?\s*$", parse_text):
        return
    record = parse_record(parse_text)
    if not record:
        # 文字帶有金額但解析失敗，才提示格式；普通聊天不回覆。
        if re.search(r"\d", parse_text):
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
        reply_message(reply_token, f"⚠️ 這筆記帳沒有成功寫入，請稍後再試。\n原因：{str(e)[:200]}")


init_db()


def push_message(line_user_id, text):
    if not LINE_CHANNEL_ACCESS_TOKEN or not line_user_id:
        return
    try:
        response = req.post("https://api.line.me/v2/bot/message/push",
            headers={"Content-Type": "application/json", "Authorization": f"Bearer {LINE_CHANNEL_ACCESS_TOKEN}"},
            json={"to": line_user_id, "messages": [{"type": "text", "text": text[:4900]}]}, timeout=15)
        if response.status_code >= 400:
            sys.stdout.write(f"[LINE PUSH] 失敗 {response.status_code}: {response.text}\n")
    except Exception as e:
        sys.stdout.write(f"[LINE PUSH] 錯誤：{e}\n")


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
