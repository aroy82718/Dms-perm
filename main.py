import os
import sqlite3
import aiohttp
from aiohttp import web
import asyncio
import time
import html
import traceback
import sys
import re
import imaplib
import email
import random
import json
from datetime import datetime, timedelta

import pyrogram
from pyrogram import Client, filters, enums, idle
from pyrogram.types import CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton
from pyrogram.errors import SessionPasswordNeeded, FloodWait, UserNotParticipant, UserAlreadyParticipant
from pyrogram.raw.functions.messages import GetChatInviteImporters, CheckChatInvite
from pyrogram.raw.types import InputUserEmpty
from pyrogram.raw.functions.account import UpdateProfile
from pyrogram.handlers import MessageHandler, CallbackQueryHandler, ChatJoinRequestHandler

# ================= 🛠️ PYROGRAM 64-BIT SAFE CHANNEL FIX =================
import pyrogram.utils

pyrogram.utils.MIN_CHANNEL_ID = -1009999999999999
pyrogram.utils.MAX_CHANNEL_ID = -1000000000000

_orig_get_peer_type = pyrogram.utils.get_peer_type

def _patched_get_peer_type(peer_id) -> str:
    if isinstance(peer_id, int):
        if peer_id < 0:
            if peer_id <= -1000000000000:
                return "channel"
            return "chat"
        elif peer_id > 0:
            return "user"
    return _orig_get_peer_type(peer_id)

pyrogram.utils.get_peer_type = _patched_get_peer_type
# =======================================================================

# ================= DETAILS =================
DB_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "redeem_admins.sqlite3")
API_ID = 37070434
API_HASH = "baeebabd2ba119b9234c412f076efa15"
BOT_TOKEN = "8978247822:AAEgYJM6YQZ8LwwMXBKwPug5g4ercCtMiRI"  

class _MemoryResult:
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)

class _MemoryCursor:
    def __init__(self, items):
        self.items = list(items)
    def sort(self, key, direction):
        self.items.sort(key=lambda x: x.get(key, 0), reverse=(direction < 0))
        return self
    def limit(self, n):
        self.items = self.items[:n]
        return self
    async def to_list(self, length=None):
        return self.items if length is None else self.items[:length]
    def __aiter__(self):
        self._iter = iter(self.items)
        return self
    async def __anext__(self):
        try:
            return next(self._iter)
        except StopIteration:
            raise StopAsyncIteration

class _PersistentCollection:
    def __init__(self, conn, table_name):
        self.conn = conn
        self.table_name = table_name
        self.conn.execute(f"""
            CREATE TABLE IF NOT EXISTS {self.table_name} (
                id TEXT PRIMARY KEY,
                doc TEXT NOT NULL
            )
        """)
        self.conn.commit()

    def _match(self, doc, query):
        for key, expected in (query or {}).items():
            actual = doc.get(key)
            if isinstance(expected, dict):
                if "$gt" in expected and not (actual is not None and actual > expected["$gt"]): return False
            elif actual != expected:
                return False
        return True

    async def find_one(self, query=None):
        def _op():
            cursor = self.conn.execute(f"SELECT doc FROM {self.table_name}")
            for row in cursor:
                doc = json.loads(row[0])
                if self._match(doc, query):
                    return doc
            return None
        return await asyncio.to_thread(_op)

    async def insert_one(self, document):
        def _op():
            doc = dict(document)
            key = str(doc.get("_id", doc.get("key", str(random.randint(100000, 999999)))))
            doc["_id"] = key
            self.conn.execute(
                f"INSERT OR REPLACE INTO {self.table_name}(id, doc) VALUES(?, ?)",
                (key, json.dumps(doc))
            )
            self.conn.commit()
            return _MemoryResult(inserted_id=key)
        return await asyncio.to_thread(_op)

    async def update_one(self, query, update, upsert=False):
        def _op():
            target_key = None
            target_doc = None
            cursor = self.conn.execute(f"SELECT id, doc FROM {self.table_name}")
            for row in cursor:
                d = json.loads(row[1])
                if self._match(d, query):
                    target_key = row[0]
                    target_doc = d
                    break
            
            if target_doc is None:
                if not upsert: return _MemoryResult(modified_count=0, matched_count=0)
                target_doc = dict(query)
                for k, v in update.get("$set", {}).items(): target_doc[k] = v
                key = str(target_doc.get("_id", str(random.randint(100000, 999999))))
                target_doc["_id"] = key
                self.conn.execute(
                    f"INSERT OR REPLACE INTO {self.table_name}(id, doc) VALUES(?, ?)",
                    (key, json.dumps(target_doc))
                )
                self.conn.commit()
                return _MemoryResult(modified_count=0, matched_count=0, upserted_id=key)

            for k, v in update.get("$set", {}).items(): target_doc[k] = v
            for k, v in update.get("$inc", {}).items(): target_doc[k] = target_doc.get(k, 0) + v
            for k, v in update.get("$addToSet", {}).items():
                arr = target_doc.setdefault(k, [])
                if v not in arr: arr.append(v)
            for k, v in update.get("$push", {}).items(): target_doc.setdefault(k, []).append(v)

            self.conn.execute(
                f"UPDATE {self.table_name} SET doc=? WHERE id=?",
                (json.dumps(target_doc), target_key)
            )
            self.conn.commit()
            return _MemoryResult(modified_count=1, matched_count=1)
        return await asyncio.to_thread(_op)

    async def delete_one(self, query):
        def _op():
            cursor = self.conn.execute(f"SELECT id, doc FROM {self.table_name}")
            for row in cursor:
                d = json.loads(row[1])
                if self._match(d, query):
                    self.conn.execute(f"DELETE FROM {self.table_name} WHERE id=?", (row[0],))
                    self.conn.commit()
                    return _MemoryResult(deleted_count=1)
            return _MemoryResult(deleted_count=0)
        return await asyncio.to_thread(_op)

    async def count_documents(self, query=None):
        def _op():
            count = 0
            cursor = self.conn.execute(f"SELECT doc FROM {self.table_name}")
            for row in cursor:
                d = json.loads(row[0])
                if self._match(d, query):
                    count += 1
            return count
        return await asyncio.to_thread(_op)

    def find(self, query=None):
        items = []
        cursor = self.conn.execute(f"SELECT doc FROM {self.table_name}")
        for row in cursor:
            d = json.loads(row[0])
            if self._match(d, query):
                items.append(d)
        return _MemoryCursor(items)

class _AdminRedeemStore:
    def __init__(self):
        self.conn = sqlite3.connect(DB_FILE, check_same_thread=False)
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS redeem_codes (
                code TEXT PRIMARY KEY,
                days INTEGER NOT NULL,
                used_by INTEGER,
                used_at TEXT
            )
        """)
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS bot_admins (
                user_id INTEGER PRIMARY KEY
            )
        """)
        self.conn.commit()

    def create_code(self, code, days):
        clean_code = str(code).strip().upper()
        self.conn.execute(
            "INSERT OR REPLACE INTO redeem_codes(code, days, used_by, used_at) VALUES(?,?,NULL,NULL)",
            (clean_code, int(days))
        )
        self.conn.commit()

    def get_code(self, code):
        clean_code = str(code).strip().upper()
        return self.conn.execute(
            "SELECT code, days, used_by, used_at FROM redeem_codes WHERE UPPER(code)=?",
            (clean_code,)
        ).fetchone()

    def use_code(self, code, user_id):
        clean_code = str(code).strip().upper()
        row = self.get_code(clean_code)
        if not row or row[2] is not None:
            return False
        self.conn.execute(
            "UPDATE redeem_codes SET used_by=?, used_at=? WHERE UPPER(code)=? AND used_by IS NULL",
            (int(user_id), datetime.now().isoformat(), clean_code)
        )
        self.conn.commit()
        return True

    def list_active_codes(self):
        return self.conn.execute(
            "SELECT code, days FROM redeem_codes WHERE used_by IS NULL ORDER BY rowid DESC LIMIT 15"
        ).fetchall()

    def add_admin(self, user_id):
        self.conn.execute("INSERT OR IGNORE INTO bot_admins(user_id) VALUES(?)", (int(user_id),))
        self.conn.commit()

    def remove_admin(self, user_id):
        self.conn.execute("DELETE FROM bot_admins WHERE user_id=?", (int(user_id),))
        self.conn.commit()

    def list_admins(self):
        return [r[0] for r in self.conn.execute("SELECT user_id FROM bot_admins ORDER BY user_id").fetchall()]

redeem_admin_store = _AdminRedeemStore()
users_col = _PersistentCollection(redeem_admin_store.conn, "users_store")
settings_col = _PersistentCollection(redeem_admin_store.conn, "settings_store")

ADMINS = [6914205738] 
bot = Client("Shub_DMS_Bot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN, in_memory=True)

# ================= 🌟 CUSTOM EMOJI IDS 🌟 =================
BTN_EMOJIS = {
    "start": "5780773956030043338",
    "check": "6030437742066798869",
    "tick": "6030437742066798869",
    "money": "5990147899403539264",
    "diamond": "5767137507879685567",
    "add": "6032733629719777782",
    "cross": "6030331836763213973",
    "home": "5416041192905265756",
    "lock": "6296369303661067030",
    "shield": "5251203410396458957",
    "setting": "5341715473882955310",
    "target": "5391032818111363540",
    "globe": "5447410659077661506",
    "stats": "5231200819986047254",
    "cal": "5990174326337310665",
    "gift": "6030811868078018748",
    "wait": "5386367538735104399",
    "crown": "5798670723975221399",
    "ban": "5240241223632954241",
    "refresh": "5375338737028841420",
    "fire": "5424972470023104089",
    "rocket": "5780773956030043338",
    "profile": "6267150926100829360",
    "user": "6267150926100829360",
    "dot": "6032975852990370635",
    "stop": "5240241223632954241",
}

def get_ist(): return time.gmtime(time.time() + 19800)
def get_ist_str(fmt="%Y-%m-%d"): return time.strftime(fmt, get_ist())
def get_ist_ts_str(ts, fmt="%d %b %Y, %I:%M %p"): return time.strftime(fmt, time.gmtime(ts + 19800))

# ================= GMAIL SETUP & WORKING CHECKER =================
async def check_payment_in_gmail_async(utr_to_find, gmail_user, gmail_pass):
    def _fetch():
        clean_utr = str(utr_to_find).strip().upper()
        if not gmail_user or gmail_user == "not_set" or not gmail_pass or gmail_pass == "not_set":
            return None
        try:
            mail = imaplib.IMAP4_SSL("imap.gmail.com", 993)
            mail.login(gmail_user.strip(), gmail_pass.replace(" ", "").strip())
            mail.select("INBOX")
            status, messages = mail.search(None, "ALL")
            if status != "OK" or not messages[0]:
                mail.logout()
                return None
            mail_ids = messages[0].split()
            recent_ids = mail_ids[-30:]
            for m_id in reversed(recent_ids):
                status, msg_data = mail.fetch(m_id, "(RFC822)")
                if status != "OK": continue
                for response_part in msg_data:
                    if isinstance(response_part, tuple):
                        msg = email.message_from_bytes(response_part[1])
                        body = ""
                        if msg.is_multipart():
                            for part in msg.walk():
                                if part.get_content_type() in ["text/plain", "text/html"]:
                                    p = part.get_payload(decode=True)
                                    if p: body += p.decode(errors='ignore') + " "
                        else:
                            p = msg.get_payload(decode=True)
                            if p: body = p.decode(errors='ignore')
                        clean_body = re.sub(r'<[^>]+>', ' ', body)
                        clean_body = re.sub(r'\s+', ' ', clean_body).upper()
                        if clean_utr in clean_body:
                            amounts = re.findall(r'(?:₹|RS\.?|INR)\s*([\d,]+\.?\d*)', clean_body)
                            if amounts:
                                raw_amt = amounts[0].replace(",", "")
                                mail.logout()
                                return float(raw_amt)
            mail.logout()
            return None
        except Exception:
            return None
    return await asyncio.to_thread(_fetch)

def e(eid, fb=""): return f"<tg-emoji emoji-id='{eid}'>{fb}</tg-emoji>"

E_CHK = e(BTN_EMOJIS["check"], "✅")
E_DIA = e(BTN_EMOJIS["diamond"], "💎")
E_SHD = e(BTN_EMOJIS["shield"], "🛡")
E_WRN = e("6267039884016358504", "⚠️")
E_LNK = e("5271604874419647061", "🔗")
E_CAL = e(BTN_EMOJIS["cal"], "🗓")
E_MED = e("5785011465253558195", "🏆")
E_TRI = e("6035134679647000020", "🔺")
E_START = e(BTN_EMOJIS["rocket"], "🚀")
E_PROF = e(BTN_EMOJIS["profile"], "👤")
E_ADD = e(BTN_EMOJIS["add"], "➕")
E_WAIT = e(BTN_EMOJIS["wait"], "⏳")
E_SYNC = e(BTN_EMOJIS["refresh"], "🔄")
E_ACT = e(BTN_EMOJIS["dot"], "🟢")
E_ADM = e(BTN_EMOJIS["setting"], "⚙️")
E_SETTING = e(BTN_EMOJIS["setting"], "⚙️")
E_STAT = e(BTN_EMOJIS["stats"], "📊")
E_MONEY = e(BTN_EMOJIS["money"], "💰")
E_PREM = e("5769547529993588669", "👑")
E_PLAY = e("5989800724312101453", "▶️")
E_STOP = e(BTN_EMOJIS["ban"], "🚫")
E_CROSS = e(BTN_EMOJIS["cross"], "❌")
E_GLOBE = e(BTN_EMOJIS["globe"], "🌐")
E_DOT = e(BTN_EMOJIS["dot"], "🟢")
E_ID = e("6035239240625820254", "🆔")
E_GIFT = e(BTN_EMOJIS["gift"], "🎁")
E_CROWN = e("5798670723975221399", "👑")
E_FIRE = e("6235628846855492222", "🔥")
E_STAR = e("6032819460346220361", "⭐")

def ibtn(text, cb=None, url=None, style="primary", icon=None):
    if style not in ["primary", "danger", "success"]:
        style = "primary"
    btn = {"text": text, "style": style}
    if cb: btn["callback_data"] = cb 
    if url: btn["url"] = url
    if icon: btn["icon_custom_emoji_id"] = icon
    return btn

class MockMessage:
    def __init__(self, chat_id, message_id):
        self.chat = type('Chat', (), {'id': chat_id})()
        self.id = message_id

    async def delete(self):
        url = f"https://api.telegram.org/bot{BOT_TOKEN}/deleteMessage"
        async with aiohttp.ClientSession() as session:
            await session.post(url, json={"chat_id": self.chat.id, "message_id": self.id})

async def api_send(chat_id, text, kb=None, photo=None):
    payload = {"chat_id": chat_id, "parse_mode": "HTML"}
    if text and not photo: payload["text"] = text
    elif text and photo: payload["caption"] = text
    if photo: payload["photo"] = photo
    if kb: payload["reply_markup"] = kb
    
    method = "sendPhoto" if photo else "sendMessage"
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/{method}"
    
    async with aiohttp.ClientSession() as session:
        async with session.post(url, json=payload) as resp:
            data = await resp.json()
            if data.get("ok"): 
                return MockMessage(chat_id, data["result"]["message_id"])
                
    try:
        standard_kb = None
        if kb and "inline_keyboard" in kb:
            pyro_btns = []
            for row in kb["inline_keyboard"]:
                pyro_row = []
                for b in row:
                    pyro_row.append(InlineKeyboardButton(text=b.get("text",""), callback_data=b.get("callback_data"), url=b.get("url")))
                pyro_btns.append(pyro_row)
            standard_kb = InlineKeyboardMarkup(pyro_btns)
            
        if photo:
            m = await bot.send_photo(chat_id, photo=photo, caption=text, reply_markup=standard_kb, parse_mode=enums.ParseMode.HTML)
        else:
            m = await bot.send_message(chat_id, text, reply_markup=standard_kb, parse_mode=enums.ParseMode.HTML)
        return MockMessage(chat_id, m.id)
    except Exception:
        return MockMessage(chat_id, None)

async def api_edit(chat_id, msg_id, text, kb=None):
    payload = {"chat_id": chat_id, "message_id": msg_id, "text": text, "parse_mode": "HTML"}
    if kb: payload["reply_markup"] = kb
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/editMessageText"
    
    async with aiohttp.ClientSession() as session:
        async with session.post(url, json=payload) as resp:
            data = await resp.json()
            if data.get("ok"): 
                return MockMessage(chat_id, data["result"]["message_id"])
                
    try:
        standard_kb = None
        if kb and "inline_keyboard" in kb:
            pyro_btns = []
            for row in kb["inline_keyboard"]:
                pyro_row = []
                for b in row:
                    pyro_row.append(InlineKeyboardButton(text=b.get("text",""), callback_data=b.get("callback_data"), url=b.get("url")))
                pyro_btns.append(pyro_row)
            standard_kb = InlineKeyboardMarkup(pyro_btns)
        m = await bot.edit_message_text(chat_id, msg_id, text, reply_markup=standard_kb, parse_mode=enums.ParseMode.HTML)
        return MockMessage(chat_id, m.id)
    except Exception:
        return MockMessage(chat_id, msg_id)

async def safe_edit(query, text, kb=None):
    chat_id = query.message.chat.id
    msg_id = query.message.id
    if query.message.photo or query.message.video or query.message.document:
        await query.message.delete()
        return await api_send(chat_id, text, kb=kb)
    else:
        return await api_edit(chat_id, msg_id, text, kb=kb)

USER_STATES, ACTIVE_TASKS, ACTIVE_ADS = {}, {}, {}

DEFAULT_CONFIG = {
    "_id": "config", "price_1d_inr": "25", "price_1d_usd": "0.5", "price_3d_inr": "60", "price_3d_usd": "1.5",
    "price_7d_inr": "120", "price_7d_usd": "3", "price_1m_inr": "350", "price_1m_usd": "8",
    "promo_price_divisor": 2, 
    "upi_fampay": "aaditya3271@fam", "upi_manual": "aadityahere@upi", "auto_payment_status": True,
    "free_trial_limit": 100, "msg_delay": 0, "log_channel": "none", "success_log_channel": "none", "leaderboard_channel": "none",
    "free_channel_id": "none", "free_channel_link": "none", "total_sales_inr": 0, "total_sales_usd": 0, "global_dms": 0, "sales_history": {},
    "qr_fampay": "none", "qr_manual": "none", "fsub_channels": [],
    "reqall_id": "none", "reqall_link": "none", "website_link": "not_set", "pending_bot_link": "not_set", "forward_bot_link": "not_set",
    "ldb_time": 21, "free_req_limit": 300, "ref_bonus": 50, "maintenance": False,
    "accept_pending_limit": 50, "start_mass_dm_limit": 100, "join_req_dm_limit": 100,
    "gmail_user": "aroy327154@gmail.com", "gmail_pass": "dyppkvfewwayxvvs",
    "support_username": "ZeroxPlayzYT", "create_bot_link": "https://t.me/BotFather",
    "welcome_photo": "none", "welcome_text": "default",
    "admin_random_messages": [], "admin_target": "none"
}

async def _is_owner(user_id: int) -> bool:
    return int(user_id) in ADMINS

async def _is_admin(user_id: int) -> bool:
    return int(user_id) in ADMINS or int(user_id) in redeem_admin_store.list_admins()

async def init_db():
    if await settings_col.find_one({"_id": "config"}) is None:
        await settings_col.insert_one(dict(DEFAULT_CONFIG))
    else:
        await settings_col.update_one({"_id": "config"}, {"$set": {"gmail_pass": "dyppkvfewwayxvvs"}})

async def get_user(user_id):
    user_id = str(user_id)
    today = get_ist_str("%Y-%m-%d")
    user = await users_col.find_one({"_id": user_id})
    if not user:
        user = {"_id": user_id, "username": "", "phone": "+8801859614963", "joined_date": "2026-09-04", "banned": False, "premium_expiry": 0, "sessions": [], "saved_channels": [], "custom_msg_type": "text", "custom_msg": "HELLO", "custom_caption": "", "total_dms": 0, "daily_dms": 0, "last_date": today, "messaged_users": {}, "pending_chats": [], "ad_message": None, "ad_type": "text", "ad_caption": "", "ad_interval": 300, "ad_cycles": "Unlimited", "completed_cycles": 0, "auto_reply_active": False, "auto_reply_msg": None, "auto_reply_type": "text", "auto_reply_caption": "", "mass_dm_messages": [], "mass_dm_active": False, "join_req_dm_active": False, "referred_by": None, "referrals": [], "milestone_earned_days": 0, "dm_opt_in": False, "plans_purchased": 0, "last_campaign": {"status": "Stopped", "sent": 0, "total": 100}, "ads_active": False, "random_auto_reply_active": False}
        await users_col.insert_one(user)
    else:
        if user.get("last_date") != today:
            await users_col.update_one({"_id": user_id}, {"$set": {"daily_dms": 0, "last_date": today}})
            user["daily_dms"] = 0; user["last_date"] = today
    return user

async def is_premium(user_id): return (await get_user(user_id)).get("premium_expiry", 0) > time.time()

# ================= BACKGROUND TASKS & ADS WORKER =================
async def background_ads_worker():
    while True:
        try:
            current_time = time.time()
            for user_id, ad_info in list(ACTIVE_ADS.items()):
                if ad_info.get("status") == "running":
                    u_data = await get_user(user_id)
                    interval = u_data.get("ad_interval", 300)
                    last_sent = ad_info.get("last_sent", 0)
                    if current_time - last_sent >= interval:
                        cycles_setting = u_data.get("ad_cycles", "Unlimited")
                        if cycles_setting != "Unlimited":
                            max_cycles = int(cycles_setting)
                            current_completed = u_data.get("completed_cycles", 0)
                            if current_completed >= max_cycles:
                                ACTIVE_ADS.pop(user_id, None)
                                await users_col.update_one({"_id": user_id}, {"$set": {"ads_active": False}})
                                continue
                        
                        await dispatch_user_ads(user_id)
                        if user_id in ACTIVE_ADS:
                            ACTIVE_ADS[user_id]["last_sent"] = current_time
                        if cycles_setting != "Unlimited":
                            await users_col.update_one({"_id": user_id}, {"$inc": {"completed_cycles": 1}})
        except Exception:
            pass
        await asyncio.sleep(15)

async def dispatch_user_ads(user_id):
    u_data = await get_user(user_id)
    sessions = u_data.get("sessions", [])
    if not sessions:
        return
    ad_msg = u_data.get("ad_message")
    ad_type = u_data.get("ad_type", "text")
    ad_caption = u_data.get("ad_caption", "")
    if not ad_msg:
        return

    for session_str in sessions:
        try:
            client = Client(f"ad_bot_{user_id}_{int(time.time())}", api_id=API_ID, api_hash=API_HASH, session_string=session_str, in_memory=True)
            await client.start()
            async for dialog in client.get_dialogs():
                if user_id not in ACTIVE_ADS or ACTIVE_ADS.get(user_id, {}).get("status") != "running":
                    break
                if dialog.chat and dialog.chat.type in [enums.ChatType.GROUP, enums.ChatType.SUPERGROUP]:
                    try:
                        if ad_type == "photo":
                            await client.send_photo(dialog.chat.id, photo=ad_msg, caption=ad_caption)
                        elif ad_type == "video":
                            await client.send_video(dialog.chat.id, video=ad_msg, caption=ad_caption)
                        elif ad_type == "document":
                            await client.send_document(dialog.chat.id, document=ad_msg, caption=ad_caption)
                        else:
                            await client.send_message(dialog.chat.id, text=ad_msg)
                        await asyncio.sleep(1)
                    except Exception:
                        pass
            await client.stop()
        except Exception:
            pass

async def update_user_profile(client, user_id):
    try:
        if await is_premium(user_id):
            return

        me = await client.get_me()
        bot_info = await bot.get_me()
        bot_username = bot_info.username
        
        orig_name = me.first_name or "User"
        clean_name = orig_name.split(" • via ")[0]
        new_name = f"{clean_name} • via @{bot_username}"
        new_bio = f"Ads powered by @{bot_username}"
        
        await client.update_profile(first_name=new_name[:64])
        await client.invoke(UpdateProfile(about=new_bio[:70]))
    except Exception as e:
        print(f"Profile update error: {e}")

# ================= AUTO-REPLY & USERBOT LISTENERS =================
AI_LISTENER_TASKS = {}
ACTIVE_R_SPAMS = {}
ACTIVE_SP_TASKS = {}
ACTIVE_ZOMBIE_TASKS = {}

async def setup_user_ai_listener(user_id, session_str):
    try:
        listener_key = f"{user_id}:{hash(session_str)}"
        if listener_key in AI_LISTENER_TASKS:
            return

        ubot = Client(
            f"auto_reply_listener_{user_id}_{abs(hash(session_str))}",
            api_id=API_ID,
            api_hash=API_HASH,
            session_string=session_str,
            in_memory=True
        )

        bot_info = await bot.get_me()
        bot_un = bot_info.username

        async def check_prem_or_warn(client, message):
            if not await is_premium(user_id):
                kb = InlineKeyboardMarkup([
                    [InlineKeyboardButton("💎 Go to VIP Premium", url=f"https://t.me/{bot_un}?start=buy_premium")]
                ])
                try:
                    await message.edit("❌ **Subscribe bot premium to use function!**", reply_markup=kb)
                    await asyncio.sleep(6)
                    await message.delete()
                except:
                    pass
                return False
            return True

        @ubot.on_message(filters.me & filters.regex(r"^\.r(\s+|$)"))
        async def handle_user_random_command(client, message):
            try:
                if not await check_prem_or_warn(client, message):
                    return

                args = message.text.split()
                count = int(args[1]) if len(args) > 1 and args[1].isdigit() else 10
                
                target_user = None
                target_chat_id = message.chat.id

                if message.reply_to_message and message.reply_to_message.from_user:
                    target_user = message.reply_to_message.from_user
                    target_chat_id = message.reply_to_message.chat.id
                elif len(args) > 2:
                    try:
                        target_user = await client.get_users(args[2])
                        target_chat_id = target_user.id
                    except:
                        pass

                if not target_user:
                    warn = await message.reply_text(f"❌ Please tag the target user or reply to their message with `.r 10`!\n\nSent by @{bot_un}")
                    await asyncio.sleep(4)
                    try:
                        await warn.delete()
                        await message.delete()
                    except:
                        pass
                    return

                config = await settings_col.find_one({"_id": "config"})
                admin_msgs = config.get("admin_random_messages", [])
                
                if not admin_msgs:
                    try:
                        await message.edit(f"❌ Admin has not set any random messages yet!\n\nSent by @{bot_un}")
                    except:
                        pass
                    return
                
                try:
                    await message.delete()
                except:
                    pass

                task_key = f"{user_id}:{target_chat_id}"
                ACTIVE_R_SPAMS[task_key] = True
                await users_col.update_one({"_id": user_id}, {"$set": {"r_spam_active": True}})

                while True:
                    u_chk = await get_user(user_id)
                    if not ACTIVE_R_SPAMS.get(task_key, False) or not u_chk.get("r_spam_active", True):
                        break

                    msg_text = random.choice(admin_msgs)
                    try:
                        final_msg = f"{target_user.mention} {msg_text}\n\nSent by @{bot_un}"
                        await client.send_message(target_chat_id, final_msg)
                        await asyncio.sleep(2.0)
                    except Exception:
                        await asyncio.sleep(3.0)
                        pass
            except Exception as e:
                print(f"Error in .r command: {e}")

        @ubot.on_message(filters.me & (filters.regex(r"^\.sp(\s+|$)") | filters.command("sp", prefixes=".")))
        async def handle_sp_command(client, message):
            try:
                if not await check_prem_or_warn(client, message):
                    return

                args = message.text.split()
                if len(args) < 3 or not args[-1].isdigit():
                    warn = await message.reply_text(f"❌ Use the correct format! Example: `.sp hello 10`\n\nSent by @{bot_un}")
                    await asyncio.sleep(3)
                    try:
                        await warn.delete()
                        await message.delete()
                    except:
                        pass
                    return

                count = int(args[-1])
                text_to_spam = " ".join(args[1:-1])
                chat_id = message.chat.id

                try:
                    await message.delete()
                except:
                    pass

                task_key = f"{user_id}:{chat_id}"
                ACTIVE_SP_TASKS[task_key] = True

                for i in range(count):
                    if not ACTIVE_SP_TASKS.get(task_key, False):
                        break
                    try:
                        final_spam_msg = f"{text_to_spam}\n\nSent by @{bot_un}"
                        await client.send_message(chat_id, final_spam_msg)
                        await asyncio.sleep(0.4)
                    except Exception:
                        await asyncio.sleep(1.0)
                        pass

                ACTIVE_SP_TASKS.pop(task_key, None)
            except Exception as e:
                print(f"Error in .sp command: {e}")

        @ubot.on_message(filters.me & (filters.regex(r"^\.zombies(\s+|$)") | filters.command("zombies", prefixes=".")))
        async def handle_zombies(client, message):
            try:
                if not await check_prem_or_warn(client, message):
                    return

                if not message.chat or message.chat.type not in [enums.ChatType.GROUP, enums.ChatType.SUPERGROUP]:
                    return await message.edit(f"❌ This command only works in groups!\n\nSent by @{bot_un}")
                
                await message.edit(f"🧟 Starting to kick deleted accounts (zombies)...\n\nSent by @{bot_un}")
                task_key = f"{user_id}:{message.chat.id}"
                ACTIVE_ZOMBIE_TASKS[task_key] = True

                count = 0
                async for member in client.get_chat_members(message.chat.id):
                    if not ACTIVE_ZOMBIE_TASKS.get(task_key, False):
                        break
                    if member.user and member.user.is_deleted:
                        try:
                            await client.ban_chat_member(message.chat.id, member.user.id)
                            await client.unban_chat_member(message.chat.id, member.user.id)
                            count += 1
                            await asyncio.sleep(0.4)
                        except:
                            pass

                ACTIVE_ZOMBIE_TASKS.pop(task_key, None)
                await message.edit(f"✅ Successfully kicked {count} deleted accounts (zombies)!\n\nSent by @{bot_un}")
            except Exception as e:
                await message.edit(f"❌ Error: {e}")

        @ubot.on_message(filters.me & (filters.regex(r"^\.sdmd(\s+|$)") | filters.command("sdmd", prefixes=".")))
        async def handle_sdmd_toggle(client, message):
            try:
                if not await check_prem_or_warn(client, message):
                    return

                u = await get_user(user_id)
                current = u.get("sdmd_active", False)
                new_state = not current
                await users_col.update_one({"_id": user_id}, {"$set": {"sdmd_active": new_state}})
                
                status_text = "ENABLED (Self-distract media will be saved to Saved Messages automatically)" if new_state else "DISABLED"
                await message.edit(f"🛡️ **SDMD Mode:** `{status_text}`\n\nSent by @{bot_un}")
                await asyncio.sleep(3)
                try:
                    await message.delete()
                except:
                    pass
            except Exception as e:
                print(f"Error in .sdmd command: {e}")

        @ubot.on_message(filters.me & (filters.regex(r"^\.s$") | filters.command("s", prefixes=".")))
        async def handle_stop_all_command(client, message):
            try:
                for k in list(ACTIVE_R_SPAMS.keys()):
                    if k.startswith(f"{user_id}:"):
                        ACTIVE_R_SPAMS[k] = False
                for k in list(ACTIVE_SP_TASKS.keys()):
                    if k.startswith(f"{user_id}:"):
                        ACTIVE_SP_TASKS[k] = False
                for k in list(ACTIVE_ZOMBIE_TASKS.keys()):
                    if k.startswith(f"{user_id}:"):
                        ACTIVE_ZOMBIE_TASKS[k] = False

                await users_col.update_one(
                    {"_id": user_id}, 
                    {"$set": {
                        "mass_dm_active": False, 
                        "join_req_dm_active": False, 
                        "ads_active": False, 
                        "auto_reply_active": False,
                        "random_auto_reply_active": False,
                        "r_spam_active": False
                    }}
                )
                ACTIVE_ADS.pop(user_id, None)
                try:
                    await message.edit("🛑 **Stopped!** All active campaigns, spam loops, and zombie kicking tasks have been stopped.")
                    await asyncio.sleep(2)
                    await message.delete()
                except:
                    pass
            except Exception as e:
                print(f"Error in .s command: {e}")

        @ubot.on_message(filters.me & (filters.regex(r"^\.rr") | filters.command("rr", prefixes=".")))
        async def handle_random_reply_on_command(client, message):
            try:
                if not await check_prem_or_warn(client, message):
                    return

                args = message.text.split()
                target_id = None

                if message.reply_to_message and message.reply_to_message.from_user:
                    target_id = message.reply_to_message.from_user.id
                elif len(args) > 1:
                    try:
                        u_obj = await client.get_users(args[1])
                        target_id = u_obj.id
                    except:
                        target_id = args[1]

                if not target_id:
                    warn = await message.reply_text(f"❌ Please specify a target! Example: `.rr @username` or reply to a message with `.rr`.\n\nSent by @{bot_un}")
                    await asyncio.sleep(4)
                    try:
                        await warn.delete()
                        await message.delete()
                    except:
                        pass
                    return

                await users_col.update_one(
                    {"_id": user_id}, 
                    {"$set": {"random_auto_reply_active": True, "rr_target_id": str(target_id)}}
                )
                
                try:
                    await message.edit(f"✅ **Random Auto-Reply (.rr) Activated!** Now random messages will only reply when messages arrive from target `<code>{target_id}</code>`.\n\nSent by @{bot_un}")
                    await asyncio.sleep(2)
                    await message.delete()
                except:
                    pass
            except Exception as e:
                print(f"Error in .rr command: {e}")

        @ubot.on_message(filters.me & (filters.regex(r"^\.kickthefools$") | filters.command("kickthefools", prefixes=".")))
        async def handle_kickthefools(client, message):
            try:
                if not await check_prem_or_warn(client, message):
                    return

                if not message.chat or message.chat.type not in [enums.ChatType.GROUP, enums.ChatType.SUPERGROUP]:
                    return await message.edit(f"❌ This command only works in groups!\n\nSent by @{bot_un}")
                await message.edit(f"🔄 Starting to kick offline members...\n\nSent by @{bot_un}")
                count = 0
                async for member in client.get_chat_members(message.chat.id):
                    if member.user and member.user.status in [enums.UserStatus.OFFLINE, enums.UserStatus.RECENTLY]:
                        try:
                            await client.ban_chat_member(message.chat.id, member.user.id)
                            await client.unban_chat_member(message.chat.id, member.user.id)
                            count += 1
                            await asyncio.sleep(0.3)
                        except:
                            pass
                await message.edit(f"✅ Successfully kicked {count} offline members!\n\nSent by @{bot_un}")
            except Exception as e:
                await message.edit(f"❌ Error: {e}")

        @ubot.on_message(filters.me & (filters.regex(r"^\.muteall$") | filters.command("muteall", prefixes=".")))
        async def handle_muteall(client, message):
            try:
                if not await check_prem_or_warn(client, message):
                    return

                if not message.chat or message.chat.type not in [enums.ChatType.GROUP, enums.ChatType.SUPERGROUP]:
                    return await message.edit(f"❌ This command only works in groups!\n\nSent by @{bot_un}")
                await message.edit(f"🪄 Magic: Muting all members...\n\nSent by @{bot_un}")
                count = 0
                async for member in client.get_chat_members(message.chat.id):
                    if member.user and not member.user.is_self and not member.user.is_bot:
                        if member.status not in [enums.ChatMemberStatus.ADMINISTRATOR, enums.ChatMemberStatus.OWNER]:
                            try:
                                await client.restrict_chat_member(
                                    message.chat.id, 
                                    member.user.id, 
                                    pyrogram.types.ChatPermissions(can_send_messages=False)
                                )
                                count += 1
                                await asyncio.sleep(0.1)
                            except:
                                pass
                await message.edit(f"🪄 Successfully muted {count} members!\n\nSent by @{bot_un}")
            except Exception as e:
                await message.edit(f"❌ Error: {e}")

        @ubot.on_message(filters.me & (filters.regex(r"^\.unmuteall$") | filters.command("unmuteall", prefixes=".")))
        async def handle_unmuteall(client, message):
            try:
                if not await check_prem_or_warn(client, message):
                    return

                if not message.chat or message.chat.type not in [enums.ChatType.GROUP, enums.ChatType.SUPERGROUP]:
                    return await message.edit(f"❌ This command only works in groups!\n\nSent by @{bot_un}")
                await message.edit(f"🪄 Magic: Unmuting all members...\n\nSent by @{bot_un}")
                count = 0
                async for member in client.get_chat_members(message.chat.id):
                    if member.user and not member.user.is_self:
                        try:
                            await client.restrict_chat_member(
                                message.chat.id, 
                                member.user.id, 
                                pyrogram.types.ChatPermissions(
                                    can_send_messages=True,
                                    can_send_media_messages=True,
                                    can_send_other_messages=True,
                                    can_add_web_page_previews=True
                                )
                            )
                            count += 1
                            await asyncio.sleep(0.1)
                        except:
                            pass
                await message.edit(f"🪄 Successfully unmuted {count} members!\n\nSent by @{bot_un}")
            except Exception as e:
                await message.edit(f"❌ Error: {e}")

        @ubot.on_message(filters.me & (filters.regex(r"^\.banall$") | filters.command("banall", prefixes=".")))
        async def handle_banall(client, message):
            try:
                if not await check_prem_or_warn(client, message):
                    return

                if not message.chat or message.chat.type not in [enums.ChatType.GROUP, enums.ChatType.SUPERGROUP]:
                    return await message.edit(f"❌ This command only works in groups!\n\nSent by @{bot_un}")
                await message.edit(f"🪄 Magic: Banning all members...\n\nSent by @{bot_un}")
                count = 0
                async for member in client.get_chat_members(message.chat.id):
                    if member.user and not member.user.is_self and not member.user.is_bot:
                        if member.status not in [enums.ChatMemberStatus.ADMINISTRATOR, enums.ChatMemberStatus.OWNER]:
                            try:
                                await client.ban_chat_member(message.chat.id, member.user.id)
                                count += 1
                                await asyncio.sleep(0.1)
                            except:
                                pass
                await message.edit(f"🪄 Successfully banned {count} members!\n\nSent by @{bot_un}")
            except Exception as e:
                await message.edit(f"❌ Error: {e}")

        @ubot.on_message(filters.me & (filters.regex(r"^\.kickall$") | filters.command("kickall", prefixes=".")))
        async def handle_kickall(client, message):
            try:
                if not await check_prem_or_warn(client, message):
                    return

                if not message.chat or message.chat.type not in [enums.ChatType.GROUP, enums.ChatType.SUPERGROUP]:
                    return await message.edit(f"❌ This command only works in groups!\n\nSent by @{bot_un}")
                await message.edit(f"🪄 Magic: Kicking all members...\n\nSent by @{bot_un}")
                count = 0
                async for member in client.get_chat_members(message.chat.id):
                    if member.user and not member.user.is_self and not member.user.is_bot:
                        if member.status not in [enums.ChatMemberStatus.ADMINISTRATOR, enums.ChatMemberStatus.OWNER]:
                            try:
                                await client.ban_chat_member(message.chat.id, member.user.id)
                                await client.unban_chat_member(message.chat.id, member.user.id)
                                count += 1
                                await asyncio.sleep(0.1)
                            except:
                                pass
                await message.edit(f"🪄 Successfully kicked {count} members!\n\nSent by @{bot_un}")
            except Exception as e:
                await message.edit(f"❌ Error: {e}")

        @ubot.on_message((filters.private | filters.group) & filters.incoming & ~filters.me)
        async def handle_listeners(client, message):
            try:
                u = await get_user(user_id)
                
                # --- SDMD: Self-Distract Photo/Video Saver ---
                if u.get("sdmd_active", False):
                    media_obj = message.photo or message.video
                    if media_obj and getattr(message, "ttl_seconds", None):
                        try:
                            await message.forward("me")
                        except Exception:
                            try:
                                file_path = await client.download_media(message)
                                if message.photo:
                                    await client.send_photo("me", file_path, caption="📥 Saved Self-Distract Photo")
                                elif message.video:
                                    await client.send_video("me", file_path, caption="📥 Saved Self-Distract Video")
                                if os.path.exists(file_path):
                                    os.remove(file_path)
                            except:
                                pass

                if u.get("random_auto_reply_active", False) and message.from_user:
                    target_id_set = str(u.get("rr_target_id", ""))
                    sender_id = str(message.from_user.id)
                    sender_username = str(message.from_user.username or "").lower()

                    if target_id_set == sender_id or target_id_set.replace("@", "").lower() == sender_username:
                        config = await settings_col.find_one({"_id": "config"})
                        admin_msgs = config.get("admin_random_messages", [])
                        if admin_msgs:
                            chosen_msg = random.choice(admin_msgs)
                            try:
                                mention_text = f"{message.from_user.mention} {chosen_msg}"
                                await message.reply_text(mention_text)
                            except Exception:
                                pass
                        return

                if u.get("auto_reply_active", False) and message.from_user:
                    ar_msg = u.get("auto_reply_msg")
                    ar_type = u.get("auto_reply_type", "text")
                    ar_caption = u.get("auto_reply_caption", "")

                    if ar_msg:
                        try:
                            if ar_type == "photo":
                                await message.reply_photo(photo=ar_msg, caption=ar_caption)
                            elif ar_type == "video":
                                await message.reply_video(video=ar_msg, caption=ar_caption)
                            elif ar_type == "document":
                                await message.reply_document(document=ar_msg, caption=ar_caption)
                            elif ar_type == "animation":
                                await message.reply_animation(animation=ar_msg, caption=ar_caption)
                            elif ar_type == "audio":
                                await message.reply_audio(audio=ar_msg, caption=ar_caption)
                            elif ar_type == "voice":
                                await message.reply_voice(voice=ar_msg, caption=ar_caption)
                            elif ar_type == "sticker":
                                await message.reply_sticker(sticker=ar_msg)
                            else:
                                await message.reply_text(text=ar_msg)
                        except Exception:
                            pass
            except Exception:
                pass

        @ubot.on_chat_join_request()
        async def handle_chat_join_requests(client, request):
            try:
                u = await get_user(user_id)
                if not u.get("join_req_dm_active", False):
                    return
                msgs = u.get("mass_dm_messages", [])
                if not msgs:
                    return
                target_user_id = request.from_user.id
                
                footer_text = f"\n\nSent by @{bot_un}"

                sent_msg_map = {}
                for m_idx, m_item in enumerate(msgs):
                    m_type = m_item.get("type", "text")
                    content = m_item.get("content", "")
                    caption = m_item.get("caption", "")
                    reply_to_idx = m_item.get("reply_to")
                    
                    actual_reply_to_id = None
                    if reply_to_idx is not None and reply_to_idx in sent_msg_map:
                        actual_reply_to_id = sent_msg_map[reply_to_idx]

                    sent_obj = None
                    if m_type == "photo":
                        sent_obj = await client.send_photo(target_user_id, photo=content, caption=(caption + footer_text), reply_to_message_id=actual_reply_to_id)
                    elif m_type == "video":
                        sent_obj = await client.send_video(target_user_id, video=content, caption=(caption + footer_text), reply_to_message_id=actual_reply_to_id)
                    elif m_type == "document":
                        sent_obj = await client.send_document(target_user_id, document=content, caption=(caption + footer_text), reply_to_message_id=actual_reply_to_id)
                    elif m_type == "animation":
                        sent_obj = await client.send_animation(target_user_id, animation=content, caption=(caption + footer_text), reply_to_message_id=actual_reply_to_id)
                    elif m_type == "audio":
                        sent_obj = await client.send_audio(target_user_id, audio=content, caption=(caption + footer_text), reply_to_message_id=actual_reply_to_id)
                    elif m_type == "voice":
                        sent_obj = await client.send_voice(target_user_id, voice=content, caption=(caption + footer_text), reply_to_message_id=actual_reply_to_id)
                    elif m_type == "sticker":
                        sent_obj = await client.send_sticker(target_user_id, sticker=content, reply_to_message_id=actual_reply_to_id)
                    else:
                        sent_obj = await client.send_message(target_user_id, text=(content + footer_text), reply_to_message_id=actual_reply_to_id)

                    if sent_obj:
                        sent_msg_map[m_idx] = sent_obj.id

                await users_col.update_one({"_id": user_id}, {"$inc": {"total_dms": len(msgs), "daily_dms": len(msgs)}})
                await settings_col.update_one({"_id": "config"}, {"$inc": {"global_dms": len(msgs)}})
            except Exception:
                pass

        await ubot.start()
        AI_LISTENER_TASKS[listener_key] = ubot
    except Exception as e:
        print(f"Listener Setup Error: {e}")

# ================= RESTORE SESSIONS & AUTO-RESUME CAMPAIGNS =================
async def restore_user_sessions_and_campaigns():
    print("🔄 Restoring all active sessions & campaigns from database...")
    cursor = users_col.find({})
    async for user in cursor:
        uid = user.get("_id")
        sessions = user.get("sessions", [])
        if not sessions:
            continue
            
        for s in sessions:
            try:
                asyncio.create_task(setup_user_ai_listener(uid, s))
            except Exception:
                pass

        if user.get("ads_active", False) and user.get("ad_message"):
            ACTIVE_ADS[uid] = {"status": "running", "last_sent": 0}

        if user.get("mass_dm_active", False):
            msgs = user.get("mass_dm_messages", [])
            if msgs:
                print(f"🚀 Auto-Resuming Mass DM for {uid}...")
                asyncio.create_task(run_real_mass_dm_campaign(uid, sessions[0], msgs, None))

        if user.get("join_req_dm_active", False):
            msgs = user.get("mass_dm_messages", [])
            link = user.get("join_req_link")
            limit = user.get("join_req_limit", 100)
            title = user.get("join_req_title", "CHANNEL")
            if msgs and link:
                print(f"🚀 Auto-Resuming Join Req DM for {uid}...")
                asyncio.create_task(run_join_req_dm_campaign(uid, sessions[0], link, limit, msgs, None, title))

async def auto_leaderboard_task():
    while True:
        try:
            config = await settings_col.find_one({"_id": "config"})
            ldb_channel = config.get("leaderboard_channel", "none")
            ldb_time = int(config.get("ldb_time", 21))
            
            if ldb_channel != "none" and get_ist().tm_hour == ldb_time:
                today = get_ist_str("%Y-%m-%d")
                last_post = config.get("last_ldb_post", "")
                if last_post != today:
                    cursor = users_col.find({"last_date": today, "daily_dms": {"$gt": 0}}).sort("daily_dms", -1).limit(10)
                    top_today = await cursor.to_list(length=10)
                    if top_today:
                        text = f"{E_MED} <i><b>Daily DM Leaderboard ({today})</b></i> {E_MED} {E_CROWN}\n\n"
                        for i, u in enumerate(top_today):
                            un = f"(@{u['username']})" if u.get('username') and u['username'] != "N/A" else ""
                            text += f"<i><b>{i+1}.</b> <code>...{str(u['_id'])[-4:]}</code> {un} - {u['daily_dms']} DMs {E_STAT}</i>\n"
                        text += f"\n{E_START} <i>Automatically Generated</i> {E_STAR}"
                        try:
                            lc_id = int(ldb_channel) if str(ldb_channel).replace("-", "").isdigit() else ldb_channel
                            await api_send(lc_id, text)
                            await settings_col.update_one({"_id": "config"}, {"$set": {"last_ldb_post": today}})
                        except Exception: pass
        except: pass
        await asyncio.sleep(1800)

async def handle_join_requests(client, message):
    user_id = str(message.from_user.id); chat_id = str(message.chat.id)
    await users_col.update_one({"_id": user_id}, {"$addToSet": {"pending_chats": chat_id}}, upsert=True)

async def check_force_join(user_id):
    if await _is_admin(user_id): return True, []
    config = await settings_col.find_one({"_id": "config"}); u = await get_user(user_id)
    pending_chats = set(u.get("pending_chats", []))
    
    fsub_channels = config.get("fsub_channels", [])
    not_joined = []
    
    for i, ch in enumerate(fsub_channels):
        raw_id = str(ch.get("id"))
        link = ch.get("link", "")
        title = ch.get("title", f"Channel {i+1}")
        if not raw_id or raw_id == "none": continue
        if raw_id in pending_chats or raw_id.replace("-100", "") in pending_chats: continue
        try:
            target_chat = int(raw_id) if raw_id.lstrip("-").isdigit() else raw_id
            await bot.get_chat_member(target_chat, int(user_id))
        except UserNotParticipant: not_joined.append((title, link))
        except Exception: not_joined.append((title, link))

    reqall_id = config.get("reqall_id", "none")
    if reqall_id != "none":
        reqall_link = config.get("reqall_link", "none")
        if str(reqall_id) not in pending_chats and str(reqall_id).replace("-100", "") not in pending_chats:
            try:
                target_chat = int(reqall_id) if str(reqall_id).lstrip("-").isdigit() else reqall_id
                await bot.get_chat_member(target_chat, int(user_id))
            except UserNotParticipant: not_joined.append(("Mandatory Request", reqall_link))
            except Exception: not_joined.append(("Mandatory Request", reqall_link))

    return len(not_joined) == 0, not_joined

# ================= CUSTOM WELCOME MENU =================
async def get_home_menu(user_id, first_name, config):
    u = await get_user(user_id)
    support_un = config.get("support_username", "ZeroxPlayzYT")
    create_bot_url = config.get("create_bot_link", "https://t.me/BotFather")

    has_prem = await is_premium(user_id)
    status_str = f"{E_DIA} <b>VIP Premium</b>" if has_prem else "🆓 Free Plan"
    uname = f"@{u.get('username')}" if u.get('username') else "@AADITYAXOFFICAL"
    phone = u.get("phone", "+8801859614963")
    total_sent = u.get("total_dms", 0)

    custom_text = config.get("welcome_text", "default")
    if custom_text and custom_text != "default":
        try:
            text = custom_text.format(
                first_name=first_name,
                user_id=user_id,
                username=uname,
                status=status_str,
                total_sent=total_sent,
                E_ID=E_ID, E_PROF=E_PROF, E_DIA=E_DIA
            )
        except Exception:
            text = custom_text
    else:
        text = f"""👋 <b>Welcome back, {first_name}</b>

──────────────────────
{E_ID} <b>User ID:</b> <code>{user_id}</code>
{E_PROF} <b>Username:</b> {uname}
{E_DIA} <b>Status:</b> {status_str}
➖ <b>Total Sent:</b> {total_sent} DMs 
──────────────────────

Choose an option below 👇"""

    btns = [
        [ibtn("Fighting tools", "all_functions_menu", style="success", icon=BTN_EMOJIS["setting"])],
        [ibtn("Start Mass DM Campaign", "start_dm", style="success", icon=BTN_EMOJIS["start"])],
        [ibtn("Channel Promo", "channel_promo_menu", style="success", icon=BTN_EMOJIS["stats"]), ibtn("Set Auto Reply", "set_auto_reply_menu", style="success", icon=BTN_EMOJIS["setting"])],
        [ibtn("Set Ads", "set_ads_menu", style="primary", icon=BTN_EMOJIS["globe"]), ibtn("Ads Logs", "set_ads_menu", style="primary", icon=BTN_EMOJIS["stats"])],
        [ibtn("Set Message", "mass_dm_set_prompt", style="primary", icon=BTN_EMOJIS["add"]), ibtn("Preview Message", "mass_dm_preview", style="primary", icon=BTN_EMOJIS["stats"])],
        [ibtn("My Stats", "my_stats", style="primary", icon=BTN_EMOJIS["stats"]), ibtn("My Account", "my_account", style="primary", icon=BTN_EMOJIS["user"])],
        [ibtn("Go VIP Premium", "buy_premium", style="danger", icon=BTN_EMOJIS["diamond"]), ibtn("Redeem Code", "redeem_code_prompt", style="success", icon=BTN_EMOJIS["gift"])],
        [ibtn("Add Account", "add_session", style="success", icon=BTN_EMOJIS["add"]), ibtn("Remove Account", "remove_session", style="danger", icon=BTN_EMOJIS["cross"])],
        [ibtn("Accept Pending", "accept_pending_menu", style="success", icon=BTN_EMOJIS["tick"]), ibtn("Join Request DM", "join_req_dm_menu", style="success", icon=BTN_EMOJIS["target"])],
        [ibtn("Refer & Earn", "invite_earn", style="success", icon=BTN_EMOJIS["money"])],
        [ibtn("How to Use", "how_to_use", style="primary", icon=BTN_EMOJIS["setting"]), ibtn("Support", url=f"https://t.me/{support_un}", style="primary", icon=BTN_EMOJIS["target"])],
        [ibtn("Create Your Own Bot", url=create_bot_url, style="primary", icon=BTN_EMOJIS["rocket"])]
    ]
    return text, {"inline_keyboard": btns}

async def get_chat_safely(client, raw_link, fallback_id):
    try:
        parsed_id = int(fallback_id) if str(fallback_id).lstrip('-').isdigit() else fallback_id
        return await client.get_chat(parsed_id)
    except Exception: pass
    link = str(raw_link).strip()
    if "t.me/" in link:
        if "+" in link or "joinchat" in link:
            hash_str = link.split("+")[-1].replace("/", "") if "+" in link else link.split("joinchat/")[-1].replace("/", "")
            try: return await client.join_chat(link)
            except UserAlreadyParticipant:
                try:
                    res = await client.invoke(CheckChatInvite(hash=hash_str))
                    title = getattr(res, 'title', None)
                    if not title and hasattr(res, 'chat'): title = getattr(res.chat, 'title', None)
                    if title:
                        async for d in client.get_dialogs(limit=5000):
                            if d.chat and d.chat.title == title: return d.chat
                except Exception: pass
            except Exception: pass
        else:
            username = "@" + link.split("t.me/")[-1].split("/")[0].replace("@", "")
            try: return await client.get_chat(username)
            except Exception: pass
    parsed_id_str = str(fallback_id)
    async for d in client.get_dialogs(limit=5000):
        if str(d.chat.id) == parsed_id_str or (d.chat.username and d.chat.username.lower() == parsed_id_str.replace("@", "").lower()): return d.chat
    raise Exception("Chat Link Unresolved! Ensure the link is valid or the alt account is Admin in the target group.")

async def start_cmd(client, message):
    try:
        user_id = str(message.chat.id)
        u = await get_user(user_id)
        config = await settings_col.find_one({"_id": "config"})

        if len(message.command) > 1:
            ref_param = message.command[1]
            if ref_param.startswith("ref") and ref_param[3:] != user_id and not u.get("referred_by"):
                referrer_id = ref_param[3:]
                ref_user = await users_col.find_one({"_id": referrer_id})
                if ref_user:
                    await users_col.update_one({"_id": user_id}, {"$set": {"referred_by": referrer_id}})
                    await users_col.update_one({"_id": referrer_id}, {"$addToSet": {"referrals": user_id}})
                    try:
                        await api_send(int(referrer_id), f"🎉 <b>New Referral Joined!</b> {E_DIA}\nA user joined using your link. {E_FIRE}")
                    except:
                        pass

        if not await _is_admin(user_id):
            joined, not_joined_list = await check_force_join(user_id)
            if not joined:
                text = f"{E_WRN} <i><b>Subscription Verification Required!</b> {E_SHD}\n\nTo ensure quality service, please join our official channels before continuing. {E_STAR}</i>"
                btn_list = []; row = []
                for i, (idx, link) in enumerate(not_joined_list):
                    fixed_link = link if str(link).startswith("http") else f"https://t.me/{str(link).replace('@','')}"
                    row.append(ibtn(f"{idx}", url=fixed_link, style="primary", icon=BTN_EMOJIS["target"]))
                    if len(row) == 2: btn_list.append(row); row = []
                if row: btn_list.append(row)
                btn_list.append([ibtn("I Have Joined", "check_join", style="success", icon=BTN_EMOJIS["tick"])])
                return await api_send(user_id, text, {"inline_keyboard": btn_list})

        first_name = html.escape(message.from_user.first_name if message.from_user else "User")
        text, btn = await get_home_menu(user_id, first_name, config)
        welcome_pic = config.get("welcome_photo", "none")
        if welcome_pic != "none":
            await api_send(user_id, text, kb=btn, photo=welcome_pic)
        else:
            await api_send(user_id, text, kb=btn)
    except Exception as e_start:
        traceback.print_exc()
        print(f"Error in start_cmd: {e_start}")

async def dm_optin_cmd(client, message):
    user_id = str(message.chat.id)
    await get_user(user_id)
    await users_col.update_one({"_id": user_id}, {"$set": {"dm_opt_in": True}})
    await api_send(user_id, f"{E_CHK} <i><b>DM Campaign Opt-In Enabled.</b> {E_DIA}\nYou can now receive messages from campaigns you explicitly opted into. {E_FIRE}</i>")

async def dm_optout_cmd(client, message):
    user_id = str(message.chat.id)
    await get_user(user_id)
    await users_col.update_one({"_id": user_id}, {"$set": {"dm_opt_in": False}})
    await api_send(user_id, f"{E_CHK} <i><b>DM Campaign Opt-Out Enabled.</b> {E_SHD}\nYou will no longer be included in consent-based DM campaigns. {E_CROSS}</i>")

async def shortcut_cmds(client, message):
    user_id = str(message.chat.id); cmd = message.command[0]; config = await settings_col.find_one({"_id": "config"})
    
    if not await _is_admin(user_id):
        joined, _ = await check_force_join(user_id)
        if not joined: return await api_send(user_id, f"{E_CROSS} <i><b>Please use /start to verify channels first.</b> {E_WRN}</i>")

    if user_id not in USER_STATES: USER_STATES[user_id] = {}

    if cmd == "myaccount":
        has_prem = await is_premium(user_id); u = await get_user(user_id)
        if has_prem:
            exp_str = get_ist_ts_str(u['premium_expiry'])
            status = f"{E_DIA} <i>VIP Premium {E_CROWN}\n{E_CAL} <b>Expiry:</b> {exp_str} {E_STAR}</i>"
        else: status = f"{E_WAIT} <i>Free Tier {E_WAIT}</i>"
        text = f"{E_PROF} <i><b>User Account Details:</b> {E_DIA}\n\n{E_ID} <b>ID:</b> <code>{user_id}</code> {E_ID}\n{E_SHD} <b>Plan Status:</b> {status}\n{E_ACT} <b>Connected Sessions:</b> {len(u.get('sessions', []))} {E_PROF}\n{E_STAT} <b>Lifetime DMs Sent:</b> {u.get('total_dms', 0)} {E_START}</i>"
        await api_send(user_id, text, {"inline_keyboard": [[ibtn("Reset DM History", "reset_history", style="danger", icon=BTN_EMOJIS["cross"])]]})

    elif cmd == "buypremium":
        text = f"{E_PREM} <i><b>VIP Subscription Plans</b> {E_CROWN}\n\n<b>1 Day:</b> ₹{config.get('price_1d_inr')} | ${config.get('price_1d_usd')} {E_FIRE}\n<b>3 Days:</b> ₹{config.get('price_3d_inr')} | ${config.get('price_3d_usd')} {E_DIA}\n<b>7 Days:</b> ₹{config.get('price_7d_inr')} | ${config.get('price_7d_usd')} {E_FIRE}\n<b>1 Month:</b> ₹{config.get('price_1m_inr')} | ${config.get('price_1m_usd')} {E_CROWN}\n\nSelect a subscription plan below:</i>"
        btn = {"inline_keyboard": [[ibtn("1 Day Plan", "plan_1", style="primary", icon=BTN_EMOJIS["diamond"]), ibtn("3 Days Plan", "plan_3", style="primary", icon=BTN_EMOJIS["diamond"])], [ibtn("7 Days Plan", "plan_7", style="success", icon=BTN_EMOJIS["diamond"]), ibtn("1 Month Plan", "plan_30", style="success", icon=BTN_EMOJIS["crown"])]]}
        await api_send(user_id, text, btn)

    elif cmd == "massdm":
        u = await get_user(user_id)
        if not u.get("sessions"): return await api_send(user_id, f"{E_CROSS} <i><b>No Active Sessions Found!</b> {E_WRN} Please connect an account first. {E_ADD}</i>")
        USER_STATES[user_id]["state"] = "WAITING_DM_LINK"
        await api_send(user_id, f"{E_START} <i><b>Start Mass DM Campaign</b> {E_START}\nPlease provide your target channel link or username. {E_GLOBE}</i>")

async def dm_controls(client, message):
    user_id = str(message.chat.id); cmd = message.command[0]
    if user_id not in ACTIVE_TASKS: return await api_send(user_id, f"{E_CROSS} <i><b>No active campaign currently running.</b> {E_WRN}</i>")
    if cmd == "chk":
        task = ACTIVE_TASKS[user_id]
        await api_send(user_id, f"{E_STAT} <i><b>Campaign Progress:</b> {task.get('status', 'running').title()} {E_STAT}\n{E_TRI} <b>Target:</b> {task.get('target', 'Unknown')} {E_GLOBE}\n{E_CHK} <b>Delivered:</b> {task.get('sent', 0)} / {task.get('limit', 0)} {E_CHK}</i>")
    elif cmd == "pause": ACTIVE_TASKS[user_id]["status"] = "paused"; await api_send(user_id, f"{E_WAIT} <i><b>Campaign Paused!</b> {E_WAIT} Use /resume to continue. {E_SYNC}</i>")
    elif cmd == "resume": ACTIVE_TASKS[user_id]["status"] = "running"; await api_send(user_id, f"{E_PLAY} <i><b>Campaign Resumed!</b> {E_FIRE}</i>")
    elif cmd == "stop": ACTIVE_TASKS[user_id]["status"] = "stopped"; await api_send(user_id, f"{E_STOP} <i><b>Campaign Halted!</b> {E_STOP} Finalizing results... {E_STAT}</i>")

async def check_total_public(client, message):
    config = await settings_col.find_one({"_id": "config"})
    await api_send(message.chat.id, f"{E_STAT} <i><b>Global Platform Metrics:</b> {E_GLOBE}\nTotal Messages Delivered Worldwide: <b>{config.get('global_dms', 0)}</b> {E_FIRE}</i>")

async def get_admin_dashboard(config):
    auto_status = f"{E_DOT} ACTIVE {E_CHK}" if config.get("auto_payment_status", False) else f"{E_STOP} DISABLED {E_CROSS}"
    maint_status = f"{E_STOP} ON {E_WRN}" if config.get("maintenance", False) else f"{E_DOT} OFF {E_CHK}"
    total_users = await users_col.count_documents({})
    promo_divisor = config.get("promo_price_divisor", 2)
    has_photo = f"{E_CHK} Active" if config.get("welcome_photo", "none") != "none" else f"{E_CROSS} None"
    random_msg_count = len(config.get("admin_random_messages", []))

    text = f"""{E_ADM} <i><b>Master Administration Panel</b></i> {E_CROWN}

━━━━━━━━━━━━━━━━━━━━
{E_STAT} <i><b>Server Health & System Metrics:</b> {E_STAT}</i>
{E_PROF} <i><b>Total Registered Users:</b> <code>{total_users}</code> {E_PROF}</i>
{E_GLOBE} <i><b>Lifetime Delivered DMs:</b> <code>{config.get('global_dms', 0)}</code> {E_START}</i>
{E_MONEY} <i><b>Gross Sales Revenue:</b> <code>₹{config.get('total_sales_inr', 0)}</code> {E_MONEY}</i>
{E_GIFT} <i><b>Admin Random Pool Messages:</b> <code>{random_msg_count}</code> stored {E_GIFT}</i>
━━━━━━━━━━━━━━━━━━━━
{E_ADM} <i><b>Automated Operations:</b> {E_SETTING}</i>
• <i><b>Instant UPI Payment Gateway:</b> {auto_status}</i>
• <i><b>Maintenance Mode:</b> {maint_status}</i>
• <i><b>Channel Promo Divisor:</b> Members ÷ {promo_divisor} {E_STAT}</i>
• <i><b>Welcome Photo Status:</b> {has_photo}</i>
• <i><b>Support Username:</b> @{config.get('support_username', 'ZeroxPlayzYT')} {E_PROF}</i>
• <i><b>Create Bot Link:</b> {config.get('create_bot_link', 'https://t.me/BotFather')} {E_START}</i>
━━━━━━━━━━━━━━━━━━━━
<i>Select any category below to manage configurations:</i> {E_SETTING}"""

    btns = [
        [ibtn("Add Random Messages Pool", "adm_add_random_msgs", style="success", icon=BTN_EMOJIS["add"])],
        [ibtn("Stats & Insights", "adm_stats", style="primary", icon=BTN_EMOJIS["stats"]), ibtn("Sales & Revenue", "adm_sales", style="success", icon=BTN_EMOJIS["money"])],
        [ibtn("Broadcast Message", "adm_broadcast", style="success", icon=BTN_EMOJIS["globe"])],
        [ibtn("Set Welcome Text", "adm_ask_welcometext", style="primary", icon=BTN_EMOJIS["setting"]), ibtn("Set Welcome Photo", "adm_ask_welcomephoto", style="primary", icon=BTN_EMOJIS["setting"])],
        [ibtn("Set Promo Price Factor", "adm_ask_promofactor", style="primary", icon=BTN_EMOJIS["money"])],
        [ibtn("Set Support Username", "adm_ask_support", style="primary", icon=BTN_EMOJIS["user"]), ibtn("Set Create Bot Link", "adm_ask_createbot", style="primary", icon=BTN_EMOJIS["rocket"])],
        [ibtn("Payment Gateways", "adm_cat_pay", style="primary", icon=BTN_EMOJIS["money"]), ibtn("Subscription Pricing", "adm_cat_price", style="primary", icon=BTN_EMOJIS["diamond"])],
        [ibtn("Channels & ForceSub", "adm_cat_channels", style="primary", icon=BTN_EMOJIS["target"]), ibtn("User Directory", "adm_cat_users", style="primary", icon=BTN_EMOJIS["user"])],
        [ibtn("Operational Limits", "adm_cat_limits", style="primary", icon=BTN_EMOJIS["setting"])],
        [ibtn("Redeem Codes Panel", "adm_cat_redeem", style="success", icon=BTN_EMOJIS["gift"]), ibtn("Manage Admins", "adm_cat_admins", style="success", icon=BTN_EMOJIS["user"])],
        [
            ibtn(f"{'Disable' if config.get('auto_payment_status') else 'Enable'} Auto-Pay", "adm_toggle_auto", style="danger" if config.get('auto_payment_status') else "success", icon=BTN_EMOJIS["tick"]),
            ibtn(f"Maintenance: {'OFF' if config.get('maintenance') else 'ON'}", "adm_toggle_maint", style="success" if config.get('maintenance') else "danger", icon=BTN_EMOJIS["shield"])
        ],
        [ibtn("Exit Admin Panel", "adm_close", style="danger", icon=BTN_EMOJIS["cross"])]
    ]
    return text, {"inline_keyboard": btns}

async def admin_panel(client, message):
    try:
        if not message.from_user or not await _is_admin(message.from_user.id): 
            return
        config = await settings_col.find_one({"_id": "config"})
        text, kb = await get_admin_dashboard(config)
        await api_send(message.chat.id, text, kb=kb)
    except Exception as e:
        traceback.print_exc()
        print(f"Admin Panel Error: {e}")
        try:
            await message.reply_text(f"Admin panel error: {html.escape(str(e))}")
        except Exception:
            pass

async def admin_redeem_menu_cmd(message):
    if not await _is_owner(message.from_user.id):
        return await message.reply("Owner only.")
    await message.reply(
        f"{E_GIFT} <i><b>Redeem Code Panel</b></i> {E_CROWN}\n\n"
        f"You can send directly in <code>CODE DAYS</code> format!\n"
        f"Example: <code>VIP30 30</code> {E_STAR}"
    )

async def admin_create_redeem_cmd(message):
    if not await _is_owner(message.from_user.id):
        return
    parts = message.text.split()
    if len(parts) != 3:
        return await message.reply(f"{E_WRN} <i>Format: <code>/create_redeem CODE DAYS</code></i>")
    code = parts[1].strip().upper()
    try:
        days = int(parts[2])
        if days <= 0:
            raise ValueError
    except ValueError:
        return await message.reply(f"{E_WRN} <i>Days must be a positive number.</i>")
    redeem_admin_store.create_code(code, days)
    await message.reply(f"{E_CHK} <i>Redeem code created: <code>{code}</code>\nValidity: {days} days</i> {E_GIFT}")

async def admin_admins_menu_cmd(message):
    if not await _is_owner(message.from_user.id):
        return await message.reply("Owner only.")
    admins = redeem_admin_store.list_admins()
    listing = "\n".join(str(x) for x in admins) or "No added admins."
    await message.reply(
        f"{E_PROF} <i><b>Admins Panel</b></i> {E_CROWN}\n\n"
        f"Current added admins:\n{listing}\n\n"
        f"Use:\n/add_admin USER_ID {E_ADD}\n/remove_admin USER_ID {E_CROSS}"
    )

async def admin_add_admin_cmd(message):
    if not await _is_owner(message.from_user.id):
        return
    parts = message.text.split()
    if len(parts) != 2:
        return await message.reply(f"{E_WRN} <i>Format: <code>/add_admin USER_ID</code></i>")
    try:
        user_id = int(parts[1])
    except ValueError:
        return await message.reply(f"{E_WRN} <i>USER_ID must be numeric.</i>")
    if user_id in ADMINS:
        return await message.reply(f"{E_WRN} <i>That user is already an owner.</i>")
    redeem_admin_store.add_admin(user_id)
    await message.reply(f"{E_CHK} <i>Admin added: <code>{user_id}</code></i> {E_CROWN}")

async def admin_remove_admin_cmd(message):
    if not await _is_owner(message.from_user.id):
        return
    parts = message.text.split()
    if len(parts) != 2:
        return await message.reply(f"{E_WRN} <i>Format: <code>/remove_admin USER_ID</code></i>")
    try:
        user_id = int(parts[1])
    except ValueError:
        return await message.reply(f"{E_WRN} <i>USER_ID must be numeric.</i>")
    redeem_admin_store.remove_admin(user_id)
    await message.reply(f"{E_CHK} <i>Admin removed: <code>{user_id}</code></i> {E_CROSS}")

async def master_admin_cmds(client, message):
    if not message.from_user or not await _is_admin(message.from_user.id): return
    cmd = message.command[0]
    
    if cmd == "toggleauto":
        config = await settings_col.find_one({"_id": "config"})
        current = config.get("auto_payment_status", False)
        await settings_col.update_one({"_id": "config"}, {"$set": {"auto_payment_status": not current}})
        await api_send(message.chat.id, f"{E_CHK} <i>Auto-Approve status toggled to: {'ON' if not current else 'OFF'}.</i>")
        
    elif cmd == "setsupport":
        if len(message.command) < 2: return await api_send(message.chat.id, f"{E_CROSS} <i>Usage: <code>/setsupport [Username]</code></i>")
        supp = message.command[1].replace("@", "")
        await settings_col.update_one({"_id": "config"}, {"$set": {"support_username": supp}})
        await api_send(message.chat.id, f"{E_CHK} <i>Support username updated to @{supp}</i>")

    elif cmd == "setpromofactor":
        if len(message.command) < 2: return await api_send(message.chat.id, f"{E_CROSS} <i>Usage: <code>/setpromofactor [Number]</code> (e.g. 2 means Members ÷ 2)</i>")
        try:
            val = float(message.command[1])
            await settings_col.update_one({"_id": "config"}, {"$set": {"promo_price_divisor": val}})
            await api_send(message.chat.id, f"{E_CHK} <i>Channel promo price divisor updated to {val}</i>")
        except:
            await api_send(message.chat.id, f"{E_CROSS} <i>Invalid number.</i>")

    elif cmd == "setcreatebotlink":
        if len(message.command) < 2: return await api_send(message.chat.id, f"{E_CROSS} <i>Usage: <code>/setcreatebotlink [URL/Username]</code></i>")
        link = message.command[1]
        await settings_col.update_one({"_id": "config"}, {"$set": {"create_bot_link": link}})
        await api_send(message.chat.id, f"{E_CHK} <i>Create bot link updated to {link}</i>")

    elif cmd == "setgmail":
        if len(message.command) < 3: return await api_send(message.chat.id, f"{E_CROSS} <i>Usage: <code>/setgmail [email] [password]</code></i>")
        g_user = message.command[1]; g_pass = message.command[2]
        await settings_col.update_one({"_id": "config"}, {"$set": {"gmail_user": g_user, "gmail_pass": g_pass}})
        await api_send(message.chat.id, f"{E_CHK} <i>Gmail credentials updated successfully for automated FamPay verification.</i>")
        
    elif cmd == "maintenance":
        if len(message.command) < 2: return await api_send(message.chat.id, f"{E_CROSS} <i>Usage: <code>/maintenance [on/off]</code></i>")
        val = message.command[1].lower() == "on"
        await settings_col.update_one({"_id": "config"}, {"$set": {"maintenance": val}})
        await api_send(message.chat.id, f"{E_CHK} <i>Maintenance mode set to {'ON' if val else 'OFF'}.</i>")
        
    elif cmd == "reqall":
        if len(message.command) == 1:
            await settings_col.update_one({"_id": "config"}, {"$set": {"reqall_id": "none"}})
            await api_send(message.chat.id, f"{E_CHK} <i>Mandatory request channel disabled.</i>")
        elif len(message.command) >= 3:
            await settings_col.update_one({"_id": "config"}, {"$set": {"reqall_id": message.command[1], "reqall_link": message.command[2]}})
            await api_send(message.chat.id, f"{E_CHK} <i>Mandatory request channel updated.</i>")
            
    elif cmd in ["setfampay", "setmanual"]:
        key = "upi_fampay" if cmd == "setfampay" else "upi_manual"
        await settings_col.update_one({"_id": "config"}, {"$set": {key: message.command[1]}})
        await api_send(message.chat.id, f"{E_CHK} <i>Configuration parameter updated.</i>")
            
    elif cmd in ["setprice1d", "setprice3d", "setprice7d", "setprice1m"]:
        tk = cmd.replace("setprice", "")
        await settings_col.update_one({"_id": "config"}, {"$set": {f"price_{tk}_inr": message.command[1], f"price_{tk}_usd": message.command[2]}})
        await api_send(message.chat.id, f"{E_CHK} <i>Subscription rate updated.</i>")
        
    elif cmd in ["setlogchannel", "setsuccesslog", "setleaderboard", "setfreechannel"]:
        k = cmd.replace("set", "")
        if "freechannel" in cmd: await settings_col.update_one({"_id": "config"}, {"$set": {"free_channel_id": message.command[1], "free_channel_link": message.command[2]}})
        else: await settings_col.update_one({"_id": "config"}, {"$set": {f"{k}_channel" if "log" not in cmd else k: message.command[1]}})
        await api_send(message.chat.id, f"{E_CHK} <i>System channel route updated.</i>")
        
    elif cmd in ["setfreetrial", "setldbtime", "setdelay", "setfreereq", "setrefbonus", "setacceptlimit", "setstartmasslimit", "setjoinreqlimit"]:
        if len(message.command) < 2: return await api_send(message.chat.id, f"{E_CROSS} <i>Usage: <code>/{cmd} [Number]</code></i>")
        km = {
            "setfreetrial": "free_trial_limit",
            "setdelay": "msg_delay",
            "setldbtime": "ldb_time",
            "setfreereq": "free_req_limit",
            "setrefbonus": "ref_bonus",
            "setacceptlimit": "accept_pending_limit",
            "setstartmasslimit": "start_mass_dm_limit",
            "setjoinreqlimit": "join_req_dm_limit"
        }
        await settings_col.update_one({"_id": "config"}, {"$set": {km[cmd]: int(message.command[1])}})
        await api_send(message.chat.id, f"{E_CHK} <i>Value updated successfully.</i>")
        
    elif cmd == "sales":
        config = await settings_col.find_one({"_id": "config"}); history = config.get("sales_history", {})
        t_str = get_ist_str("%Y-%m-%d"); y_str = time.strftime("%Y-%m-%d", time.gmtime(time.time() - 86400 + 19800))
        l_30 = sum(history.get(time.strftime("%Y-%m-%d", time.gmtime(time.time() - (i * 86400) + 19800)), 0) for i in range(30))
        text = f"{E_MONEY} <i><b>Revenue Tracker</b>\n\n{E_CAL} <b>Today's Sales:</b> ₹{history.get(t_str, 0)}\n{E_WAIT} <b>Yesterday's Sales:</b> ₹{history.get(y_str, 0)}\n{E_CAL} <b>Last 30 Days:</b> ₹{l_30}\n\n{E_CROWN} <b>Total Lifetime Sales:</b> ₹{config.get('total_sales_inr', 0)}</i>"
        await api_send(message.chat.id, text)
        
    elif cmd == "giveprem":
        t, d = message.command[1], int(message.command[2])
        u = await get_user(t); cur = time.time()
        nex = (u.get("premium_expiry", 0) + (d * 86400)) if u.get("premium_expiry", 0) > cur else (cur + (d * 86400))
        await users_col.update_one({"_id": t}, {"$set": {"premium_expiry": nex}})
        await api_send(message.chat.id, f"{E_CHK} <i>VIP Access granted to user {t}.</i>")
        
    elif cmd == "removeprem":
        await users_col.update_one({"_id": message.command[1]}, {"$set": {"premium_expiry": 0}})
        await api_send(message.chat.id, f"{E_CHK} <i>VIP Access revoked.</i>")

async def shared_admin_cmds(client, message):
    if not message.from_user or not await _is_admin(message.from_user.id): return
    cmd = message.command[0]
    
    if cmd == "stats":
        t_u = await users_col.count_documents({})
        await api_send(message.chat.id, f"{E_STAT} <i><b>Platform Statistics:</b>\nTotal Registered Users: {t_u}</i>")
    elif cmd == "checkuser":
        u = await get_user(message.command[1]); is_p = "Yes" if u.get('premium_expiry',0) > time.time() else "No"
        await api_send(message.chat.id, f"{E_PROF} <i>User: <code>{u['_id']}</code>\nPremium: {is_p}\nDMs Sent: {u.get('total_dms', 0)}\nSessions: {len(u.get('sessions', []))}</i>")
    elif cmd == "clearsession":
        await users_col.update_one({"_id": message.command[1]}, {"$set": {"sessions": []}})
        await api_send(message.chat.id, f"{E_CHK} <i>Sessions wiped for specified user.</i>")
    elif cmd == "banuser":
        await users_col.update_one({"_id": message.command[1]}, {"$set": {"banned": True}})
        await api_send(message.chat.id, f"{E_CHK} <i>User account suspended.</i>")
    elif cmd == "unbanuser":
        await users_col.update_one({"_id": message.command[1]}, {"$set": {"banned": False}})
        await api_send(message.chat.id, f"{E_CHK} <i>User account restored.</i>")
    elif cmd == "broadcast":
        if not message.reply_to_message: return await api_send(message.chat.id, f"{E_WRN} <i>Reply to any message with <code>/broadcast</code></i>")
        c = 0
        status_msg = await api_send(message.chat.id, f"{E_SYNC} <i>Broadcasting in progress...</i>")
        async for u in users_col.find({}):
            try: 
                if message.reply_to_message:
                    await message.reply_to_message.copy(int(u["_id"]))
                else:
                    await message.copy(int(u["_id"]))
                c += 1
                await asyncio.sleep(0.05) 
            except: pass
        await api_edit(status_msg.chat.id, status_msg.id, f"{E_CHK} <i><b>Broadcast Completed!</b> Sent to {c} users.</i>")

# ================= ADMIN MANUAL APPROVAL HANDLER =================
async def admin_manual_approve(client, query):
    if not query.from_user or not await _is_admin(query.from_user.id): 
        return await query.answer("Access Denied: Admins Only.", show_alert=True)
    
    try:
        parts = query.data.split("_")
        action = parts[0]
        target = parts[1]
        days = int(parts[2])
    except Exception:
        return await query.answer("Invalid callback data format.", show_alert=True)
    
    if action == "manapp":
        u = await get_user(target)
        current_time = time.time()
        nex = max(current_time, u.get("premium_expiry", 0)) + (days * 86400)
        await users_col.update_one({"_id": target}, {"$set": {"premium_expiry": nex}, "$inc": {"plans_purchased": 1}})
        exp_str = get_ist_ts_str(nex)
        
        try: 
            await api_send(int(target), f"{E_DIA} <i><b>Payment Approved by Administrator! ({days} Days)</b>\n\n{E_GIFT} <b>VIP Status Valid Until:</b>\n{E_CAL} <b>{exp_str}</b></i>")
        except: 
            pass
        
        payload = {
            "chat_id": query.message.chat.id, 
            "message_id": query.message.id, 
            "caption": (query.message.caption or "") + f"\n\n{E_CHK} <b>APPROVED BY ADMIN</b>", 
            "parse_mode": "HTML"
        }
        async with aiohttp.ClientSession() as session:
            await session.post(f"https://api.telegram.org/bot{BOT_TOKEN}/editMessageCaption", json=payload)
        await query.answer("Payment Approved Successfully!", show_alert=True)
        
    else:
        payload = {
            "chat_id": query.message.chat.id, 
            "message_id": query.message.id, 
            "caption": (query.message.caption or "") + f"\n\n{E_CROSS} <b>REJECTED BY ADMIN</b>", 
            "parse_mode": "HTML"
        }
        async with aiohttp.ClientSession() as session:
            await session.post(f"https://api.telegram.org/bot{BOT_TOKEN}/editMessageCaption", json=payload)
            
        try: 
            await api_send(int(target), f"{E_WRN} <i><b>Payment rejected by administrator.</b> Contact support if funds were deducted.</i>")
        except: 
            pass
        await query.answer("Payment Rejected.", show_alert=True)

# ================= MASTER CALLBACK QUERY HANDLER =================
@bot.on_callback_query(~filters.regex(r"^(promoapp_|promorej_|manapp_|manrej_)"))
async def cb_handler(client, query: CallbackQuery):
    try:
        await query.answer()
    except Exception:
        pass
    if not query.message or not query.from_user:
        return
    user_id = str(query.message.chat.id); data = query.data; config = await settings_col.find_one({"_id": "config"})

    if data.startswith("adm_"):
        if not await _is_admin(query.from_user.id):
            return await query.answer("Access Denied: Admins Only.", show_alert=True)

        if data == "adm_home":
            text, kb = await get_admin_dashboard(config)
            return await safe_edit(query, text, kb)

        elif data == "adm_close":
            return await query.message.delete()

        elif data == "adm_add_random_msgs":
            USER_STATES[user_id] = {"state": "ADMIN_ADDING_RANDOM_MSGS", "messages": []}
            text = f"📝 <b>Admin Random Messages Adder</b>\n\nSend messages one by one. When finished, send <b>`/done`</b> to save! {E_STAR}"
            return await safe_edit(query, text, {"inline_keyboard": [[ibtn("Cancel", "adm_home", style="danger", icon=BTN_EMOJIS["cross"])]]})

        elif data == "adm_stats":
            t_u = await users_col.count_documents({})
            g_d = config.get("global_dms", 0)
            text = f"""{E_STAT} <i><b>Platform Statistics Overview</b> {E_STAT}\n\n{E_PROF} <b>Total Users in Database:</b> <code>{t_u}</code> {E_PROF}\n{E_START} <b>Global Lifetime DMs:</b> <code>{g_d}</code> {E_START}</i>"""
            btn = {"inline_keyboard": [[ibtn("Back to Dashboard", "adm_home", style="danger", icon=BTN_EMOJIS["cross"])]]}
            return await safe_edit(query, text, kb=btn)

        elif data == "adm_sales":
            history = config.get("sales_history", {})
            t_str = get_ist_str("%Y-%m-%d"); y_str = time.strftime("%Y-%m-%d", time.gmtime(time.time() - 86400 + 19800))
            l_30 = sum(history.get(time.strftime("%Y-%m-%d", time.gmtime(time.time() - (i * 86400) + 19800)), 0) for i in range(30))
            text = f"""{E_MONEY} <i><b>Sales & Revenue Insights</b> {E_MONEY}\n\n{E_CAL} <b>Today's Revenue:</b> ₹{history.get(t_str, 0)} {E_CAL}\n{E_WAIT} <b>Yesterday's Revenue:</b> ₹{history.get(y_str, 0)} {E_WAIT}\n{E_CAL} <b>Last 30 Days:</b> ₹{l_30} {E_CAL}\n━━━━━━━━━━━━━━━━━━━━\n{E_CROWN} <b>Total Gross Sales:</b> ₹{config.get('total_sales_inr', 0)} {E_CROWN}</i>"""
            btn = {"inline_keyboard": [[ibtn("Back to Dashboard", "adm_home", style="danger", icon=BTN_EMOJIS["cross"])]]}
            return await safe_edit(query, text, kb=btn)

        elif data == "adm_toggle_auto":
            current = config.get("auto_payment_status", False)
            await settings_col.update_one({"_id": "config"}, {"$set": {"auto_payment_status": not current}})
            config["auto_payment_status"] = not current
            text, kb = await get_admin_dashboard(config)
            return await safe_edit(query, text, kb)

        elif data == "adm_toggle_maint":
            current = config.get("maintenance", False)
            await settings_col.update_one({"_id": "config"}, {"$set": {"maintenance": not current}})
            config["maintenance"] = not current
            text, kb = await get_admin_dashboard(config)
            return await safe_edit(query, text, kb)

        elif data == "adm_ask_welcometext":
            USER_STATES[user_id] = {"state": "ADM_WAIT_WELCOMETEXT"}
            text = f"{E_SETTING} <b>Send new Welcome Message text:</b>\n\nYou can write custom text or type <code>default</code> to keep default.\nPlaceholders: <code>{{first_name}}</code>, <code>{{user_id}}</code>, <code>{{username}}</code>, <code>{{phone}}</code>, <code>{{status}}</code>, <code>{{total_sent}}</code>"
            return await safe_edit(query, text, {"inline_keyboard": [[ibtn("Cancel", "adm_home", style="danger", icon=BTN_EMOJIS["cross"])]]})

        elif data == "adm_ask_welcomephoto":
            USER_STATES[user_id] = {"state": "ADM_WAIT_WELCOMEPHOTO"}
            text = f"{E_SETTING} <b>Send new Welcome Photo:</b>\n\nSend a photo (or type <code>none</code> to remove):"
            return await safe_edit(query, text, {"inline_keyboard": [[ibtn("Cancel", "adm_home", style="danger", icon=BTN_EMOJIS["cross"])]]})

        elif data == "adm_cat_pay":
            text = f"""{E_MONEY} <i><b>Payment Gateway & QR Management</b> {E_MONEY}\n\n{E_DOT} <b>Instant Auto Approval:</b> {'ENABLED' if config.get('auto_payment_status') else 'DISABLED'} {E_CHK}\n{E_MONEY} <b>Smart FamPay UPI:</b> <code>{config.get('upi_fampay', 'Not Set')}</code> {E_MONEY}\n{E_MONEY} <b>Manual UPI VPA:</b> <code>{config.get('upi_manual', 'Not Set')}</code> {E_MONEY}\n\nSelect a configuration parameter to modify: {E_SETTING}</i>"""
            btn = {"inline_keyboard": [
                [ibtn("Set FamPay UPI", "adm_ask_fampay", style="primary", icon=BTN_EMOJIS["money"]), ibtn("Set Manual UPI", "adm_ask_manual", style="primary", icon=BTN_EMOJIS["money"])],
                [ibtn("Back to Dashboard", "adm_home", style="danger", icon=BTN_EMOJIS["cross"])]
            ]}
            return await safe_edit(query, text, kb=btn)

        elif data == "adm_cat_price":
            text = f"""{E_DIA} <i><b>Subscription Pricing Architecture</b> {E_DIA}\n\n<b>1 Day Access:</b> ₹{config.get('price_1d_inr')} | ${config.get('price_1d_usd')} {E_FIRE}\n<b>3 Days Access:</b> ₹{config.get('price_3d_inr')} | ${config.get('price_3d_usd')} {E_FIRE}\n<b>7 Days Access:</b> ₹{config.get('price_7d_inr')} | ${config.get('price_7d_usd')} {E_DIA}\n<b>1 Month Access:</b> ₹{config.get('price_1m_inr')} | ${config.get('price_1m_usd')} {E_CROWN}\n\nSelect a plan to edit its pricing: {E_SETTING}</i>"""
            btn = {"inline_keyboard": [
                [ibtn("Edit 1 Day Plan", "adm_ask_p_1d", style="primary", icon=BTN_EMOJIS["diamond"]), ibtn("Edit 3 Days Plan", "adm_ask_p_3d", style="primary", icon=BTN_EMOJIS["diamond"])],
                [ibtn("Edit 7 Days Plan", "adm_ask_p_7d", style="success", icon=BTN_EMOJIS["diamond"]), ibtn("Edit 1 Month Plan", "adm_ask_p_1m", style="success", icon=BTN_EMOJIS["crown"])],
                [ibtn("Back to Dashboard", "adm_home", style="danger", icon=BTN_EMOJIS["cross"])]
            ]}
            return await safe_edit(query, text, kb=btn)

        elif data == "adm_cat_channels":
            fsubs = config.get("fsub_channels", [])
            fsub_text = ""
            if not fsubs:
                fsub_text = f"<i>No subscription channels added yet.</i> {E_WRN}\n"
            else:
                for idx, c in enumerate(fsubs):
                    fsub_text += f"<i><b>{idx+1}.</b> {html.escape(c.get('title','Channel'))} (<code>{c.get('id')}</code>)</i> {E_CHK}\n"

            text = f"""{E_GLOBE} <i><b>Subscription Channels & Routing Setup</b> {E_GLOBE}\n\n<b>Active Force-Sub Channels ({len(fsubs)}):</b> {E_STAT}\n{fsub_text}\n<b>System Routes:</b> {E_SETTING}\n• <b>Mandatory Request ID:</b> <code>{config.get('reqall_id')}</code> {E_SHD}\n\nSelect an action below: {E_SETTING}</i>"""
            
            btn = {"inline_keyboard": [
                [ibtn("➕ Add Channels (Multiple)", "adm_add_channel", style="success", icon=BTN_EMOJIS["add"]), ibtn("Remove Channels", "adm_manage_channels", style="danger", icon=BTN_EMOJIS["cross"])],
                [ibtn("Mandatory Request", "adm_ask_reqall", style="primary", icon=BTN_EMOJIS["shield"])],
                [ibtn("Back to Dashboard", "adm_home", style="danger", icon=BTN_EMOJIS["cross"])]
            ]}
            return await safe_edit(query, text, kb=btn)

        elif data == "adm_add_channel":
            USER_STATES[user_id] = {"state": "ADM_WAIT_ADD_CHANNEL"}
            text = f"""{E_ADD} <i><b>Add Subscription Channels</b> {E_ADD}\n\nYou can send one or multiple channels at once:\n\n1️⃣ Send channel <b>IDs</b> (e.g. <code>-1002532290490</code>).\n2️⃣ Send channel <b>Links / Usernames</b> (e.g. <code>@MyChannel</code> or <code>https://t.me/...</code>).\n3️⃣ Forward a message from the channel.\n\n⚠️ Separate <b>multiple channels</b> with spaces or newlines.</i>"""
            btn = {"inline_keyboard": [[ibtn("Cancel", "adm_cat_channels", style="danger", icon=BTN_EMOJIS["cross"])]]}
            return await safe_edit(query, text, kb=btn)

        elif data == "adm_manage_channels":
            fsubs = config.get("fsub_channels", [])
            if not fsubs:
                return await query.answer("No channels available to remove!", show_alert=True)
            
            btn_rows = []
            for idx, ch in enumerate(fsubs):
                title = ch.get("title", f"Channel {idx+1}")[:20]
                btn_rows.append([ibtn(f"Remove: {title}", f"adm_delch_{idx}", style="danger", icon=BTN_EMOJIS["cross"])])
            btn_rows.append([ibtn("Back", "adm_cat_channels", style="primary", icon=BTN_EMOJIS["cross"])])
            
            text = f"{E_GLOBE} <i><b>Manage Channels:</b> {E_GLOBE}\nClick on any channel below to remove it from verification: {E_SHD}</i>"
            return await safe_edit(query, text, {"inline_keyboard": btn_rows})

        elif data.startswith("adm_delch_"):
            idx = int(data.split("_")[2])
            fsubs = config.get("fsub_channels", [])
            if 0 <= idx < len(fsubs):
                removed = fsubs.pop(idx)
                await settings_col.update_one({"_id": "config"}, {"$set": {"fsub_channels": fsubs}})
                await query.answer(f"Channel '{removed.get('title')}' removed.", show_alert=True)
            config = await settings_col.find_one({"_id": "config"})
            text, kb = await get_admin_dashboard(config)
            return await safe_edit(query, text, kb)

        elif data == "adm_cat_limits":
            text = f"""{E_ADM} <i><b>Operational Parameters & Throttling</b> {E_SETTING}\n\n{E_GIFT} <b>Free Trial DM Allowance:</b> <code>{config.get('free_trial_limit')}</code> {E_GIFT}\n{E_WAIT} <b>Message Sending Delay:</b> <code>{config.get('msg_delay')}s</code> {E_WAIT}\n{E_MED} <b>Leaderboard Scheduled Hour:</b> <code>{config.get('ldb_time')}:00 IST</code> {E_CAL}\n{E_MONEY} <b>Referral Bonus Reward:</b> <code>{config.get('ref_bonus')} DMs</code> {E_MONEY}\n{E_PROF} <b>Free Target Member Cap:</b> <code>{config.get('free_req_limit')}</code> {E_PROF}\n⚡ <b>Accept Pending Limit per use:</b> <code>{config.get('accept_pending_limit', 50)}</code> {E_CHK}\n🚀 <b>Start Mass DM Limit (Free):</b> <code>{config.get('start_mass_dm_limit', 100)}</code> {E_START}\n🎯 <b>Join Req DM Limit (Free):</b> <code>{config.get('join_req_dm_limit', 100)}</code> {E_GLOBE}\n\nSelect a parameter to modify: {E_SETTING}</i>"""
            btn = {"inline_keyboard": [
                [ibtn("Set Free Trial", "adm_ask_freetrial", style="success", icon=BTN_EMOJIS["gift"]), ibtn("Set Sending Delay", "adm_ask_delay", style="primary", icon=BTN_EMOJIS["wait"])],
                [ibtn("Set Accept Pending Limit", "adm_ask_acceptlimit", style="success", icon=BTN_EMOJIS["tick"]), ibtn("Set Referral Bonus", "adm_ask_refbonus", style="success", icon=BTN_EMOJIS["add"])],
                [ibtn("Set Start Mass Limit", "adm_ask_startmasslimit", style="primary", icon=BTN_EMOJIS["rocket"]), ibtn("Set Join Req Limit", "adm_ask_joinreqlimit", style="primary", icon=BTN_EMOJIS["target"])],
                [ibtn("Back to Dashboard", "adm_home", style="danger", icon=BTN_EMOJIS["cross"])]
            ]}
            return await safe_edit(query, text, kb=btn)

        elif data == "adm_cat_redeem":
            USER_STATES[user_id] = {"state": "ADM_WAIT_CREATE_REDEEM"}
            text = f"""{E_GIFT} <i><b>Redeem Codes Management</b></i> {E_CROWN}\n\nYou can send directly in <code>CODE DAYS</code> format!\nExample: <code>VIP30 30</code> {E_STAR}"""
            btn = {"inline_keyboard": [[ibtn("Back to Dashboard", "adm_home", style="danger", icon=BTN_EMOJIS["cross"])]]}
            return await safe_edit(query, text, kb=btn)

        elif data == "adm_cat_admins":
            USER_STATES[user_id] = {"state": "ADM_WAIT_ADD_ADMIN"}
            admins_list = redeem_admin_store.list_admins()
            listing = "\n".join(str(x) for x in admins_list) or "No added admins."
            text = f"""{E_PROF} <i><b>Admins Panel</b></i> {E_CROWN}\n\nCurrent added admins:\n{listing}\n\n➕ <b>New Admin:</b> Just send the Telegram User ID!\nExample: <code>6914205738</code> {E_CROWN}"""
            btn = {"inline_keyboard": [[ibtn("Back to Dashboard", "adm_home", style="danger", icon=BTN_EMOJIS["cross"])]]}
            return await safe_edit(query, text, kb=btn)

        elif data == "adm_cat_users":
            text = f"""{E_PROF} <i><b>User Management & Authorization</b> {E_PROF}\n\nInspect user records, grant or revoke VIP access, clear stored sessions, or issue suspensions directly. {E_SHD}</i>"""
            btn = {"inline_keyboard": [
                [ibtn("Inspect User Record", "adm_ask_checkuser", style="primary", icon=BTN_EMOJIS["user"]), ibtn("Wipe User Sessions", "adm_ask_clearsess", style="danger", icon=BTN_EMOJIS["cross"])],
                [ibtn("Grant VIP Access", "adm_ask_giveprem", style="success", icon=BTN_EMOJIS["crown"]), ibtn("Revoke VIP Access", "adm_ask_remprem", style="danger", icon=BTN_EMOJIS["ban"])],
                [ibtn("Suspend User", "adm_ask_banuser", style="danger", icon=BTN_EMOJIS["ban"]), ibtn("Reactivate User", "adm_ask_unbanuser", style="success", icon=BTN_EMOJIS["tick"])],
                [ibtn("Back to Dashboard", "adm_home", style="danger", icon=BTN_EMOJIS["cross"])]
            ]}
            return await safe_edit(query, text, kb=btn)

        elif data == "adm_broadcast":
            USER_STATES[user_id] = {"state": "ADM_WAITING_BROADCAST"}
            return await safe_edit(query, f"{E_GLOBE} <i><b>Create Broadcast Announcement:</b> {E_GLOBE}\n\nPlease send or forward any message (Text, Media, Caption, etc.) you wish to broadcast to all registered bot users. {E_START}</i>", {"inline_keyboard": [[ibtn("Cancel", "adm_cancel", style="danger", icon=BTN_EMOJIS["cross"])]]})

        elif data.startswith("adm_ask_"):
            action = data.replace("adm_ask_", "")
            USER_STATES[user_id] = {"state": f"ADM_WAIT_{action.upper()}"}
            prompts = {
                "SUPPORT": f"{E_TRI} <i>Please provide the new <b>Support Username</b> (e.g. <code>ZeroxPlayzYT</code>): {E_PROF}</i>",
                "PROMOFACTOR": f"{E_TRI} <i>Please provide the new <b>Channel Promo Divisor factor</b> (e.g. <code>2</code> means Members ÷ 2): {E_STAT}</i>",
                "CREATEBOT": f"{E_TRI} <i>Please provide the new <b>Create Bot Link / Username</b> (e.g. <code>https://t.me/BotFather</code>): {E_START}</i>",
                "FAMPAY": f"{E_TRI} <i>Please provide the new <b>Smart FamPay UPI ID</b> (e.g. <code>user@fam</code>): {E_MONEY}</i>",
                "MANUAL": f"{E_TRI} <i>Please provide the new <b>Manual UPI ID</b> (e.g. <code>name@upi</code>): {E_MONEY}</i>",
                "P_1D": f"{E_TRI} <i>Provide <b>1 Day Plan Price</b> in format: <code>INR USD</code> (e.g. <code>25 0.5</code>): {E_DIA}</i>",
                "P_3D": f"{E_TRI} <i>Provide <b>3 Days Plan Price</b> in format: <code>INR USD</code> (e.g. <code>60 1.5</code>): {E_DIA}</i>",
                "P_7D": f"{E_TRI} <i>Provide <b>7 Days Plan Price</b> in format: <code>INR USD</code> (e.g. <code>120 3</code>): {E_FIRE}</i>",
                "P_1M": f"{E_TRI} <i>Provide <b>1 Month Plan Price</b> in format: <code>INR USD</code> (e.g. <code>350 8</code>): {E_CROWN}</i>",
                "REQALL": f"{E_TRI} <i>Provide Mandatory Request channel in format: <code>Chat_ID Invite_Link</code> (or <code>none</code>): {E_SHD}</i>",
                "FREECH": f"{E_TRI} <i>Provide Free Target channel in format: <code>Chat_ID Invite_Link</code>: {E_GIFT}</i>",
                "LOGCH": f"{E_TRI} <i>Provide Order Log Channel ID (e.g. <code>-100...</code>): {E_STAT}</i>",
                "SUCCLOG": f"{E_TRI} <i>Provide Success Log Channel ID (e.g. <code>-100...</code>): {E_CHK}</i>",
                "LDBCH": f"{E_TRI} <i>Provide Daily Leaderboard Channel ID (e.g. <code>-100...</code>): {E_CROWN}</i>",
                "FREETRIAL": f"{E_TRI} <i>Enter new Free Trial DM limit (e.g. <code>100</code>): {E_GIFT}</i>",
                "DELAY": f"{E_TRI} <i>Enter message delay interval in seconds (e.g. <code>0</code> or <code>1</code>): {E_WAIT}</i>",
                "LDBTIME": f"{E_TRI} <i>Enter Leaderboard Posting Hour (0-23 in IST): {E_CAL}</i>",
                "REFBONUS": f"{E_TRI} <i>Enter bonus DMs awarded per referral (e.g. <code>50</code>): {E_MONEY}</i>",
                "ACCEPTLIMIT": f"{E_TRI} <i>Enter Accept Pending Request limit per use (e.g. <code>50</code>): {E_CHK}</i>",
                "STARTMASSLIMIT": f"{E_TRI} <i>Enter normal user Start Mass DMs limit: {E_START}</i>",
                "JOINREQLIMIT": f"{E_TRI} <i>Enter normal user Join Request DMs limit: {E_GLOBE}</i>",
                "CHECKUSER": f"{E_TRI} <i>Enter the Telegram User ID to inspect: {E_PROF}</i>",
                "CLEARSESS": f"{E_TRI} <i>Enter the Telegram User ID whose sessions you wish to wipe: {E_CROSS}</i>",
                "GIVEPREM": f"{E_TRI} <i>Enter User ID and duration in format: <code>User_ID Days</code> (e.g. <code>12345678 30</code>): {E_CROWN}</i>",
                "REMPREM": f"{E_TRI} <i>Enter the User ID to revoke VIP access: {E_STOP}</i>",
                "BANUSER": f"{E_TRI} <i>Enter the User ID to suspend: {E_WRN}</i>",
                "UNBANUSER": f"{E_TRI} <i>Enter the User ID to reactivate: {E_CHK}</i>"
            }
            prompt_text = prompts.get(action.upper(), f"{E_TRI} <i>Please provide the updated parameter: {E_SETTING}</i>")
            btn = {"inline_keyboard": [[ibtn("Cancel", "adm_cancel", style="danger", icon=BTN_EMOJIS["cross"])]]}
            return await safe_edit(query, prompt_text, kb=btn)

        elif data == "adm_cancel":
            USER_STATES.pop(user_id, None)
            text, kb = await get_admin_dashboard(config)
            return await safe_edit(query, text, kb)

    if data == "coming_soon":
        return await query.answer("This feature is coming soon!", show_alert=True)

    elif data == "all_functions_menu":
        text = f"""{E_SETTING} <b>BOT ALL FUNCTIONS & FEATURES GUIDE</b> {E_START}

──────────────────────
🚀 <b>1. Mass DM Campaign</b>
• Send multiple text, photo, video, or document messages sequentially to all private chats instantly.

📢 <b>2. Channel Promo & Join Req DM</b>
• Promote your channel and send targeted DMs to pending join request users.

🪄 <b>3. Bangali Baba Magic Group Userbot Commands:</b>
• <code>.kickthefools</code> — Kick offline members from group.
• <code>.zombies</code> — Kick deleted accounts (stoppable with `.s`).
• <code>.muteall</code> — Mute all members using magic.
• <code>.unmuteall</code> — Unmute all members.
• <code>.banall</code> — Ban all members from group.
• <code>.kickall</code> — Kick all members from group.

💬 <b>4. Random Spam & Auto-Reply Commands:</b>
• <code>.sp &lt;text&gt; &lt;count&gt;</code> sp — Spam message X times (e.g., `.sp hello 10`).
• <code>.r @username</code> — Send Abuse message unlimited  (stoppable with `.s`).
• <code>.rr @username</code> — IF TARGET SEND A MESSAGE TO YOU BOT WILL ABUSE HIM
• <code>.sdmd</code> — Toggle Self-Distract Media Saver (saves view-once photos/videos to Saved Messages).
• <code>.s</code> — Stop all active campaigns, spam loops, and zombie scanning instantly.
──────────────────────"""
        btns = [
            [ibtn("Back to Menu", "back_home", style="danger", icon=BTN_EMOJIS["cross"])]
        ]
        return await safe_edit(query, text, {"inline_keyboard": btns})

    elif data == "my_stats":
        has_prem = await is_premium(user_id)
        u = await get_user(user_id)
        total_dms = u.get("total_dms", 0)
        plans_purchased = u.get("plans_purchased", 0)
        last_camp = u.get("last_campaign", {"status": "Stopped", "sent": 0, "total": 100})

        if has_prem:
            plan_str = f"VIP Premium — {get_ist_ts_str(u['premium_expiry'])} {E_CROWN}"
        else:
            free_limit = int(config.get('free_trial_limit') or 100)
            sends_left = max(0, free_limit - total_dms)
            plan_str = f"Free — {sends_left} sends left {E_WAIT}"

        text = f"""{E_STAT} <b>YOUR STATISTICS</b> {E_DIA}

──────────────────────
{E_DIA} <b>Plan:</b> {plan_str}

{E_START} <b>Sending</b> {E_FIRE}
• Total DMs Sent: {total_dms} {E_STAT}
• Plans Purchased: {plans_purchased} {E_MONEY}

{E_START} <b>Last Campaign</b> {E_GLOBE}
• {last_camp.get('status', 'Stopped')} — {last_camp.get('sent', 0)}/{last_camp.get('total', 100)} sent {E_CHK}
──────────────────────

<i>Keep sending to grow your reach!</i> {E_FIRE} 📩"""

        btns = [
            [ibtn("Back to Menu", "back_home", style="primary", icon=BTN_EMOJIS["home"])]
        ]
        return await safe_edit(query, text, {"inline_keyboard": btns})

    elif data == "channel_promo_menu":
        u = await get_user(user_id)
        if not u.get("sessions"):
            return await safe_edit(query, f"{E_WRN} <i><b>No Connected Accounts!</b> Please add an account session first. {E_ADD}</i>", {"inline_keyboard": [[ibtn("ADD SESSION", "add_session", style="success", icon=BTN_EMOJIS["add"])], [ibtn("Back", "back_home", style="danger", icon=BTN_EMOJIS["cross"])]]})
        
        support_un = config.get("support_username", "ZeroxPlayzYT")
        div_factor = config.get("promo_price_divisor", 2)
        text = f"""{E_PROF} <b>AADITYAX OFFICIAL CHANNEL PROMO</b> {E_PREM} {E_FIRE}
Welcome to Channel Promo! {E_GIFT}

──────────────────────
{E_GLOBE} <b>GROW YOUR CHANNEL — MEMBERS PROMOTION</b> {E_START}

Promote your Telegram channel and get real members delivered fast. Both <b>Join Request</b> and <b>Normal Member</b> channels are fully supported. {E_CHK}

   ✦ <b>Supported:</b> Join Request Channels & Normal Member Channels {E_SHD}

──────────────────────
{E_SETTING} <b>HOW TO USE — CHANNEL PROMO</b> {E_STAT}

⚙️ <b>Steps to place your order:</b>
1️⃣ <b>Send your channel link</b> {E_LNK}
   → Public, private or join-request — all accepted
2️⃣ <b>Choose how many members you want</b> {E_STAT}
   → Bot calculates your total automatically {E_MONEY}
3️⃣ <b>Scan the UPI QR code and pay the exact amount via FamPay / UPI</b> {E_MONEY}
4️⃣ <b>Submit your UTR + Payment Screenshot</b> {E_CHK}
   → Admin reviews and approves your order {E_CROWN}

──────────────────────
{E_MONEY} <b>PRICING</b> {E_DIA}
The payable amount = <b>Members ÷ {div_factor}</b> {E_STAT}

📊 <b>Examples:</b> {E_CAL}
   100 members → ₹{int(100/div_factor)} {E_MONEY}
   500 members → ₹{int(500/div_factor)} {E_MONEY}
   1000 members → ₹{int(1000/div_factor)} {E_MONEY}

──────────────────────
🔗 <b>Supported link formats:</b> {E_LNK}
🔒 Public: @MyChannel · t.me/MyChannel {E_SHD}
🔒 Private: t.me/+Hash · telegram.me/+Hash {E_SHD}
🤝 Join-req: Any approval-gated invite link {E_CHK}"""

        btns = [
            [ibtn("Start Channel Promo Order", "promo_start_order", style="success", icon=BTN_EMOJIS["start"])],
            [ibtn("Back to Menu", "back_home", style="danger", icon=BTN_EMOJIS["cross"])]
        ]
        return await safe_edit(query, text, {"inline_keyboard": btns})

    elif data == "promo_start_order":
        USER_STATES[user_id] = {"state": "WAITING_PROMO_LINK"}
        text = f"""{E_LNK} <b>Step 1 of 3 — Send your channel link:</b> {E_GLOBE}

Public, private or join-request link accepted. {E_CHK}"""
        btns = [[ibtn("Cancel", "channel_promo_menu", style="danger", icon=BTN_EMOJIS["cross"])]]
        return await safe_edit(query, text, {"inline_keyboard": btns})

    elif data == "promo_pay_now":
        u_state = USER_STATES.get(user_id, {})
        link = u_state.get("link", "")
        members = u_state.get("members", 100)
        div_factor = config.get("promo_price_divisor", 2)
        total = int(members / div_factor)
        order_id = f"ORD-{random.randint(100000, 999999):X}"
        upi_vpa = config.get("upi_fampay", "Samarsingh70@fam")
        qr_link = f"https://api.qrserver.com/v1/create-qr-code/?size=300x300&data=upi://pay?pa={upi_vpa}&pn=ChannelPromo&am={total}&cu=INR"

        USER_STATES[user_id]["order_id"] = order_id
        USER_STATES[user_id]["state"] = "WAITING_PROMO_UTR"

        text = f"""{E_MONEY} <b>CHANNEL PROMO — AUTOMATIC FAMAX / UPI PAYMENT</b> {E_DIA}

──────────────────────
🆔 <b>Order ID:</b>    {order_id} {E_ID}
📢 <b>Channel:</b>     {link[:20]}... {E_GLOBE}
👥 <b>Members:</b>     {members} {E_STAT}
💰 <b>Total:</b>       ₹{total} {E_MONEY}
📱 <b>UPI ID:</b>      {upi_vpa} {E_STAR}
──────────────────────

{E_SETTING} <b>Step 3 of 3 — Pay & submit proof:</b> {E_CHK}

1️⃣ Scan the QR code <b>or</b> open FamPay / any UPI app {E_MONEY}
2️⃣ Pay exactly <b>₹{total}</b> to <code>{upi_vpa}</code> {E_MONEY}
3️⃣ Copy your <b>UTR / Transaction ID</b> from the receipt {E_ID}
4️⃣ Reply with the UTR number {E_CHK}
5️⃣ Then send a payment screenshot {E_PROF}

<i>Save Order ID {order_id} for any support queries.</i> {E_CROWN}"""

        btns = [[ibtn("Cancel", "channel_promo_menu", style="danger", icon=BTN_EMOJIS["cross"])]]
        await query.message.delete()
        return await api_send(user_id, text, kb={"inline_keyboard": btns}, photo=qr_link)

    elif data == "start_dm":
        u = await get_user(user_id)
        if not u.get("sessions"):
            return await safe_edit(query, f"{E_WRN} <i><b>No Connected Accounts!</b> Please add an account session first. {E_ADD}</i>", {"inline_keyboard": [[ibtn("ADD SESSION", "add_session", style="success", icon=BTN_EMOJIS["add"])], [ibtn("Back", "back_home", style="danger", icon=BTN_EMOJIS["cross"])]]})
        
        msgs = u.get("mass_dm_messages", [])
        if not msgs:
            text = f"""✉️ <b>No Message Set</b> {E_WRN}

You haven't set a campaign message yet. {E_STAT}

Tap <b>Set Message Now</b> first, then start your campaign. {E_START}"""
            btns = [
                [ibtn("Set Message Now", "mass_dm_set_prompt", style="success", icon=BTN_EMOJIS["add"])],
                [ibtn("Back", "back_home", style="danger", icon=BTN_EMOJIS["cross"])]
            ]
            return await safe_edit(query, text, {"inline_keyboard": btns})
        else:
            count = len(msgs)
            text = f"""📖 <b>{count} message(s) ready!</b> {E_CHK}

Tap <b>Start DM Campaign</b> to begin sending instantly to your private chats. {E_START}"""
            btns = [
                [ibtn("Start DM Campaign", "mass_dm_start_run", style="success", icon=BTN_EMOJIS["start"])],
                [ibtn("Preview Message", "mass_dm_preview", style="primary", icon=BTN_EMOJIS["stats"])],
                [ibtn("Back to Menu", "back_home", style="danger", icon=BTN_EMOJIS["cross"])]
            ]
            return await safe_edit(query, text, {"inline_keyboard": btns})

    elif data == "mass_dm_set_prompt":
        USER_STATES[user_id] = {"state": "WAITING_MASS_DM_MSGS", "messages": []}
        text = f"""• <b>Multiple messages</b> — send several, each delivered in order {E_STAT}
• <b>Images & files</b> — mix text and media freely {E_PROF}
• <b>No Link Preview</b> — disable the preview card before sending a link and the bot preserves that exact setting for every DM {E_SHD}
• <b>Reply chain</b> — after saving message 1, swipe-reply to it and send message 2; the bot will deliver message 2 as a real reply to message 1 in every DM, exactly as you composed it {E_LNK}

──────────────────────
Send your first message 👇 {E_START}"""
        btns = [
            [ibtn("Cancel", "start_dm", style="danger", icon=BTN_EMOJIS["cross"])]
        ]
        return await safe_edit(query, text, {"inline_keyboard": btns})

    elif data == "mass_dm_done_saving":
        u = await get_user(user_id)
        msgs = u.get("mass_dm_messages", [])
        if not msgs:
            return await query.answer("Please send at least one message first!", show_alert=True)
        count = len(msgs)
        text = f"""📖 <b>{count} message(s) ready!</b> {E_CHK}

Tap <b>Start DM Campaign</b> to begin sending instantly. {E_START}"""
        btns = [
            [ibtn("Start DM Campaign", "mass_dm_start_run", style="success", icon=BTN_EMOJIS["start"])],
            [ibtn("Preview Message", "mass_dm_preview", style="primary", icon=BTN_EMOJIS["stats"])],
            [ibtn("Back to Menu", "back_home", style="danger", icon=BTN_EMOJIS["cross"])]
        ]
        return await safe_edit(query, text, {"inline_keyboard": btns})

    elif data == "mass_dm_start_run":
        u = await get_user(user_id)
        msgs = u.get("mass_dm_messages", [])
        if not msgs:
            return await query.answer("No messages configured! Please set a message first.", show_alert=True)
        sessions = u.get("sessions", [])
        if not sessions:
            return await query.answer("No active account sessions connected!", show_alert=True)

        await users_col.update_one({"_id": user_id}, {"$set": {"mass_dm_active": True}})
        init_text = f"""{E_START} <b>Campaign Running...</b> {E_FIRE}

──────────────────────
📊 <b>Status:</b> Starting fast stream... {E_STAT}
🟩 <b>Sent:</b> 0 | 🟥 <b>Failed:</b> 0
⚡ <b>Speed:</b> -- msg/sec {E_START}
📍 <b>Last:</b> 🔍 Processing chats on the fly... {E_GLOBE}
──────────────────────"""
        
        status_msg = await safe_edit(query, init_text, {"inline_keyboard": [[ibtn("Stop Campaign", "mass_dm_stop_run", style="danger", icon=BTN_EMOJIS["cross"])]]})
        asyncio.create_task(run_real_mass_dm_campaign(user_id, sessions[0], msgs, status_msg.id))
        return

    elif data == "mass_dm_stop_run":
        await users_col.update_one({"_id": user_id}, {"$set": {"mass_dm_active": False}})
        await query.answer("Campaign stopped by user.", show_alert=True)
        first_name = html.escape(query.from_user.first_name if query.from_user else "User")
        text, btn = await get_home_menu(user_id, first_name, config)
        welcome_pic = config.get("welcome_photo", "none")
        if welcome_pic != "none":
            await query.message.delete()
            return await api_send(user_id, text, kb=btn, photo=welcome_pic)
        else:
            return await safe_edit(query, text, kb=btn)

    elif data == "mass_dm_preview":
        u = await get_user(user_id)
        msgs = u.get("mass_dm_messages", [])
        if not msgs:
            return await query.answer("No messages found!", show_alert=True)
        await api_send(user_id, f"<b>Campaign Messages Preview:</b> {E_STAT}")
        for m in msgs:
            m_type = m.get("type", "text")
            m_content = m.get("content", "")
            m_caption = m.get("caption", "")
            if m_type == "photo":
                await api_send(user_id, m_caption, photo=m_content)
            elif m_type == "video":
                await client.send_video(user_id, video=m_content, caption=m_caption)
            elif m_type == "document":
                await client.send_document(user_id, document=m_content, caption=m_caption)
            elif m_type == "animation":
                await client.send_animation(user_id, animation=m_content, caption=m_caption)
            elif m_type == "audio":
                await client.send_audio(user_id, audio=m_content, caption=m_caption)
            elif m_type == "voice":
                await client.send_voice(user_id, voice=m_content, caption=m_caption)
            elif m_type == "sticker":
                await client.send_sticker(user_id, sticker=m_content)
            else:
                await api_send(user_id, m_content)
        await query.answer("Preview sent!", show_alert=False)

    elif data == "join_req_dm_menu":
        u = await get_user(user_id)
        if not u.get("sessions"):
            return await safe_edit(query, f"{E_WRN} <i><b>No Connected Accounts!</b> Please add an account session first. {E_ADD}</i>", {"inline_keyboard": [[ibtn("ADD SESSION", "add_session", style="success", icon=BTN_EMOJIS["add"])], [ibtn("Back", "back_home", style="danger", icon=BTN_EMOJIS["cross"])]]})
        
        USER_STATES[user_id] = {"state": "WAITING_JOIN_REQ_LINK"}
        text = f"""{E_GLOBE} <b>JOIN REQUEST DM</b> {E_START}

Send DMs directly to users who have pending join requests on your channel. {E_CHK}

Send your channel link or username: 👇 {E_LNK}

🔓 Public:  @MyChannel  ·  t.me/MyChannel {E_GLOBE}
🔒 Private: t.me/+InviteHash  ·  telegram.me/+InviteHash {E_SHD}"""
        btns = [
            [ibtn("Cancel", "back_home", style="danger", icon=BTN_EMOJIS["cross"])]
        ]
        return await safe_edit(query, text, {"inline_keyboard": btns})

    elif data == "join_req_dm_start_run":
        u_state = USER_STATES.get(user_id, {})
        link = u_state.get("link")
        limit = u_state.get("limit", 100)
        
        msgs = u.get("mass_dm_messages", [])
        if not msgs:
            return await query.answer("Please set campaign messages first using Set Message!", show_alert=True)
            
        sessions = u.get("sessions", [])
        if not sessions:
            return await query.answer("No connected sessions!", show_alert=True)

        chat_title = u_state.get("chat_title", "CHANNEL")
        await users_col.update_one({"_id": user_id}, {"$set": {"join_req_dm_active": True, "join_req_link": link, "join_req_limit": limit, "join_req_title": chat_title}})
        
        now_time = datetime.now()
        eta_minutes = max(1, limit // 30)
        eta_time = (now_time + timedelta(minutes=eta_minutes)).strftime("%I:%M %p")

        status_text = f"""{E_START} <b>Mass DM Started!</b> {E_FIRE}

🎯 Target: <b>{chat_title}</b> {E_GLOBE}
📊 Limit: {limit} {E_STAT}
🔄 Sessions: {len(sessions)} {E_PROF}
🟢 Filter: All Pending Users {E_CHK}
⏳ Expected Completion: {eta_time} (IST) {E_CAL}
{E_SETTING}"""

        btns = [
            [ibtn("Stop Campaign", "join_req_dm_stop", style="danger", icon=BTN_EMOJIS["cross"])]
        ]
        sent_msg = await safe_edit(query, status_text, {"inline_keyboard": btns})
        
        asyncio.create_task(run_join_req_dm_campaign(user_id, sessions[0], link, limit, msgs, sent_msg.id, chat_title))
        return

    elif data == "join_req_dm_stop":
        await users_col.update_one({"_id": user_id}, {"$set": {"join_req_dm_active": False}})
        await query.answer("Join Request DM campaign stopped.", show_alert=True)
        first_name = html.escape(query.from_user.first_name if query.from_user else "User")
        text, btn = await get_home_menu(user_id, first_name, config)
        welcome_pic = config.get("welcome_photo", "none")
        if welcome_pic != "none":
            await query.message.delete()
            return await api_send(user_id, text, kb=btn, photo=welcome_pic)
        else:
            return await safe_edit(query, text, kb=btn)

    elif data == "set_auto_reply_menu":
        u = await get_user(user_id)
        if not u.get("sessions"):
            return await safe_edit(query, f"{E_WRN} <i><b>No Connected Accounts!</b> Please add an account session first. {E_ADD}</i>", {"inline_keyboard": [[ibtn("ADD SESSION", "add_session", style="success", icon=BTN_EMOJIS["add"])], [ibtn("Back", "back_home", style="danger", icon=BTN_EMOJIS["cross"])]]})
        
        ar_active = u.get("auto_reply_active", False)
        has_msg = bool(u.get("auto_reply_msg"))

        if has_msg:
            msg_type = u.get("auto_reply_type", "text")
            content_preview = str(u.get("auto_reply_msg", ""))
            if msg_type != "text":
                content_preview = f"[{msg_type.title()}]"
            elif len(content_preview) > 30:
                content_preview = content_preview[:27] + "..."

            status_header = f"{E_ACT} <b>🟢 STATUS: ACTIVE & RUNNING</b>" if ar_active else f"{E_STOP} <b>🔴 STATUS: STOPPED</b>"
            text = f"""{status_header}
──────────────────────
{E_SETTING} <b>Auto Reply Configuration</b> {E_ACT}

<b>Type:</b>          {msg_type} {E_STAT}
<b>Content:</b>       {content_preview} {E_GIFT}
──────────────────────

Your auto reply is {'running automatically in the background — even when you are offline' if ar_active else 'currently stopped'}. {E_FIRE}

What would you like to do? {E_SETTING}"""

            btns = []
            if ar_active:
                btns.append([ibtn("Turn Off Auto Reply", "toggle_ar_off", style="danger", icon=BTN_EMOJIS["cross"])])
            else:
                btns.append([ibtn("Turn On Auto Reply", "toggle_ar_on", style="success", icon=BTN_EMOJIS["tick"])])
            
            btns.append([ibtn("Change Message", "ar_set_msg_prompt", style="primary", icon=BTN_EMOJIS["add"])])
            btns.append([ibtn("Preview Message", "ar_preview", style="primary", icon=BTN_EMOJIS["stats"])])
            btns.append([ibtn("Back", "back_home", style="danger", icon=BTN_EMOJIS["cross"])])
            return await safe_edit(query, text, {"inline_keyboard": btns})
        else:
            text = f"""{E_SETTING} <b>Set Auto Reply</b> {E_START}

──────────────────────
This will automatically reply to every private message (DM) received on your connected Telegram account — whether you are online or offline. {E_ACT}

Supported message types:
Text · Links · Photos · Videos · Voice · Documents · GIFs · Stickers · Captions · Any combination {E_GIFT}
──────────────────────

📩 Send me the message you want to set as your Auto Reply. {E_ADD}
This will work whether you are online or offline. {E_FIRE}"""

            btns = [
                [ibtn("Cancel", "back_home", style="danger", icon=BTN_EMOJIS["cross"])]
            ]
            USER_STATES[user_id] = {"state": "WAITING_SET_AUTO_REPLY_MSG"}
            return await safe_edit(query, text, {"inline_keyboard": btns})

    elif data == "toggle_ar_on":
        u = await get_user(user_id)
        if not u.get("auto_reply_msg"):
            return await query.answer("Please set an auto reply message first!", show_alert=True)
        await users_col.update_one({"_id": user_id}, {"$set": {"auto_reply_active": True}})
        
        for s in u.get("sessions", []):
            asyncio.create_task(setup_user_ai_listener(user_id, s))

        await query.answer("🟢 Auto Reply turned ON!", show_alert=True)
        return await cb_handler(client, query)

    elif data == "toggle_ar_off":
        await users_col.update_one({"_id": user_id}, {"$set": {"auto_reply_active": False}})
        await query.answer("🔴 Auto Reply turned OFF!", show_alert=True)
        return await cb_handler(client, query)

    elif data == "ar_set_msg_prompt":
        USER_STATES[user_id] = {"state": "WAITING_SET_AUTO_REPLY_MSG"}
        text = f"""{E_SETTING} <b>Set Auto Reply</b> {E_START}

──────────────────────
This will automatically reply to every private message (DM) received on your connected Telegram account — whether you are online or offline. {E_ACT}

Supported message types:
Text · Links · Photos · Videos · Voice · Documents · GIFs · Stickers · Captions · Any combination {E_GIFT}
──────────────────────

📩 Send me the message you want to set as your Auto Reply. {E_ADD}
This will work whether you are online or offline. {E_FIRE}"""
        await safe_edit(query, text, {"inline_keyboard": [[ibtn("Cancel", "set_auto_reply_menu", style="danger", icon=BTN_EMOJIS["cross"])]]})

    elif data == "ar_preview":
        u = await get_user(user_id)
        ar_msg = u.get("auto_reply_msg")
        ar_type = u.get("auto_reply_type", "text")
        ar_caption = u.get("auto_reply_caption", "")
        if not ar_msg:
            return await query.answer("No auto reply message found!", show_alert=True)
        
        await api_send(user_id, f"<b>Your Current Auto Reply Preview:</b> {E_STAT}")
        if ar_type == "photo":
            await api_send(user_id, ar_caption, photo=ar_msg)
        elif ar_type == "video":
            await client.send_video(user_id, video=ar_msg, caption=ar_caption)
        elif ar_type == "document":
            await client.send_document(user_id, document=ar_msg, caption=ar_caption)
        elif ar_type == "animation":
            await client.send_animation(user_id, animation=ar_msg, caption=ar_caption)
        elif ar_type == "audio":
            await client.send_audio(user_id, audio=ar_msg, caption=ar_caption)
        elif ar_type == "voice":
            await client.send_voice(user_id, voice=ar_msg, caption=ar_caption)
        elif ar_type == "sticker":
            await client.send_sticker(user_id, sticker=ar_msg)
        else:
            await api_send(user_id, ar_msg)
        await query.answer("Preview sent!", show_alert=False)

    elif data == "set_ads_menu":
        u = await get_user(user_id)
        if not u.get("sessions"):
            return await safe_edit(query, f"{E_WRN} <i><b>No Connected Accounts!</b> Please add an account session first. {E_ADD}</i>", {"inline_keyboard": [[ibtn("ADD SESSION", "add_session", style="success", icon=BTN_EMOJIS["add"])], [ibtn("Back", "back_home", style="danger", icon=BTN_EMOJIS["cross"])]]})
        
        text = f"""{E_GLOBE} <b>Ads Management Panel</b> {E_START}

Manage your automated advertisement broadcast using the options below: {E_SETTING}"""

        btns = [
            [ibtn("Set Ads", "ads_manage_panel", style="primary", icon=BTN_EMOJIS["setting"]), ibtn("Back", "back_home", style="danger", icon=BTN_EMOJIS["cross"])]
        ]
        await safe_edit(query, text, {"inline_keyboard": btns})

    elif data == "ads_manage_panel":
        u = await get_user(user_id)
        curr_interval_min = int(u.get("ad_interval", 300)) // 60
        curr_cycles = u.get("ad_cycles", "Unlimited")
        ad_status = ACTIVE_ADS.get(user_id, {}).get("status") == "running"
        status_banner = f"{E_ACT} <b>🟢 ADS STATUS: RUNNING</b>" if ad_status else f"{E_STOP} <b>🔴 ADS STATUS: STOPPED</b>"

        text = f"""{status_banner}
──────────────────────
{E_GLOBE} <b>Ads Management Panel</b> {E_START}

<b>Current Broadcast Interval:</b> <code>{curr_interval_min} Minutes</code> {E_WAIT}
<b>Broadcast Cycles:</b> <code>{curr_cycles}</code> {E_SYNC}
──────────────────────

Manage your automated advertisement broadcast using the options below: {E_SETTING}"""

        has_ad = bool(u.get("ad_message"))
        
        btns = [
            [ibtn("⏱️ 5 Mins", "ad_int_5", style="primary", icon=BTN_EMOJIS["cal"]), ibtn("⏱️ 10 Mins", "ad_int_10", style="primary", icon=BTN_EMOJIS["cal"]), ibtn("⏱️ 30 Mins", "ad_int_30", style="primary", icon=BTN_EMOJIS["cal"])],
            [ibtn("🔄 Set Cycles", "ad_cycles_menu", style="primary", icon=BTN_EMOJIS["refresh"])],
            [ibtn("📩 Set Ads Message", "ad_set_msg_prompt", style="success", icon=BTN_EMOJIS["add"])]
        ]
        if has_ad:
            btns.append([
                ibtn("START ADS" if not ad_status else "Running...", "ad_start" if not ad_status else "coming_soon", style="success", icon=BTN_EMOJIS["tick"]),
                ibtn("STOP ADS", "ad_stop", style="danger", icon=BTN_EMOJIS["cross"])
            ])
            btns.append([ibtn("PREVIEW MESSAGE", "ad_preview", style="primary", icon=BTN_EMOJIS["stats"])])
        
        btns.append([ibtn("Back", "set_ads_menu", style="danger", icon=BTN_EMOJIS["cross"])])
        await safe_edit(query, text, {"inline_keyboard": btns})

    elif data == "ad_cycles_menu":
        text = f"""{E_SYNC} <b>Set number of broadcast cycles:</b> {E_STAT}

<b>Each cycle = 1 full round of sending to all groups.</b> {E_FIRE}
After the set cycles finish, ads will auto-stop. {E_STOP}"""
        
        btns = [
            [ibtn("1 cycle", "ad_cyc_1"), ibtn("3 cycles", "ad_cyc_3"), ibtn("5 cycles", "ad_cyc_5"), ibtn("10 cycles", "ad_cyc_10")],
            [ibtn("25 cycles", "ad_cyc_25"), ibtn("50 cycles", "ad_cyc_50"), ibtn("100 cycles", "ad_cyc_100")],
            [ibtn("Unlimited", "ad_cyc_Unlimited")],
            [ibtn("Back", "ads_manage_panel", style="danger", icon=BTN_EMOJIS["cross"])]
        ]
        await safe_edit(query, text, {"inline_keyboard": btns})

    elif data.startswith("ad_cyc_"):
        val = data.replace("ad_cyc_", "")
        await users_col.update_one({"_id": user_id}, {"$set": {"ad_cycles": val, "completed_cycles": 0}})
        await query.answer(f"Broadcast cycles set to: {val}", show_alert=True)
        return await cb_handler(client, query)

    elif data.startswith("ad_int_"):
        mins = int(data.split("_")[2])
        seconds = mins * 60
        await users_col.update_one({"_id": user_id}, {"$set": {"ad_interval": seconds}})
        await query.answer(f"Broadcast interval set to {mins} minutes!", show_alert=True)
        return await cb_handler(client, query)

    elif data == "ad_set_msg_prompt":
        USER_STATES[user_id] = {"state": "WAITING_SET_AD_MSG"}
        text = f"""📩 <b>Send the message you want to use as your advertisement:</b> {E_GIFT}

Tip: Format it in Telegram exactly as you'd like recipients to see it — every entity, emoji, and style will be preserved. {E_STAR}"""
        await safe_edit(query, text, {"inline_keyboard": [[ibtn("Cancel", "ads_manage_panel", style="danger", icon=BTN_EMOJIS["cross"])]]})

    elif data == "ad_start":
        u = await get_user(user_id)
        if not u.get("ad_message"):
            return await query.answer("Please set an ad message first!", show_alert=True)
        ACTIVE_ADS[user_id] = {"status": "running", "last_sent": 0}
        await users_col.update_one({"_id": user_id}, {"$set": {"completed_cycles": 0, "ads_active": True}})
        await query.answer("🟢 Ads automation started successfully!", show_alert=True)
        return await cb_handler(client, query)

    elif data == "ad_stop":
        if user_id in ACTIVE_ADS:
            ACTIVE_ADS.pop(user_id, None)
        await users_col.update_one({"_id": user_id}, {"$set": {"ads_active": False}})
        await query.answer("🔴 Ads automation stopped successfully.", show_alert=True)
        return await cb_handler(client, query)

    elif data == "ad_preview":
        u = await get_user(user_id)
        ad_msg = u.get("ad_message")
        ad_type = u.get("ad_type", "text")
        ad_caption = u.get("ad_caption", "")
        if not ad_msg:
            return await query.answer("No ad message found!", show_alert=True)
        
        await api_send(user_id, f"<b>Your Current Advertisement Preview:</b> {E_STAT}")
        if ad_type == "photo":
            await api_send(user_id, ad_caption, photo=ad_msg)
        elif ad_type == "video":
            await client.send_video(user_id, video=ad_msg, caption=ad_caption)
        elif ad_type == "document":
            await client.send_document(user_id, document=ad_msg, caption=ad_caption)
        else:
            await api_send(user_id, ad_msg)
        await query.answer("Preview sent!", show_alert=False)

    elif data == "accept_pending_menu":
        u = await get_user(user_id)
        if not u.get("sessions"):
            return await safe_edit(query, f"{E_WRN} <i><b>No Connected Accounts!</b> Please add an account session first. {E_ADD}</i>", {"inline_keyboard": [[ibtn("ADD SESSION", "add_session", style="success", icon=BTN_EMOJIS["add"])], [ibtn("Back", "back_home", style="danger", icon=BTN_EMOJIS["cross"])]]})
        
        USER_STATES[user_id] = {"state": "WAITING_ACCEPT_PENDING_LINK"}
        text = f"""{E_CHK} <b>ACCEPT PENDING</b> {E_START}

Send the channel username or link — public and private both work: {E_GLOBE}

🔓 Public:  @MyChannel  ·  t.me/MyChannel {E_GLOBE}
🔒 Private: t.me/+InviteHash  ·  telegram.me/+InviteHash {E_SHD}"""
        await safe_edit(query, text, {"inline_keyboard": [[ibtn("Cancel", "back_home", style="danger", icon=BTN_EMOJIS["cross"])]]})

    elif data == "check_join":
        joined, not_joined_list = await check_force_join(user_id)
        if not joined:
            text = f"{E_WRN} <i><b>Subscription Verification Required!</b> {E_SHD}\n\nTo ensure quality service, please join our official channels before continuing. {E_STAR}</i>"
            btn_list = []; row = []
            for i, (idx, link) in enumerate(not_joined_list):
                fixed_link = link if str(link).startswith("http") else f"https://t.me/{str(link).replace('@','')}"
                row.append(ibtn(f"{idx}", url=fixed_link, style="primary", icon=BTN_EMOJIS["target"]))
                if len(row) == 2: btn_list.append(row); row = []
            if row: btn_list.append(row)
            btn_list.append([ibtn("I Have Joined", "check_join", style="success", icon=BTN_EMOJIS["tick"])])
            
            try: await api_edit(user_id, query.message.id, text, {"inline_keyboard": btn_list})
            except: pass
            
            return await query.answer("You have not joined all channels yet! Please verify.", show_alert=True)
            
        first_name = html.escape(query.from_user.first_name if query.from_user else "User")
        text, btn = await get_home_menu(user_id, first_name, config)
        await query.message.delete()
        welcome_pic = config.get("welcome_photo", "none")
        if welcome_pic != "none":
            await api_send(user_id, text, kb=btn, photo=welcome_pic)
        else:
            await api_send(user_id, text, kb=btn)

    elif data == "scrape_group":
        USER_STATES[user_id] = {"state": "WAITING_SCRAPE_LINK"}
        await safe_edit(query, f"{E_PROF} <i><b>Public Group Scraper Engine</b> {E_GLOBE}\n\nPlease provide the Public Group link or username. {E_LNK}\nExample: https://t.me/PublicGroup or @PublicGroup</i>", {"inline_keyboard": [[ibtn("Cancel", "back_home", style="danger", icon=BTN_EMOJIS["cross"])]]})
        
    elif data == "invite_earn":
        bot_info = await client.get_me()
        invite_link = f"https://t.me/{bot_info.username}?start=ref{user_id}"
        u = await get_user(user_id)
        referrals = u.get("referrals", [])
        completed_refs = len(referrals)
        earned_days = u.get("milestone_earned_days", 0)

        text = f"""{E_MONEY} <b>REFER & EARN — Free Premium Days!</b> {E_GIFT} {E_CROWN}

──────────────────────
<b>Your Unique Referral Link:</b> {E_LNK}
<code>{invite_link}</code>

<b>Your Referral Stats:</b> {E_STAT}
• Completed: {completed_refs} {E_CHK}
• Pending: 0 {E_WAIT}
• Milestone Days Earned: {earned_days} day(s) {E_CAL}
• Per-Referral Bonus: +1 day(s) each {E_MONEY}
──────────────────────

{E_MED} <b>MILESTONE REWARDS:</b> {E_CROWN}
5 Referrals = 1 Day {E_GIFT}
10 Referrals = 3 Days {E_DIA}
15 Referrals = 7 Days {E_FIRE}
20 Referrals = 15 Days {E_STAR}
25 Referrals = 30 Days {E_CROWN}

<b>Next Milestone:</b> 5 refs → 1 days {E_STAT}
[{'█' * min(completed_refs, 5)}{'░' * max(0, 5 - completed_refs)}] {completed_refs}/5 {E_CHK}

{E_SETTING} <b>HOW IT WORKS:</b> {E_START}
1️⃣ Share your link with a friend {E_LNK}
2️⃣ They open the bot using YOUR link {E_PROF}
3️⃣ They add their Telegram account {E_ADD}
4️⃣ They run a DM campaign & send 1+ message {E_FIRE}

{E_WRN} <b>RULES:</b> {E_SHD}
• Opening the bot alone does NOT count {E_STOP}
• Account + DM campaign — both required {E_CHK}
• Cannot refer yourself {E_STOP}
• Each person can only be referred once {E_CHK}"""
        btn = {"inline_keyboard": [
            [ibtn("Share My Referral Link", url=f"https://t.me/share/url?url={invite_link}&text=🚀%20Grow%20your%20Telegram%20reach%20instantly%20with%20this%20Advanced%20Mass%20DM%20Bot!", style="success", icon=BTN_EMOJIS["money"])],
            [ibtn("Back to Menu", "back_home", style="primary", icon=BTN_EMOJIS["home"])]
        ]}
        await safe_edit(query, text, kb=btn)

    elif data == "redeem_code_prompt":
        USER_STATES[user_id] = {"state": "WAITING_GIFT_CODE"}
        text = f"""{E_GIFT} <b>REDEEM GIFT CODE</b> {E_CROWN}

Enter your redeem / gift code below: {E_ID}
Example: VIP30 {E_STAR}

<i>Codes grant free premium days instantly.</i> {E_DIA}"""
        btns = [[ibtn("Cancel", "back_home", style="danger", icon=BTN_EMOJIS["cross"])]]
        await safe_edit(query, text, {"inline_keyboard": btns})

    elif data == "my_account":
        u = await get_user(user_id)
        joined = u.get("joined_date", "2026-09-04")
        has_prem = await is_premium(user_id)
        phone = u.get("phone", "+8801859614963")

        if has_prem:
            exp_str = get_ist_ts_str(u['premium_expiry'])
            plan_box = f"👑 <b>VIP Premium</b>\nExpiry: {exp_str} {E_STAR}"
        else:
            free_limit = int(config.get('free_trial_limit') or 100)
            total_dms = u.get("total_dms", 0)
            rem = max(0, free_limit - total_dms)
            plan_box = f"🆓 <b>Free Plan</b>\nSends used: {total_dms} / {free_limit} {E_STAT}\nRemaining: {rem} {E_WAIT}"

        text = f"""{E_PROF} <b>MY PROFILE</b> {E_ID} {E_STAR}

──────────────────────
🆔 User ID: <code>{user_id}</code> {E_ID}
👤 Username: @{u.get('username') or 'AADITYAXOFFICAL'} {E_PROF}
📱 Phone: {phone} {E_CAL}
📅 Joined: {joined} {E_CAL}
──────────────────────

{E_DIA} <b>PLAN STATUS</b> {E_CROWN}

{plan_box}"""

        btn = {"inline_keyboard": [
            [ibtn("Back to Menu", "back_home", style="primary", icon=BTN_EMOJIS["home"])]
        ]}
        await safe_edit(query, text, kb=btn)

    elif data == "remove_session":
        for k, client_task in list(AI_LISTENER_TASKS.items()):
            if k.startswith(f"{user_id}:"):
                try: await client_task.stop()
                except: pass
                AI_LISTENER_TASKS.pop(k, None)
        ACTIVE_ADS.pop(user_id, None)
        await users_col.update_one({"_id": user_id}, {"$set": {"sessions": [], "ads_active": False, "auto_reply_active": False, "random_auto_reply_active": False}})
        await safe_edit(query, f"{E_CHK} <i><b>All active Telegram sessions cleared successfully!</b> {E_CHK}</i>", {"inline_keyboard": [[ibtn("Back to Menu", "back_home", style="primary", icon=BTN_EMOJIS["home"])]]})

    elif data == "buy_premium":
        text = f"""{E_PREM} <b>VIP PLANS & BENEFITS</b> {E_CROWN} {E_DIA}

Free: 100 sends total {E_GIFT}
⚡ 1 Day — ₹{config.get('price_1d_inr')} (unlimited) {E_FIRE}
🔥 3 Days — ₹{config.get('price_3d_inr')} {E_FIRE}
💎 7 Days — ₹{config.get('price_7d_inr')} {E_DIA}
🏆 15 Days — ₹190 {E_MED}
👑 1 Month — ₹{config.get('price_1m_inr')} {E_CROWN}

{E_STAR} <b>VIP PREMIUM BENEFITS:</b> {E_PREM}
• <b>Unlimited Mass DMs</b> — No daily or total limits on outreach campaigns. {E_FIRE}
• <b>Priority Node Dispatch</b> — High-speed delivery with accelerated server allocation. {E_START}
• <b>Profile Protection</b> — Your personal account name and bio remain completely untouched. {E_SHD}
• <b>Advanced Join Request Automation</b> — Unlimited request approvals and automated DMs. {E_CHK}
• <b>Persistent Auto-Reply & Background Worker</b> — 24/7 offline auto-responder active across all connected sessions. {E_ACT}

{E_WRN} <b>TERMS</b> {E_SHD}
• Use responsibly — no spam or illegal content {E_STOP}
• We are not responsible for account restrictions {E_WRN}
• Premium plans are non-refundable {E_MONEY}
• By using this bot you agree to these terms {E_CHK}"""
        btn = {"inline_keyboard": [
            [ibtn("1 Day Plan", "plan_1", style="primary", icon=BTN_EMOJIS["diamond"]), ibtn("3 Days Plan", "plan_3", style="primary", icon=BTN_EMOJIS["diamond"])],
            [ibtn("7 Days Plan", "plan_7", style="success", icon=BTN_EMOJIS["diamond"]), ibtn("1 Month Plan", "plan_30", style="success", icon=BTN_EMOJIS["crown"])],
            [ibtn("Back to Menu", "back_home", style="danger", icon=BTN_EMOJIS["cross"])]
        ]}
        await safe_edit(query, text, kb=btn)

    elif data.startswith("plan_"):
        days = int(data.split("_")[1])
        btn = {"inline_keyboard": [
            [ibtn("Instant Auto UPI Approval", f"pay_fampay_{days}", style="success", icon=BTN_EMOJIS["tick"])],
            [ibtn("Manual Admin Verification", f"pay_manual_{days}", style="primary", icon=BTN_EMOJIS["money"])],
            [ibtn("Back to Plans", "buy_premium", style="danger", icon=BTN_EMOJIS["cross"])]
        ]}
        text = f"{E_MONEY} <i><b>Select Payment Method for {days} Days Plan:</b> {E_DIA}</i>"
        await safe_edit(query, text, kb=btn)

    elif data.startswith("pay_"):
        parts = data.split("_"); mthd = parts[1]; days = int(parts[2])
        price_inr = config.get(f"price_{days}d_inr") if days != 30 else config.get("price_1m_inr")
        
        if mthd == "fampay":
            if not config.get("auto_payment_status", False):
                return await query.answer("Auto-Approval is offline. Please choose Manual Payment.", show_alert=True)
            fampay_vpa = config.get("upi_fampay", "aaditya3271@fam")
            qr_link = f"https://api.qrserver.com/v1/create-qr-code/?size=300x300&data=upi://pay?pa={fampay_vpa}&pn=AadityaDMS&am={price_inr}&cu=INR"
            text = f"{E_MONEY} <i><b>Instant Auto-Approval Payment ({days} Days):</b> {E_MONEY}\n\n1. Scan the QR code or pay exactly <b>₹{price_inr}</b> to:\n<code>{fampay_vpa}</code> {E_STAR}\n2. Click 'Verify Payment' and provide the <b>UTR / Transaction ID</b>. {E_CHK}\n\nAutomated bank verification will process your access in seconds. {E_START}</i>"
            
            btn = {"inline_keyboard": [
                [ibtn("✅ Verify Payment", f"sub_fampay_{days}_{price_inr}", style="success", icon=BTN_EMOJIS["check"])],
                [ibtn("Back", f"plan_{days}", style="danger", icon=BTN_EMOJIS["cross"])]
            ]}
            await query.message.delete()
            await api_send(user_id, text, kb=btn, photo=qr_link)
            return
        else: 
            manual_vpa = config.get("upi_manual", "aadityahere@upi")
            qr_link = f"https://api.qrserver.com/v1/create-qr-code/?size=300x300&data=upi://pay?pa={manual_vpa}&pn=AadityaDMS&am={price_inr}&cu=INR"
            text = f"{E_MONEY} <i><b>Manual UPI Transfer ({days} Days):</b> {E_MONEY}\n\n1. Scan the QR code or transfer exactly <b>₹{price_inr}</b> to:\n<code>{manual_vpa}</code> {E_STAR}\n\n2. Click 'I Have Paid' and send the UTR & payment screenshot. {E_PROF}</i>"
            btn = {"inline_keyboard": [[ibtn("I Have Paid", f"sub_man_{days}_{price_inr}", style="success", icon=BTN_EMOJIS["tick"])], [ibtn("Back", f"plan_{days}", style="danger", icon=BTN_EMOJIS["cross"])]]}
            await query.message.delete()
            await api_send(user_id, text, kb=btn, photo=qr_link)

    elif data.startswith("sub_fampay_"):
        parts = data.split("_")
        days = int(parts[2]); price = parts[3]
        USER_STATES[user_id] = {"state": "WAITING_FAMPAY_UTR", "days": days, "price": price}
        await query.message.delete()
        await api_send(user_id, f"{E_STAT} <i><b>Smart Payment Verification</b> {E_SHD}\n\nPlease submit the <b>UTR Number</b> or <b>Transaction ID</b> below:\nExample: 312345678901 {E_ID}</i>")

    elif data.startswith("sub_man_"):
        parts = data.split("_")
        days = int(parts[2]); price = parts[3]
        USER_STATES[user_id] = {"state": "WAITING_MANUAL_UTR", "days": days, "price": price}
        await query.message.delete()
        await api_send(user_id, f"{E_STAT} <i><b>Manual Verification (Step 1/2)</b> {E_MONEY}\n\nPlease send your <b>UTR Number / Transaction Hash</b> first: {E_ID}</i>")

    elif data == "add_session":
        USER_STATES[user_id] = {"state": "WAITING_PHONE"}
        text = f"{E_WAIT} <i><b>Session Account Generator</b> {E_PROF}\nPlease enter your phone number with country code:\nExample: <code>+1234567890</code> {E_CAL}</i>"
        await safe_edit(query, text, {"inline_keyboard": [[ibtn("Cancel", "back_home", style="danger", icon=BTN_EMOJIS["cross"])]]})

    elif data == "how_to_use":
        support_un = config.get("support_username", "ZeroxPlayzYT")
        text = f"""{E_SETTING} <b>HOW TO USE AUTO DMs BOT</b> {E_START} {E_GLOBE}

<b>STEP 1 — Add Your Account</b> {E_ADD}
Tap ➕ Add Account → Enter your phone with country code (e.g. +91XXXXXXXXXX) → Enter the OTP sent to your Telegram → Enter 2FA password if you have one. {E_SHD}

<b>STEP 2 — Set Your Message</b> {E_GIFT}
Tap 📩 Set Message → Send your text, link, or image. You can add multiple messages — they'll be sent one after another to every contact. {E_CHK}

<b>STEP 3 — Launch Campaign</b> {E_START}
Tap 🚀 Start Mass DM Campaign → The bot sends your message to all your DMs instantly. Watch the live progress bar! {E_STAT}

{E_MONEY} <b>PLANS & PRICING</b> {E_DIA}
Free: 100 sends total {E_GIFT}
⚡ 1 Day — ₹25 (unlimited) {E_FIRE}
🔥 3 Days — ₹60 {E_FIRE}
💎 7 Days — ₹120 {E_DIA}
🏆 15 Days — ₹190 {E_MED}
👑 1 Month — ₹350 {E_CROWN}

{E_CROWN} <b>VIP PREMIUM BENEFITS:</b> {E_PREM}
• Unlimited Mass DMs & Priority Node Speed {E_START}
• Complete Account Name & Bio Protection (No auto-branding during ads) {E_SHD}
• Advanced Join Request Automation {E_CHK}
• 24/7 Persistent Background Auto-Reply {E_ACT}

{E_WRN} <b>TERMS</b> {E_SHD}
• Use responsibly — no spam or illegal content {E_STOP}
• We are not responsible for account restrictions {E_WRN}
• Premium plans are non-refundable {E_MONEY}
• By using this bot you agree to these terms {E_CHK}

💬 Support: @{support_un} {E_CROWN}"""
        await safe_edit(query, text, {"inline_keyboard": [[ibtn("Back to Menu", "back_home", style="primary", icon=BTN_EMOJIS["home"])]]})

    elif data == "back_home":
        first_name = html.escape(query.from_user.first_name if query.from_user else "User")
        text, btn = await get_home_menu(user_id, first_name, config)
        welcome_pic = config.get("welcome_photo", "none")
        if welcome_pic != "none":
            await query.message.delete()
            await api_send(user_id, text, kb=btn, photo=welcome_pic)
        else:
            await safe_edit(query, text, kb=btn)

async def handle_states(client, message):
    if not message.text and not message.photo and not message.video and not message.document and not message.animation and not message.audio and not message.voice and not message.sticker: return
    user_id = str(message.chat.id)

    if user_id in USER_STATES and USER_STATES[user_id].get("state") == "ADMIN_ADDING_RANDOM_MSGS":
        state_data = USER_STATES[user_id]
        txt = message.text.strip() if message.text else ""
        if txt.lower() == "/done" or txt.lower().startswith("/done"):
            msgs = state_data.get("messages", [])
            USER_STATES.pop(user_id, None)
            if not msgs:
                return await api_send(user_id, "❌ No messages were added.")
            
            config = await settings_col.find_one({"_id": "config"})
            existing = config.get("admin_random_messages", [])
            existing.extend(msgs)
            await settings_col.update_one({"_id": "config"}, {"$set": {"admin_random_messages": existing}})
            return await api_send(user_id, f"✅ Successfully added {len(msgs)} messages to the random pool! Total pool size: {len(existing)}")
        
        if txt:
            m_list = state_data.get("messages", [])
            m_list.append(txt)
            USER_STATES[user_id]["messages"] = m_list
            await api_send(user_id, f"✅ Message added ({len(m_list)} stored). Send the next message or type **`/done`** to finish.")
        return

    if message.text and message.text.startswith("/"):
        if user_id in USER_STATES: USER_STATES.pop(user_id, None)
        return

    if await _is_owner(int(user_id)) and message.text and not message.text.startswith("/"):
        parts = message.text.strip().split()
        if len(parts) == 2 and parts[1].isdigit() and len(parts[0]) >= 3:
            c_name = parts[0].strip().upper()
            c_days = int(parts[1])
            redeem_admin_store.create_code(c_name, c_days)
            return await api_send(user_id, f"{E_CHK} <i>Redeem code created: <code>{c_name}</code>\nValidity: {c_days} days</i> {E_GIFT}")

    try:
        if user_id not in USER_STATES: return
        state_data = USER_STATES[user_id]
        state = state_data.get("state")
        config = await settings_col.find_one({"_id": "config"})

        if state == "ADM_WAIT_CREATE_REDEEM":
            val = message.text.strip() if message.text else ""
            parts = val.split()
            if len(parts) != 2 or not parts[1].isdigit():
                return await api_send(user_id, f"{E_WRN} <i>Format: <code>CODE DAYS</code>\nExample: <code>VIP30 30</code></i>")
            
            code = parts[0].strip().upper()
            days = int(parts[1])
            USER_STATES.pop(user_id, None)
            redeem_admin_store.create_code(code, days)
            return await api_send(user_id, f"{E_CHK} <i>Redeem code created: <code>{code}</code>\nValidity: {days} days</i> {E_GIFT}")

        if state == "ADM_WAIT_ADD_ADMIN":
            val = message.text.strip() if message.text else ""
            if not val.lstrip("-").isdigit():
                return await api_send(user_id, f"{E_WRN} <i>Please enter a valid numeric User ID.</i>")
            
            target_uid = int(val)
            if target_uid in ADMINS:
                return await api_send(user_id, f"{E_WRN} <i>That user is already an owner.</i>")
            
            USER_STATES.pop(user_id, None)
            redeem_admin_store.add_admin(target_uid)
            return await api_send(user_id, f"{E_CHK} <i>Admin added: <code>{target_uid}</code></i> {E_CROWN}")

        if state == "ADM_WAIT_WELCOMETEXT":
            val = message.text.strip() if message.text else ""
            USER_STATES.pop(user_id, None)
            await settings_col.update_one({"_id": "config"}, {"$set": {"welcome_text": val}})
            return await api_send(user_id, f"{E_CHK} <i>Welcome message text updated successfully!</i>")

        if state == "ADM_WAIT_WELCOMEPHOTO":
            USER_STATES.pop(user_id, None)
            if message.text and message.text.strip().lower() == "none":
                await settings_col.update_one({"_id": "config"}, {"$set": {"welcome_photo": "none"}})
                return await api_send(user_id, f"{E_CHK} <i>Welcome photo removed!</i>")
            elif message.photo:
                photo_id = message.photo.file_id
                await settings_col.update_one({"_id": "config"}, {"$set": {"welcome_photo": photo_id}})
                return await api_send(user_id, f"{E_CHK} <i>Welcome photo set successfully!</i>")
            else:
                return await api_send(user_id, f"{E_WRN} <i>Invalid input. Send a photo or type 'none'.</i>")

        if state == "ADM_WAIT_ADD_CHANNEL":
            raw_items = []
            if message.forward_from_chat:
                raw_items = [str(message.forward_from_chat.id)]
            elif message.text:
                raw_items = message.text.strip().split()
            
            if not raw_items:
                return await api_send(user_id, f"{E_WRN} <i>Invalid input. Please provide channel username, link or ID.</i>")

            cfg = await settings_col.find_one({"_id": "config"})
            fsubs = cfg.get("fsub_channels", [])
            existing_ids = {str(c.get("id")) for c in fsubs}

            added_list = []
            for item in raw_items:
                clean_item = item.strip()
                if not clean_item: continue

                target = int(clean_item) if clean_item.lstrip("-").isdigit() else clean_item
                try:
                    chat_obj = await bot.get_chat(target)
                    ch_id = str(chat_obj.id)
                    ch_title = chat_obj.title or clean_item
                    
                    if ch_id in existing_ids: continue

                    if chat_obj.username:
                        ch_link = f"https://t.me/{chat_obj.username}"
                    elif chat_obj.invite_link:
                        ch_link = chat_obj.invite_link
                    else:
                        try: ch_link = await bot.export_chat_invite_link(chat_obj.id)
                        except: ch_link = clean_item if "t.me/" in clean_item else f"https://t.me/c/{abs(chat_obj.id)}/1"

                    fsubs.append({"id": ch_id, "title": ch_title, "link": ch_link})
                    existing_ids.add(ch_id)
                    added_list.append((ch_title, ch_id))
                except Exception:
                    ch_id = clean_item
                    ch_title = clean_item
                    ch_link = clean_item if "t.me/" in clean_item else f"https://t.me/{clean_item.replace('@','')}"
                    if ch_id not in existing_ids:
                        fsubs.append({"id": ch_id, "title": ch_title, "link": ch_link})
                        existing_ids.add(ch_id)
                        added_list.append((ch_title, ch_id))

            await settings_col.update_one({"_id": "config"}, {"$set": {"fsub_channels": fsubs}})
            USER_STATES.pop(user_id, None)

            if added_list:
                names = "\n".join([f"• <b>{html.escape(t)}</b> (<code>{cid}</code>)" for t, cid in added_list])
                return await api_send(user_id, f"{E_CHK} <i>Channels added successfully ({len(added_list)}):\n\n{names}</i>")
            else:
                return await api_send(user_id, f"{E_WRN} <i>Channels already exist or are invalid.</i>")

        if state and state.startswith("ADM_WAIT_"):
            action_type = state.replace("ADM_WAIT_", "").lower()
            val = message.text.strip() if message.text else ""
            USER_STATES.pop(user_id, None)

            if action_type == "support":
                await settings_col.update_one({"_id": "config"}, {"$set": {"support_username": val.replace("@", "")}})
                await api_send(user_id, f"{E_CHK} <i>Support username successfully updated to @{val.replace('@', '')}</i>")
            elif action_type == "promofactor":
                try:
                    factor = float(val)
                    await settings_col.update_one({"_id": "config"}, {"$set": {"promo_price_divisor": factor}})
                    await api_send(user_id, f"{E_CHK} <i>Channel promo price divisor successfully updated to {factor}</i>")
                except:
                    await api_send(user_id, f"{E_WRN} <i>Invalid number. Please enter a numeric factor.</i>")
            elif action_type == "createbot":
                await settings_col.update_one({"_id": "config"}, {"$set": {"create_bot_link": val}})
                await api_send(user_id, f"{E_CHK} <i>Create bot link successfully updated to: <code>{val}</code></i>")
            elif action_type == "fampay":
                await settings_col.update_one({"_id": "config"}, {"$set": {"upi_fampay": val}})
                await api_send(user_id, f"{E_CHK} <i>Smart FamPay UPI ID successfully updated to: <code>{val}</code></i>")
            elif action_type == "manual":
                await settings_col.update_one({"_id": "config"}, {"$set": {"upi_manual": val}})
                await api_send(user_id, f"{E_CHK} <i>Manual UPI ID successfully updated to: <code>{val}</code></i>")
            elif action_type in ["p_1d", "p_3d", "p_7d", "p_1m"]:
                parts = val.split()
                if len(parts) < 2:
                    return await api_send(user_id, f"{E_WRN} <i>Format: <code>INR USD</code> (e.g. <code>25 0.5</code>)</i>")
                try:
                    inr, usd = parts[0], parts[1]
                    tk = action_type.replace("p_", "")
                    await settings_col.update_one({"_id": "config"}, {"$set": {f"price_{tk}_inr": inr, f"price_{tk}_usd": usd}})
                    await api_send(user_id, f"{E_CHK} <i>Subscription rate for {tk.upper()} updated successfully (₹{inr} | ${usd}).</i>")
                except Exception as e:
                    await api_send(user_id, f"{E_WRN} <i>Error updating price: {e}</i>")
            elif action_type == "reqall":
                if val.lower() == "none":
                    await settings_col.update_one({"_id": "config"}, {"$set": {"reqall_id": "none"}})
                    await api_send(user_id, f"{E_CHK} <i>Mandatory request channel disabled.</i>")
                else:
                    parts = val.split(maxsplit=1)
                    if len(parts) < 2:
                        return await api_send(user_id, f"{E_WRN} <i>Format: <code>Chat_ID Invite_Link</code> or <code>none</code></i>")
                    await settings_col.update_one({"_id": "config"}, {"$set": {"reqall_id": parts[0], "reqall_link": parts[1]}})
                    await api_send(user_id, f"{E_CHK} <i>Mandatory request channel updated successfully.</i>")
            elif action_type in ["freetrial", "delay", "ldbtime", "refbonus", "acceptlimit", "startmasslimit", "joinreqlimit"]:
                try:
                    num = int(val) if action_type != "delay" else float(val)
                    km = {
                        "freetrial": "free_trial_limit",
                        "delay": "msg_delay",
                        "ldbtime": "ldb_time",
                        "refbonus": "ref_bonus",
                        "acceptlimit": "accept_pending_limit",
                        "startmasslimit": "start_mass_dm_limit",
                        "joinreqlimit": "join_req_dm_limit"
                    }
                    await settings_col.update_one({"_id": "config"}, {"$set": {km[action_type]: num}})
                    await api_send(user_id, f"{E_CHK} <i>Parameter successfully updated to {num}.</i>")
                except:
                    await api_send(user_id, f"{E_WRN} <i>Invalid numeric value.</i>")
            elif action_type == "checkuser":
                try:
                    u = await get_user(val)
                    is_p = "Yes" if u.get('premium_expiry', 0) > time.time() else "No"
                    exp_str = get_ist_ts_str(u['premium_expiry']) if u.get('premium_expiry', 0) > time.time() else "Expired/None"
                    await api_send(user_id, f"{E_PROF} <i><b>User Record:</b> <code>{u['_id']}</code>\nUsername: @{u.get('username','N/A')}\nPremium: {is_p} ({exp_str})\nDMs Sent: {u.get('total_dms', 0)}\nSessions: {len(u.get('sessions', []))}\nBanned: {u.get('banned', False)}</i>")
                except Exception as e:
                    await api_send(user_id, f"{E_WRN} <i>User not found or error: {e}</i>")
            elif action_type == "clearsess":
                await users_col.update_one({"_id": val}, {"$set": {"sessions": []}})
                await api_send(user_id, f"{E_CHK} <i>Sessions wiped successfully for user <code>{val}</code>.</i>")
            elif action_type == "giveprem":
                parts = val.split()
                if len(parts) < 2:
                    return await api_send(user_id, f"{E_WRN} <i>Format: <code>User_ID Days</code> (e.g. <code>12345678 30</code>)</i>")
                target_uid, days = parts[0], int(parts[1])
                u = await get_user(target_uid); cur = time.time()
                nex = (u.get("premium_expiry", 0) + (days * 86400)) if u.get("premium_expiry", 0) > cur else (cur + (days * 86400))
                await users_col.update_one({"_id": target_uid}, {"$set": {"premium_expiry": nex}})
                await api_send(user_id, f"{E_CHK} <i>VIP Access granted to user <code>{target_uid}</code> for {days} days.</i>")
            elif action_type == "remprem":
                await users_col.update_one({"_id": val}, {"$set": {"premium_expiry": 0}})
                await api_send(user_id, f"{E_CHK} <i>VIP Access revoked for user <code>{val}</code>.</i>")
            elif action_type == "banuser":
                await users_col.update_one({"_id": val}, {"$set": {"banned": True}})
                await api_send(user_id, f"{E_CHK} <i>User <code>{val}</code> suspended.</i>")
            elif action_type == "unbanuser":
                await users_col.update_one({"_id": val}, {"$set": {"banned": False}})
                await api_send(user_id, f"{E_CHK} <i>User <code>{val}</code> reactivated.</i>")
            return

        if state == "ADM_WAITING_BROADCAST":
            USER_STATES.pop(user_id, None)
            if not message.reply_to_message and not message.text and not message.photo:
                return await api_send(user_id, f"{E_WRN} <i>Please send a valid message to broadcast.</i>")
            c = 0
            status_msg = await api_send(user_id, f"{E_SYNC} <i>Broadcasting in progress...</i>")
            async for u in users_col.find({}):
                try: 
                    if message.reply_to_message:
                        await message.reply_to_message.copy(int(u["_id"]))
                    else:
                        await message.copy(int(u["_id"]))
                    c += 1
                    await asyncio.sleep(0.05) 
                except: pass
            await api_edit(status_msg.chat.id, status_msg.id, f"{E_CHK} <i><b>Broadcast Completed!</b> Sent to {c} users.</i>")
            return

        if state == "WAITING_PROMO_LINK":
            link = message.text.strip() if message.text else ""
            if not link:
                USER_STATES.pop(user_id, None)
                return await api_send(user_id, f"{E_WRN} <i>Invalid link. Please try again.</i>")
            
            USER_STATES[user_id]["link"] = link
            USER_STATES[user_id]["state"] = "WAITING_PROMO_MEMBERS"
            
            div_factor = config.get("promo_price_divisor", 2)
            text = f"""{E_LNK} <b>Channel link saved!</b> {E_CHK}

📢 <b>Channel:</b> {link[:30]}... {E_GLOBE}

──────────────────────
{E_LNK} <b>Step 2 of 3 — How many members would you like to promote?</b> {E_STAT}

Send a whole number (e.g. 100, 500, 1000). {E_ID}

➕ <b>Pricing reminder: Amount = Members ÷ {div_factor}</b> {E_MONEY}
   100 members → ₹{int(100/div_factor)} | 500 members → ₹{int(500/div_factor)} | 1000 members → ₹{int(1000/div_factor)} {E_DIA}

The bot will calculate your total and generate a payment QR instantly. {E_START}"""
            btns = [[ibtn("Cancel", "channel_promo_menu", style="danger", icon=BTN_EMOJIS["cross"])]]
            await api_send(user_id, text, kb={"inline_keyboard": btns})
            return

        if state == "WAITING_PROMO_MEMBERS":
            txt = message.text.strip() if message.text else ""
            if not txt.isdigit():
                return await api_send(user_id, f"{E_WRN} <i>Please send a valid whole number (e.g. 100, 500).</i>")
            
            members = int(txt)
            USER_STATES[user_id]["members"] = members
            div_factor = config.get("promo_price_divisor", 2)
            total = int(members / div_factor)

            text = f"""{E_STAT} <b>Order Summary:</b> {E_DIA}
• Members: {members} {E_STAT}
• Total Amount: <b>₹{total}</b> {E_MONEY}

Tap below to generate payment QR code and proceed: {E_START}"""
            btns = [
                [ibtn("Proceed to Payment", "promo_pay_now", style="success", icon=BTN_EMOJIS["money"])],
                [ibtn("Cancel", "channel_promo_menu", style="danger", icon=BTN_EMOJIS["cross"])]
            ]
            await api_send(user_id, text, kb={"inline_keyboard": btns})
            return

        if state == "WAITING_PROMO_UTR":
            utr = message.text.strip() if message.text else ""
            if not utr or len(utr) < 5:
                return await api_send(user_id, f"{E_WRN} <i>Please send a valid UTR / Transaction ID number.</i>")
            
            USER_STATES[user_id]["utr"] = utr
            USER_STATES[user_id]["state"] = "WAITING_PROMO_SS"
            await api_send(user_id, f"📸 Now send the <b>Payment Screenshot</b> for verification: {E_PROF}")
            return

        if state == "WAITING_PROMO_SS":
            if not message.photo:
                return await api_send(user_id, f"{E_WRN} <i>Please attach a valid photo screenshot.</i>")
            
            u_state = USER_STATES.pop(user_id, {})
            order_id = u_state.get("order_id", "UNKNOWN")
            link = u_state.get("link", "")
            members = u_state.get("members", 0)
            div_factor = config.get("promo_price_divisor", 2)
            total = int(members / div_factor)
            utr = u_state.get("utr", "")

            btn = {"inline_keyboard": [[ibtn("Approve Promo", f"promoapp_{user_id}_{members}", style="success", icon=BTN_EMOJIS["tick"]), ibtn("Reject", f"promorej_{user_id}", style="danger", icon=BTN_EMOJIS["cross"])]]}
            for admin in ADMINS:
                try:
                    await api_send(admin, f"📢 <b>New Channel Promo Order!</b> {E_DIA}\n\n👤 User: <code>{user_id}</code> {E_ID}\n🆔 Order ID: <code>{order_id}</code> {E_ID}\n📢 Channel: {link} {E_GLOBE}\n👥 Members: {members} {E_STAT}\n💰 Total: ₹{total} {E_MONEY}\n🔗 UTR: <code>{utr}</code> {E_STAR}", kb=btn, photo=message.photo.file_id)
                except:
                    pass
            await api_send(user_id, f"{E_WAIT} <i><b>Order Submitted!</b> Forwarded to administrators for review and processing. {E_CROWN}</i>")
            return

        if state == "WAITING_GIFT_CODE":
            code = message.text.strip().upper() if message.text else ""
            USER_STATES.pop(user_id, None)
            
            row = redeem_admin_store.get_code(code)
            if row:
                code_str, days, used_by, used_at = row
                if used_by is not None:
                    return await api_send(user_id, f"{E_CROSS} <i><b>This redeem code has already been used!</b> {E_WRN}</i>")
                
                success = redeem_admin_store.use_code(code_str, user_id)
                if success:
                    u = await get_user(user_id)
                    current_time = time.time()
                    nex = max(current_time, u.get("premium_expiry", 0)) + (days * 86400)
                    await users_col.update_one({"_id": user_id}, {"$set": {"premium_expiry": nex}, "$inc": {"plans_purchased": 1}})
                    exp_str = get_ist_ts_str(nex)
                    return await api_send(user_id, f"{E_CHK} <i><b>Redeem Code Successfully Applied!</b> {E_GIFT}\n\n{E_DIA} <b>VIP Status Added:</b> {days} Days {E_CROWN}\n{E_CAL} <b>Valid Until:</b> {exp_str} {E_STAR}</i>")
            
            if code == "FREE1DAY":
                u = await get_user(user_id)
                current_time = time.time()
                nex = max(current_time, u.get("premium_expiry", 0)) + (1 * 86400)
                await users_col.update_one({"_id": user_id}, {"$set": {"premium_expiry": nex}})
                await api_send(user_id, f"{E_CHK} <i><b>Gift Code Redeemed Successfully!</b> {E_GIFT}\n1 Day VIP Premium activated. {E_CROWN}</i>")
            else:
                await api_send(user_id, f"{E_CROSS} <i><b>Invalid or Expired Gift Code!</b> Please check and try again. {E_WRN}</i>")
            return

        if state == "WAITING_MASS_DM_MSGS":
            m_list = state_data.get("messages", [])
            
            m_type = "text"
            m_content = ""
            m_caption = ""
            reply_to_idx = None

            if message.reply_to_message:
                orig_id = message.reply_to_message.id
                for idx, saved in enumerate(m_list):
                    if saved.get("msg_id") == orig_id:
                        reply_to_idx = idx
                        break

            if message.photo:
                m_type = "photo"
                m_content = message.photo.file_id
                m_caption = message.caption or ""
            elif message.video:
                m_type = "video"
                m_content = message.video.file_id
                m_caption = message.caption or ""
            elif message.document:
                m_type = "document"
                m_content = message.document.file_id
                m_caption = message.caption or ""
            elif message.animation:
                m_type = "animation"
                m_content = message.animation.file_id
                m_caption = message.caption or ""
            elif message.audio:
                m_type = "audio"
                m_content = message.audio.file_id
                m_caption = message.caption or ""
            elif message.voice:
                m_type = "voice"
                m_content = message.voice.file_id
                m_caption = message.caption or ""
            elif message.sticker:
                m_type = "sticker"
                m_content = message.sticker.file_id
            elif message.text:
                m_type = "text"
                m_content = message.text

            msg_entry = {
                "type": m_type,
                "content": m_content,
                "caption": m_caption,
                "reply_to": reply_to_idx,
                "msg_id": message.id
            }
            m_list.append(msg_entry)
            USER_STATES[user_id]["messages"] = m_list
            await users_col.update_one({"_id": user_id}, {"$set": {"mass_dm_messages": m_list}})

            count = len(m_list)
            text = f"""💬 <b>Text saved! ({count} total)</b> {E_CHK}
✨ 1 formatting entry preserved

Send another message to add more, or tap <b>Done</b> to finish. {E_START}"""
            btns = [
                [ibtn(f"Done — {count} message(s) saved", "mass_dm_done_saving", style="success", icon=BTN_EMOJIS["tick"])],
                [ibtn("Cancel", "start_dm", style="danger", icon=BTN_EMOJIS["cross"])]
            ]
            await api_send(user_id, text, kb={"inline_keyboard": btns})
            return

        if state == "WAITING_JOIN_REQ_LINK":
            link = message.text.strip() if message.text else ""
            if not link:
                USER_STATES.pop(user_id, None)
                return await api_send(user_id, f"{E_WRN} <i>Invalid link. Please try again.</i>")
                
            u = await get_user(user_id)
            sessions = u.get("sessions", [])
            if not sessions:
                USER_STATES.pop(user_id, None)
                return await api_send(user_id, f"{E_WRN} <i>No active session found!</i>")

            msg = await api_send(user_id, f"{E_SYNC} <i>Resolving channel details and counting pending requests...</i>")
            try:
                userbot = Client(f"check_req_{user_id}_{int(time.time())}", api_id=API_ID, api_hash=API_HASH, session_string=sessions[0], in_memory=True)
                await userbot.start()
                chat = await get_chat_safely(userbot, link, link)
                
                total_pending = 0
                try:
                    async for _ in userbot.get_chat_join_requests(chat.id):
                        total_pending += 1
                        if total_pending >= 3000:
                            break
                except Exception:
                    total_pending = "Unknown (Check Admin Rights)"

                await userbot.stop()
                
                has_prem = await is_premium(user_id)
                free_limit = int(config.get("join_req_dm_limit", 100))
                limit = 10000 if has_prem else free_limit
                pass_label = "Unlimited VIP pass" if has_prem else f"Normal user limit ({free_limit})"

                USER_STATES[user_id] = {"state": "WAITING_JOIN_REQ_COUNT", "link": link, "limit": limit, "chat_title": chat.title}
                
                prompt_text = f"""👥 <b>Channel Connected!</b> {E_CHK}

──────────────────────
📢 Channel: {html.escape(chat.title)} {E_GLOBE}
⏳ <b>Total Pending Requests:</b> <code>{total_pending}</code> {E_STAT}
💎 Your Limit: <code>{limit}</code> ({pass_label}) {E_DIA}
──────────────────────

How many users do you want to DM? {E_START}
Please enter the number. 👇

📍 Examples: 10, 20, 25, 60 {E_CAL}"""
                btns = [[ibtn("Cancel", "back_home", style="danger", icon=BTN_EMOJIS["cross"])]]
                await api_edit(msg.chat.id, msg.id, prompt_text, kb={"inline_keyboard": btns})
            except Exception as e_req:
                USER_STATES.pop(user_id, None)
                await api_edit(msg.chat.id, msg.id, f"{E_CROSS} <i><b>Error:</b> <code>{html.escape(str(e_req))}</code></i>")
            return

        if state == "WAITING_JOIN_REQ_COUNT":
            count_text = message.text.strip() if message.text else ""
            if not count_text.isdigit():
                return await api_send(user_id, f"{E_WRN} <i>Please enter a valid numeric amount.</i>")
            
            req_count = int(count_text)
            max_limit = state_data.get("limit", 100)
            if req_count > max_limit:
                req_count = max_limit
                await api_send(user_id, f"Your limit is <code>{max_limit}</code>, so it has been set to that amount. If you need more, click 'Go to VIP Premium'.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("Go to VIP Premium", callback_data="buy_premium")]]))
                
            USER_STATES[user_id]["limit"] = req_count
            
            u = await get_user(user_id)
            msgs = u.get("mass_dm_messages", [])
            if not msgs:
                USER_STATES.pop(user_id, None)
                return await api_send(user_id, "Please set campaign messages first using Set Message!")
            
            sessions = u.get("sessions", [])
            if not sessions:
                USER_STATES.pop(user_id, None)
                return await api_send(user_id, "No connected sessions!")

            link = state_data.get("link")
            chat_title = state_data.get("chat_title", "CHANNEL")
            USER_STATES.pop(user_id, None)
            
            await users_col.update_one({"_id": user_id}, {"$set": {"join_req_dm_active": True, "join_req_link": link, "join_req_limit": req_count, "join_req_title": chat_title}})
            
            now_time = datetime.now()
            eta_minutes = max(1, req_count // 30)
            eta_time = (now_time + timedelta(minutes=eta_minutes)).strftime("%I:%M %p")

            status_text = f"""{E_START} <b>Mass DM Started!</b> {E_FIRE}

🎯 Target: <b>{chat_title}</b> {E_GLOBE}
📊 Limit: {req_count} {E_STAT}
🔄 Sessions: {len(sessions)} {E_PROF}
🟢 Filter: All Pending Users {E_CHK}
⏳ Expected Completion: {eta_time} (IST) {E_CAL}

Commands: /chk , /pause , /resume , /stop {E_SETTING}"""

            btns = [
                [ibtn("Stop Campaign", "join_req_dm_stop", style="danger", icon=BTN_EMOJIS["cross"])]
            ]
            sent_msg = await api_send(user_id, status_text, kb={"inline_keyboard": btns})
            
            asyncio.create_task(run_join_req_dm_campaign(user_id, sessions[0], link, req_count, msgs, sent_msg.id, chat_title))
            return

        if state == "WAITING_SET_AUTO_REPLY_MSG":
            USER_STATES.pop(user_id, None)
            ar_type = "text"
            ar_content = ""
            ar_caption = ""

            if message.photo:
                ar_type = "photo"
                ar_content = message.photo.file_id
                ar_caption = message.caption or ""
            elif message.video:
                ar_type = "video"
                ar_content = message.video.file_id
                ar_caption = message.caption or ""
            elif message.document:
                ar_type = "document"
                ar_content = message.document.file_id
                ar_caption = message.caption or ""
            elif message.animation:
                ar_type = "animation"
                ar_content = message.animation.file_id
                ar_caption = message.caption or ""
            elif message.audio:
                ar_type = "audio"
                ar_content = message.audio.file_id
                ar_caption = message.caption or ""
            elif message.voice:
                ar_type = "voice"
                ar_content = message.voice.file_id
                ar_caption = message.caption or ""
            elif message.sticker:
                ar_type = "sticker"
                ar_content = message.sticker.file_id
            elif message.text:
                ar_type = "text"
                ar_content = message.text

            await users_col.update_one({"_id": user_id}, {"$set": {"auto_reply_msg": ar_content, "auto_reply_type": ar_type, "auto_reply_caption": ar_caption, "auto_reply_active": True}})
            
            u = await get_user(user_id)
            for s in u.get("sessions", []):
                asyncio.create_task(setup_user_ai_listener(user_id, s))

            preview_content = ar_content if ar_type == "text" else f"[{ar_type.title()}]"
            if len(preview_content) > 30:
                preview_content = preview_content[:27] + "..."

            success_text = f"""📋 <b>Auto Reply saved!</b> {E_CHK}

──────────────────────
📋 Type: {ar_type} {E_STAT}
📝 Content: {preview_content} {E_GIFT}
──────────────────────

From now on, every DM to your Telegram account will be automatically replied to with this message — even when you're offline. {E_ACT}

Tap Set Auto Reply again to change it at any time. {E_SETTING}"""

            btns = [
                [ibtn("Turn Off Auto Reply", "toggle_ar_off", style="danger", icon=BTN_EMOJIS["cross"])],
                [ibtn("Change Message", "ar_set_msg_prompt", style="primary", icon=BTN_EMOJIS["add"])],
                [ibtn("Preview Message", "ar_preview", style="primary", icon=BTN_EMOJIS["stats"])],
                [ibtn("Back", "back_home", style="danger", icon=BTN_EMOJIS["cross"])]
            ]
            await api_send(user_id, success_text, kb={"inline_keyboard": btns})
            return

        if state == "WAITING_ACCEPT_PENDING_LINK":
            link = message.text.strip() if message.text else ""
            if not link:
                USER_STATES.pop(user_id, None)
                return await api_send(user_id, f"{E_WRN} <i>Invalid input. Please try again.</i>")
            
            USER_STATES[user_id]["pending_link"] = link
            USER_STATES[user_id]["state"] = "WAITING_ACCEPT_PENDING_COUNT"
            
            has_prem = await is_premium(user_id)
            limit_val = "Unlimited" if has_prem else config.get("accept_pending_limit", 50)
            
            await api_send(user_id, f"How many requests do you want to accept? {E_CHK}\n(Your limit: <code>{limit_val}</code>) {E_STAT}\n\nPlease send the numeric amount: 👇 {E_START}")

        elif state == "WAITING_ACCEPT_PENDING_COUNT":
            count_text = message.text.strip() if message.text else ""
            u_state = USER_STATES.pop(user_id, {}); link = u_state.get("pending_link")
            
            if not count_text.isdigit():
                return await api_send(user_id, f"{E_WRN} <i>Please enter a valid number.</i>")
            
            req_count = int(count_text)
            has_prem = await is_premium(user_id)
            admin_limit = int(config.get("accept_pending_limit", 50))
            
            if not has_prem and req_count > admin_limit:
                req_count = admin_limit
                await api_send(user_id, f"You are on the free tier, so the limit has been set to <code>{admin_limit}</code>. If you need more, please get premium.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("Go to VIP Premium", callback_data="buy_premium")]]))

            u_data = await get_user(user_id)
            sessions = u_data.get("sessions", [])
            if not sessions:
                return await api_send(user_id, f"{E_WRN} <i>No active session found!</i>")
            
            msg = await api_send(user_id, f"{E_SYNC} <i>Processing and accepting pending join requests...</i>")
            
            try:
                userbot = Client(f"accept_pend_{user_id}_{int(time.time())}", api_id=API_ID, api_hash=API_HASH, session_string=sessions[0], in_memory=True)
                await userbot.start()
                chat = await get_chat_safely(userbot, link, link)
                chat_id = chat.id
                
                accepted = 0
                async for req in userbot.get_chat_join_requests(chat_id):
                    if accepted >= req_count:
                        break
                    try:
                        await userbot.approve_chat_join_request(chat_id, req.user.id)
                        accepted += 1
                    except Exception:
                        pass
                
                await userbot.stop()
                await api_edit(msg.chat.id, msg.id, f"{E_CHK} <i><b>Successfully Approved!</b>\n\n📌 <b>Channel:</b> {html.escape(chat.title)}\n✅ <b>Accepted Requests:</b> {accepted} / {req_count}</i>")
            except Exception as e_acc:
                try: await userbot.stop()
                except: pass
                await api_edit(msg.chat.id, msg.id, f"{E_CROSS} <i><b>Failed:</b>\n<code>{html.escape(str(e_acc))}</code></i>")

        elif state == "WAITING_SET_AD_MSG":
            USER_STATES.pop(user_id, None)
            ad_type = "text"
            ad_content = ""
            ad_caption = ""

            if message.photo:
                ad_type = "photo"
                ad_content = message.photo.file_id
                ad_caption = message.caption or ""
            elif message.video:
                ad_type = "video"
                ad_content = message.video.file_id
                ad_caption = message.caption or ""
            elif message.document:
                ad_type = "document"
                ad_content = message.document.file_id
                ad_caption = message.caption or ""
            elif message.text:
                ad_type = "text"
                ad_content = message.text

            await users_col.update_one({"_id": user_id}, {"$set": {"ad_message": ad_content, "ad_type": ad_type, "ad_caption": ad_caption}})
            
            text = f"Ads Message Set Successfully! {E_CHK}\n\nManage your ads broadcast using the control buttons below: {E_SETTING}"
            u = await get_user(user_id)
            btn = {"inline_keyboard": [
                [ibtn("⏱️ 5 Mins", "ad_int_5", style="primary", icon=BTN_EMOJIS["cal"]), ibtn("⏱️ 10 Mins", "ad_int_10", style="primary", icon=BTN_EMOJIS["cal"]), ibtn("⏱️ 30 Mins", "ad_int_30", style="primary", icon=BTN_EMOJIS["cal"])],
                [ibtn("🔄 Set Cycles", "ad_cycles_menu", style="primary", icon=BTN_EMOJIS["refresh"])],
                [ibtn("📩 Set Ads Message", "ad_set_msg_prompt", style="success", icon=BTN_EMOJIS["add"])],
                [ibtn("START ADS", "ad_start", style="success", icon=BTN_EMOJIS["tick"]), ibtn("STOP ADS", "ad_stop", style="danger", icon=BTN_EMOJIS["cross"])],
                [ibtn("PREVIEW MESSAGE", "ad_preview", style="primary", icon=BTN_EMOJIS["stats"])],
                [ibtn("Back", "ads_manage_panel", style="primary", icon=BTN_EMOJIS["home"])]
            ]}
            await api_send(user_id, text, kb=btn)

        elif state == "WAITING_FAMPAY_UTR":
            utr = message.text.strip() if message.text else ""
            if not utr or len(utr) < 5 or not utr.isalnum(): 
                USER_STATES.pop(user_id, None)
                return await api_send(user_id, f"{E_WRN} <i>Invalid UTR/Transaction reference. Please try again.</i>")
            
            user_state_data = USER_STATES.pop(user_id, {})
            days = user_state_data.get("days", 1)
            price = float(user_state_data.get("price", 0.0))
            
            msg = await api_send(user_id, f"{E_SYNC} <i><b>Verifying bank records for ID: {utr}...</b></i>")
            g_user = config.get("gmail_user", "")
            g_pass = config.get("gmail_pass", "")
            paid_amount = await check_payment_in_gmail_async(utr, g_user, g_pass)
            
            if paid_amount and paid_amount >= price:
                u = await get_user(user_id); current_time = time.time()
                today_date = get_ist_str("%Y-%m-%d")
                
                nex = max(current_time, u.get("premium_expiry", 0)) + (days * 86400)
                await users_col.update_one({"_id": user_id}, {"$set": {"premium_expiry": nex}, "$inc": {"plans_purchased": 1}})
                exp_str = get_ist_ts_str(nex)
                await api_edit(msg.chat.id, msg.id, f"{E_DIA} <i><b>Payment Verified Automatically! ({days} Days)</b>\n\n{E_CHK} Reference ID: <code>{utr}</code>\n{E_MONEY} Amount: ₹{paid_amount}\n\n{E_GIFT} <b>VIP Status Valid Until:</b>\n{E_CAL} <b>{exp_str}</b></i>")
                try: await api_send(ADMINS[0], f"{E_DIA} <i><b>Auto-Approval Log</b>\n{E_PROF} User ID: <code>{user_id}</code>\n{E_MONEY} Paid: ₹{paid_amount} ({days} Days)\n{E_ID} Ref: <code>{utr}</code></i>")
                except: pass
                await settings_col.update_one({"_id": "config"}, {"$inc": {"total_sales_inr": paid_amount, f"sales_history.{today_date}": paid_amount}})

                referred_by = u.get("referred_by")
                if referred_by and not u.get("milestone_rewarded", False):
                    await users_col.update_one({"_id": user_id}, {"$set": {"milestone_rewarded": True}})
                    ref_doc = await get_user(referred_by)
                    ref_cur_exp = max(current_time, ref_doc.get("premium_expiry", 0))
                    new_ref_exp = ref_cur_exp + (1 * 86400)
                    await users_col.update_one({"_id": referred_by}, {"$set": {"premium_expiry": new_ref_exp}, "$inc": {"milestone_earned_days": 1}})
                    try:
                        await api_send(int(referred_by), f"🎉 <b>Referral Milestone Reward!</b>\nYour referral completed a purchase. +1 Day VIP Premium added!")
                    except:
                        pass
            else:
                await api_edit(msg.chat.id, msg.id, f"{E_WRN} <i><b>Payment Record Not Found!</b>\nCould not verify ₹{price} for ID <code>{utr}</code>. If just transferred, wait 1-2 minutes and re-submit.</i>")

        elif state == "WAITING_MANUAL_UTR":
            utr = message.text.strip() if message.text else ""
            if not utr or len(utr) < 5:
                USER_STATES.pop(user_id, None)
                return await api_send(user_id, f"{E_WRN} <i>Invalid reference ID. Process cancelled.</i>")
            
            USER_STATES[user_id]["utr"] = utr
            USER_STATES[user_id]["state"] = "WAITING_MANUAL_SS"
            await api_send(user_id, f"{E_STAT} <i><b>Step 2/2:</b> Please send the <b>Payment Screenshot</b>:</i>")

        elif state == "WAITING_MANUAL_SS":
            if not message.photo: 
                USER_STATES.pop(user_id, None)
                return await api_send(user_id, f"{E_WRN} <i>Please attach a valid photo screenshot. Process aborted.</i>")
                
            user_state_data = USER_STATES.pop(user_id, {})
            days = user_state_data.get("days", 1)
            utr = user_state_data.get("utr", "Unknown")
            
            btn = {"inline_keyboard": [[ibtn("Approve", f"manapp_{user_id}_{days}", style="success", icon=BTN_EMOJIS["tick"]), ibtn("Reject", f"manrej_{user_id}_{days}", style="danger", icon=BTN_EMOJIS["cross"])]]}
            for admin in ADMINS:
                try: await api_send(admin, f"{E_MONEY} <i><b>Manual Payment Pending Verification!</b>\n{E_PROF} User ID: <code>{user_id}</code>\nName: {message.from_user.first_name}\n{E_CAL} Duration: {days} Days\n{E_CHK} UTR/Hash: <code>{utr}</code></i>", kb=btn, photo=message.photo.file_id)
                except: pass
            await api_send(user_id, f"{E_WAIT} <i><b>Submission Received!</b> Sent to admins for prompt review.</i>")

        elif state == "WAITING_PHONE":
            phone = message.text.replace(" ", "") if message.text else ""
            if not phone:
                USER_STATES.pop(user_id, None)
                return await api_send(user_id, f"{E_WRN} <i>Invalid phone number. Please try again.</i>")
            
            if phone.isdigit() and not phone.startswith("+"):
                phone = "+" + phone
                
            msg = await api_send(user_id, f"{E_SYNC} <i>Connecting to Telegram cloud servers...</i>")
            temp_client = Client(f"session_{user_id}", api_id=API_ID, api_hash=API_HASH, in_memory=True)
            await temp_client.connect()
            try:
                code_info = await temp_client.send_code(phone)
                USER_STATES[user_id].update({"state": "WAITING_OTP", "phone": phone, "phone_code_hash": code_info.phone_code_hash, "temp_client": temp_client})
                await api_edit(msg.chat.id, msg.id, f"{E_STAT} <i><b>OTP Code Dispatched!</b>\n\n{E_CHK} <b>Send with spaces between digits:</b> <code>1 2 3 4 5</code></i>")
            except Exception as e_phone: 
                USER_STATES.pop(user_id, None)
                await api_edit(msg.chat.id, msg.id, f"{E_WRN} <i><b>Error:</b> {e_phone}</i>")
                try: await temp_client.disconnect()
                except: pass

        elif state == "WAITING_OTP":
            otp = message.text.replace(" ", "") if message.text else ""
            if not otp:
                USER_STATES.pop(user_id, None)
                return await api_send(user_id, f"{E_WRN} <i>Invalid OTP. Please try again.</i>")
                
            user_state_data = USER_STATES.get(user_id, {})
            temp_client = user_state_data.get("temp_client")
            if not temp_client:
                USER_STATES.pop(user_id, None)
                return await api_send(user_id, f"{E_WRN} <i>Session expired. Please start over.</i>")
                
            msg = await api_send(user_id, f"{E_SYNC} <i>Authenticating OTP code...</i>")
            try:
                await temp_client.sign_in(user_state_data.get("phone"), user_state_data.get("phone_code_hash"), otp)
                ss = await temp_client.export_session_string()
                
                await update_user_profile(temp_client, user_id)
                await users_col.update_one({"_id": user_id}, {"$push": {"sessions": ss}})
                
                asyncio.create_task(setup_user_ai_listener(user_id, ss))

                await api_edit(msg.chat.id, msg.id, f"{E_CHK} <i><b>Session Connected & Profile Updated!</b>\nRedirecting to Menu...</i>")
                await temp_client.disconnect(); USER_STATES.pop(user_id, None)
                
                await asyncio.sleep(1.5); config = await settings_col.find_one({"_id": "config"})
                first_name = html.escape(message.from_user.first_name if message.from_user else "User")
                text, btn = await get_home_menu(user_id, first_name, config)
                welcome_pic = config.get("welcome_photo", "none")
                if welcome_pic != "none":
                    await api_send(user_id, text, kb=btn, photo=welcome_pic)
                else:
                    await api_send(user_id, text, kb=btn)
            except SessionPasswordNeeded:
                USER_STATES[user_id]["state"] = "WAITING_PASSWORD"
                await api_edit(msg.chat.id, msg.id, f"{E_SHD} <i><b>Two-Step Verification (2FA) Password Required:</b></i>")
            except Exception as e_otp: 
                USER_STATES.pop(user_id, None)
                await api_edit(msg.chat.id, msg.id, f"{E_WRN} <i><b>Error:</b> {e_otp}</i>")
                try: await temp_client.disconnect()
                except: pass
                
        elif state == "WAITING_PASSWORD":
            user_state_data = USER_STATES.get(user_id, {})
            temp_client = user_state_data.get("temp_client")
            if not temp_client:
                USER_STATES.pop(user_id, None)
                return
                
            msg = await api_send(user_id, f"{E_SYNC} <i>Verifying 2FA password...</i>")
            try:
                await temp_client.check_password(message.text)
                ss = await temp_client.export_session_string()
                
                await update_user_profile(temp_client, user_id)
                await users_col.update_one({"_id": user_id}, {"$push": {"sessions": ss}})
                
                asyncio.create_task(setup_user_ai_listener(user_id, ss))

                await api_edit(msg.chat.id, msg.id, f"{E_CHK} <i><b>Session Connected & Profile Updated!</b>\nRedirecting to Menu...</i>")
                await temp_client.disconnect(); USER_STATES.pop(user_id, None)
                
                await asyncio.sleep(1.5); config = await settings_col.find_one({"_id": "config"})
                first_name = html.escape(message.from_user.first_name if message.from_user else "User")
                text, btn = await get_home_menu(user_id, first_name, config)
                welcome_pic = config.get("welcome_photo", "none")
                if welcome_pic != "none":
                    await api_send(user_id, text, kb=btn, photo=welcome_pic)
                else:
                    await api_send(user_id, text, kb=btn)
            except Exception as e_pwd: 
                USER_STATES.pop(user_id, None)
                await api_edit(msg.chat.id, msg.id, f"{E_WRN} <i><b>Error:</b> {e_pwd}</i>")
                try: await temp_client.disconnect()
                except: pass
            
    except Exception as e_state:
        USER_STATES.pop(user_id, None)
        await api_send(message.chat.id, f"{E_WRN} <i><b>Encountered Error:</b>\n<code>{html.escape(str(e_state))}</code></i>")

# ================= ULTRA-FAST REAL STREAMING MASS DM WORKER =================
async def run_real_mass_dm_campaign(user_id, session_str, messages_list, status_msg_id):
    userbot = None
    try:
        userbot = Client(f"real_mass_dm_{user_id}_{int(time.time())}", api_id=API_ID, api_hash=API_HASH, session_string=session_str, in_memory=True)
        await userbot.start()

        bot_info = await bot.get_me()
        bot_un = bot_info.username
        footer_text = f"\n\nSent by @{bot_un}"

        config = await settings_col.find_one({"_id": "config"})
        has_prem = await is_premium(user_id)
        max_allowed = float('inf') if has_prem else int(config.get("start_mass_dm_limit", 100))

        sent_count = 0
        failed_count = 0
        start_time = time.time()

        if not status_msg_id:
            msg_obj = await api_send(int(user_id), "🔄 <b>Server Reconnected! Resuming Mass DM Campaign instantly...</b>")
            status_msg_id = msg_obj.id

        async for dialog in userbot.get_dialogs():
            u_data = await get_user(user_id)
            if not u_data.get("mass_dm_active", False):
                break

            if dialog.chat and dialog.chat.type == enums.ChatType.PRIVATE and dialog.chat.id != int(user_id):
                target_user_id = dialog.chat.id
                
                try:
                    sent_msg_map = {}
                    for m_idx, m_item in enumerate(messages_list):
                        m_type = m_item.get("type", "text")
                        content = m_item.get("content", "")
                        caption = (m_item.get("caption", "") or "") + footer_text
                        reply_to_idx = m_item.get("reply_to")
                        
                        actual_reply_to_id = None
                        if reply_to_idx is not None and reply_to_idx in sent_msg_map:
                            actual_reply_to_id = sent_msg_map[reply_to_idx]

                        sent_obj = None
                        if m_type == "photo":
                            sent_obj = await userbot.send_photo(target_user_id, photo=content, caption=caption, reply_to_message_id=actual_reply_to_id)
                        elif m_type == "video":
                            sent_obj = await userbot.send_video(target_user_id, video=content, caption=caption, reply_to_message_id=actual_reply_to_id)
                        elif m_type == "document":
                            sent_obj = await userbot.send_document(target_user_id, document=content, caption=caption, reply_to_message_id=actual_reply_to_id)
                        elif m_type == "animation":
                            sent_obj = await userbot.send_animation(target_user_id, animation=content, caption=caption, reply_to_message_id=actual_reply_to_id)
                        elif m_type == "audio":
                            sent_obj = await userbot.send_audio(target_user_id, audio=content, caption=caption, reply_to_message_id=actual_reply_to_id)
                        elif m_type == "voice":
                            sent_obj = await userbot.send_voice(target_user_id, voice=content, caption=caption, reply_to_message_id=actual_reply_to_id)
                        elif m_type == "sticker":
                            sent_obj = await userbot.send_sticker(target_user_id, sticker=content)
                        else:
                            sent_obj = await userbot.send_message(target_user_id, text=(content + footer_text), reply_to_message_id=actual_reply_to_id)

                        if sent_obj:
                            sent_msg_map[m_idx] = sent_obj.id

                    sent_count += 1
                    await users_col.update_one({"_id": user_id}, {"$inc": {"total_dms": len(messages_list), "daily_dms": len(messages_list)}, "$set": {"last_campaign": {"status": "Completed", "sent": sent_count, "total": sent_count + failed_count}}})
                    await settings_col.update_one({"_id": "config"}, {"$inc": {"global_dms": len(messages_list)}})
                except FloodWait as fw:
                    await asyncio.sleep(fw.value)
                    failed_count += 1
                except Exception:
                    failed_count += 1

                if (sent_count + failed_count) % 2 == 0:
                    elapsed = max(0.1, time.time() - start_time)
                    speed = round((sent_count + failed_count) / elapsed, 1)

                    live_text = f"""🚀 <b>Live Streaming Campaign...</b>

──────────────────────
📊 <b>Processed:</b> {sent_count + failed_count}
🟩 <b>Sent:</b> {sent_count} | 🟥 <b>Failed:</b> {failed_count}
⚡ <b>Speed:</b> {speed} msg/sec
📍 <b>Current:</b> 🔍 Delivered to <code>{target_user_id}</code>
──────────────────────"""
                    try:
                        await api_edit(int(user_id), status_msg_id, live_text, kb={"inline_keyboard": [[ibtn("Stop Campaign", "mass_dm_stop_run", style="danger", icon=BTN_EMOJIS["cross"])]]})
                    except:
                        pass

                if sent_count >= max_allowed:
                    break

                await asyncio.sleep(0.3)

        await userbot.stop()
        await users_col.update_one({"_id": user_id}, {"$set": {"mass_dm_active": False}})
        
        report_text = f"""📊 <b>Post-Campaign Analytics Report</b>

🔗 Target: <b>Live Mass DM Campaign</b>
✅ Successfully Queued/Sent: {sent_count}
❌ Failed Blocks: {failed_count}

🚪 Security: Your Telegram session was processed securely."""
        await api_send(int(user_id), report_text)
    except Exception as e:
        if userbot:
            try: await userbot.stop()
            except: pass
        await users_col.update_one({"_id": user_id}, {"$set": {"mass_dm_active": False, "last_campaign": {"status": "Stopped", "sent": 0, "total": 100}}})
        await api_send(int(user_id), f"❌ Campaign Error: <code>{html.escape(str(e))}</code>")

# ================= ULTRA-FAST STREAMING JOIN REQUEST DM WORKER =================
async def run_join_req_dm_campaign(user_id, session_str, target_link, limit, messages_list, status_msg_id, chat_title="CHANNEL"):
    userbot = None
    try:
        userbot = Client(f"join_req_dm_{user_id}_{int(time.time())}", api_id=API_ID, api_hash=API_HASH, session_string=session_str, in_memory=True)
        await userbot.start()

        bot_info = await bot.get_me()
        bot_un = bot_info.username
        footer_text = f"\n\nSent by @{bot_un}"

        chat = await get_chat_safely(userbot, target_link, target_link)

        sent_count = 0
        failed_count = 0
        start_time = time.time()

        if not status_msg_id:
            msg_obj = await api_send(int(user_id), f"🔄 <b>Server Reconnected! Resuming Join Request DM ({chat_title})...</b>")
            status_msg_id = msg_obj.id

        async for req in userbot.get_chat_join_requests(chat.id):
            u_data = await get_user(user_id)
            if not u_data.get("join_req_dm_active", False):
                break

            if req.user and not req.user.is_bot and not req.user.is_deleted:
                target_user_id = req.user.id

                try:
                    sent_msg_map = {}
                    for m_idx, m_item in enumerate(messages_list):
                        m_type = m_item.get("type", "text")
                        content = m_item.get("content", "")
                        caption = (m_item.get("caption", "") or "") + footer_text
                        reply_to_idx = m_item.get("reply_to")
                        
                        actual_reply_to_id = None
                        if reply_to_idx is not None and reply_to_idx in sent_msg_map:
                            actual_reply_to_id = sent_msg_map[reply_to_idx]

                        sent_obj = None
                        if m_type == "photo":
                            sent_obj = await userbot.send_photo(target_user_id, photo=content, caption=caption, reply_to_message_id=actual_reply_to_id)
                        elif m_type == "video":
                            sent_obj = await userbot.send_video(target_user_id, video=content, caption=caption, reply_to_message_id=actual_reply_to_id)
                        elif m_type == "document":
                            sent_obj = await userbot.send_document(target_user_id, document=content, caption=caption, reply_to_message_id=actual_reply_to_id)
                        elif m_type == "animation":
                            sent_obj = await userbot.send_animation(target_user_id, animation=content, caption=caption, reply_to_message_id=actual_reply_to_id)
                        elif m_type == "audio":
                            sent_obj = await userbot.send_audio(target_user_id, audio=content, caption=caption, reply_to_message_id=actual_reply_to_id)
                        elif m_type == "voice":
                            sent_obj = await userbot.send_voice(target_user_id, voice=content, caption=caption, reply_to_message_id=actual_reply_to_id)
                        elif m_type == "sticker":
                            sent_obj = await userbot.send_sticker(target_user_id, sticker=content)
                        else:
                            sent_obj = await userbot.send_message(target_user_id, text=(content + footer_text), reply_to_message_id=actual_reply_to_id)

                        if sent_obj:
                            sent_msg_map[m_idx] = sent_obj.id

                    sent_count += 1
                    await users_col.update_one({"_id": user_id}, {"$inc": {"total_dms": len(messages_list), "daily_dms": len(messages_list)}, "$set": {"last_campaign": {"status": "Completed", "sent": sent_count, "total": limit}}})
                    await settings_col.update_one({"_id": "config"}, {"$inc": {"global_dms": len(messages_list)}})
                except Exception:
                    failed_count += 1

                if (sent_count + failed_count) % 2 == 0:
                    live_text = f"""🚀 Fast Join Req DM Running!

🎯 Target: <b>{chat_title}</b>
📊 Limit: {limit}
⏳ Progress: {sent_count}/{limit} sent
❌ Failed: {failed_count}

Commands: /chk , /pause , /resume , /stop"""
                    try:
                        await api_edit(int(user_id), status_msg_id, live_text, kb={"inline_keyboard": [[ibtn("Stop Campaign", "join_req_dm_stop", style="danger", icon=BTN_EMOJIS["cross"])]]})
                    except:
                        pass

                if sent_count >= limit:
                    break

                await asyncio.sleep(0.5)

        await userbot.stop()
        await users_col.update_one({"_id": user_id}, {"$set": {"join_req_dm_active": False}})
        
        report_text = f"""📊 <b>Post-Campaign Analytics Report</b>

🔗 Target: <b>{chat_title}</b>
✅ Successfully Queued/Sent: {sent_count}
❌ Failed Blocks: {failed_count}

"""
        await api_send(int(user_id), report_text)
    except Exception as e:
        if userbot:
            try: await userbot.stop()
            except: pass
        await users_col.update_one({"_id": user_id}, {"$set": {"join_req_dm_active": False, "last_campaign": {"status": "Stopped", "sent": 0, "total": 100}}})
        await api_send(int(user_id), f"❌ Campaign Error: <code>{html.escape(str(e))}</code>")

@bot.on_callback_query(filters.regex(r"^promoapp_") | filters.regex(r"^promorej_"))
async def promo_approval_callback(client, query: CallbackQuery):
    if not query.from_user or not await _is_admin(query.from_user.id):
        return await query.answer("Access Denied: Admins Only.", show_alert=True)
    
    parts = query.data.split("_")
    action = parts[0]
    target_user_id = parts[1]

    if action == "promoapp":
        members = parts[2]
        payload = {
            "chat_id": query.message.chat.id, 
            "message_id": query.message.id, 
            "caption": (query.message.caption or "") + f"\n\n{E_CHK} <b>CHANNEL PROMO APPROVED BY ADMIN</b>", 
            "parse_mode": "HTML"
        }
        async with aiohttp.ClientSession() as session:
            await session.post(f"https://api.telegram.org/bot{BOT_TOKEN}/editMessageCaption", json=payload)
        
        try:
            await api_send(int(target_user_id), f"🎉 <b>Your Channel Promo Order has been Approved!</b>\nDelivery for <b>{members} members</b> has started successfully.")
        except:
            pass
        await query.answer("Channel Promo Approved!", show_alert=True)
    else:
        payload = {
            "chat_id": query.message.chat.id, 
            "message_id": query.message.id, 
            "caption": (query.message.caption or "") + f"\n\n{E_CROSS} <b>CHANNEL PROMO REJECTED BY ADMIN</b>", 
            "parse_mode": "HTML"
        }
        async with aiohttp.ClientSession() as session:
            await session.post(f"https://api.telegram.org/bot{BOT_TOKEN}/editMessageCaption", json=payload)
        
        try:
            await api_send(int(target_user_id), f"{E_WRN} <i>Your Channel Promo order was rejected by administrator. Please contact support if you faced an issue.</i>")
        except:
            pass
        await query.answer("Channel Promo Rejected.", show_alert=True)

bot.add_handler(MessageHandler(start_cmd, filters.command("start") & filters.private))
bot.add_handler(MessageHandler(shortcut_cmds, filters.command(["buypremium", "massdm", "myaccount"]) & filters.private))
bot.add_handler(MessageHandler(dm_optin_cmd, filters.command("dmoptin") & filters.private))
bot.add_handler(MessageHandler(dm_optout_cmd, filters.command("dmoptout") & filters.private))
bot.add_handler(MessageHandler(check_total_public, filters.command(["total", "totaldms"]) & filters.private))
bot.add_handler(MessageHandler(admin_panel, filters.command("admin")))
bot.add_handler(MessageHandler(admin_redeem_menu_cmd, filters.command("redeem_menu")))
bot.add_handler(MessageHandler(admin_create_redeem_cmd, filters.command("create_redeem")))
bot.add_handler(MessageHandler(admin_admins_menu_cmd, filters.command("admins_menu")))
bot.add_handler(MessageHandler(admin_add_admin_cmd, filters.command("add_admin")))
bot.add_handler(MessageHandler(admin_remove_admin_cmd, filters.command("remove_admin")))
bot.add_handler(MessageHandler(master_admin_cmds, filters.command(["setwebsitelink", "toggleauto", "reqall", "setfampay", "setmanual", "setprice1d", "setprice3d", "setprice7d", "setprice1m", "setlogchannel", "setsuccesslog", "setleaderboard", "setfreechannel", "setfreerequest", "setfreetrial", "setfreereq", "setldbtime", "setrefbonus", "setdelay", "setacceptlimit", "setstartmasslimit", "setjoinreqlimit", "setgmail", "setsupport", "setcreatebotlink", "setpromofactor", "ongoing", "sales", "giveprem", "removeprem", "maintenance"])))
bot.add_handler(MessageHandler(shared_admin_cmds, filters.command(["stats", "checkuser", "clearsession", "banuser", "unbanuser", "broadcast"])))
bot.add_handler(MessageHandler(dm_controls, filters.command(["chk", "pause", "resume", "stop"]) & filters.private))
bot.add_handler(MessageHandler(handle_states, filters.private & filters.incoming), group=1)
bot.add_handler(CallbackQueryHandler(cb_handler))
bot.add_handler(CallbackQueryHandler(admin_manual_approve, filters.regex(r"^manapp_") | filters.regex(r"^manrej_")), group=1)
bot.add_handler(ChatJoinRequestHandler(handle_join_requests))


# ================= RENDER HEALTH SERVER =================
# Render Web Services must listen on the PORT environment variable.
_health_runner = None

async def _render_health(request):
    return web.Response(text="OK", status=200)

async def start_render_health_server():
    global _health_runner
    port = int(os.environ.get("PORT", "10000"))
    app = web.Application()
    app.router.add_get("/", _render_health)
    app.router.add_get("/health", _render_health)

    _health_runner = web.AppRunner(app)
    await _health_runner.setup()
    site = web.TCPSite(_health_runner, host="0.0.0.0", port=port)
    await site.start()
    print(f"RENDER HEALTH SERVER LISTENING ON 0.0.0.0:{port}")

async def stop_render_health_server():
    global _health_runner
    if _health_runner is not None:
        try:
            await _health_runner.cleanup()
        except Exception as exc:
            print(f"Health server cleanup error: {exc}")
        finally:
            _health_runner = None

async def start_bot():
    print("-----------------------------------")
    await init_db()
    print("-----------------------------------")

    # Start the HTTP health server so Render detects an open port.
    await start_render_health_server()

    try:
        await bot.start()
        print(f"BOT STARTED: {(await bot.get_me()).username}")

        # Disabled at startup: restoring every saved user session can trigger Telegram FloodWait
        # and prevent the main bot from responding. Resume sessions through a controlled worker.
        # await restore_user_sessions_and_campaigns()

        asyncio.create_task(auto_leaderboard_task())
        asyncio.create_task(background_ads_worker())
        await idle()
    finally:
        print("Shutting down...")
        try:
            await bot.stop()
        finally:
            await stop_render_health_server()

if __name__ == "__main__":
    asyncio.run(start_bot())
