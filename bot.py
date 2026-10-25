"""
Телеграм-бот для генерации студийных фото товаров одежды (English UI).

Флоу: фото товара → модель → кадр (+ ручные настройки) → нейросеть → результат файлом.
  Кадры: анфас, спина, flat lay, невидимый манекен, город.
  Режимы: перекрас по образцу цвета, реальный фон Винницы (мульти-фото).
  Нейромережа: GPT Image 2 (через PiAPI).

Доступ по заявкам: новый пользователь → заявка админу → одобрение кнопкой.
Команды: /start /help /id, админская /users (список + отзыв доступа).

Обидві нейромережі працюють через PiAPI (один ключ PIAPI_API_KEY). Если PiAPI ограничивает доступ
из Украины — сервер нужно держать за границей (проверьте это отдельно).
Используются EDIT-модели: принимают фото товара и сохраняют его, в отличие от
text-to-image.
Автозапуск — cron (@reboot + watchdog каждые 2 мин).
"""
from __future__ import annotations

import asyncio
import base64
import fcntl
from html import escape
import io
import itertools
import json
import logging
import os
import random
import sys
import uuid
from datetime import date
from pathlib import Path

import aiohttp
from PIL import Image, ImageDraw, ImageFont, ImageOps
from aiogram import Bot, Dispatcher, F
from aiogram.client.default import DefaultBotProperties
from aiogram.filters import Command, CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.utils.chat_action import ChatActionSender
from aiogram.types import (
    BotCommand,
    BotCommandScopeChat,
    BotCommandScopeDefault,
    BufferedInputFile,
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    InputMediaDocument,
    KeyboardButton,
    Message,
    ReactionTypeEmoji,
    ReplyKeyboardMarkup,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger("bot")

FOLDER = Path(__file__).parent
PIAPI_BASE = "https://api.piapi.ai/api/v1"

# Формат кадра. Меняется здесь одним местом ("дальше будем смотреть").
# Итоговый размер: любой результат приводим ровно к нему через Pillow.
TARGET = (1050, 1400)

# ---------------------------------------------------------------------------
# Конфигурация из .env
# ---------------------------------------------------------------------------

def load_env(path):
    values = {}
    if not path.exists():
        return values
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        values[key.strip()] = value.strip().strip('"').strip("'")
    return values


ENV = load_env(FOLDER / ".env")
TG_TOKEN = ENV["TELEGRAM_TOKEN"]
PIAPI_KEY = ENV["PIAPI_API_KEY"]   # PiAPI key for GPT Image 2
ADMIN_ID = int(ENV.get("ADMIN_ID", "0"))  # владелец: получает заявки, всегда с доступом
WATERMARK = ENV.get("WATERMARK", "").strip()  # текст водяного знака; пусто = выкл

# Одобренные пользователи хранятся в файле (переживают перезапуски).
APPROVED_FILE = FOLDER / "approved.json"


USAGE_FILE = FOLDER / "usage.json"

# Денний лічильник теж лежить у файлі: інакше кожен перезапуск обнуляв би його,
# і DAILY_LIMIT обходився б кількома рестартами за добу.
LIMIT_FILE = FOLDER / "limit.json"

# Замок від другого примірника. Два процеси, що одночасно тягнуть getUpdates,
# вибивають один одного (у лозі це TelegramConflictError), і бот то працює,
# то ні. Дескриптор тримаємо відкритим до кінця роботи — ОС відпустить замок
# сама, навіть якщо процес уб'ють kill -9.
LOCK_FILE = FOLDER / "bot.lock"
_lock_fd = None


def load_approved() -> dict:
    """Возвращает {id: {"name":..., "since":...}}. Мигрирует старые форматы."""
    try:
        data = json.loads(APPROVED_FILE.read_text(encoding="utf-8"))
        if isinstance(data, dict):
            out = {}
            for k, v in data.items():
                out[int(k)] = v if isinstance(v, dict) else {"name": str(v), "since": "—"}
            return out
        return {int(x): {"name": str(x), "since": "—"} for x in data}  # старый список id
    except Exception:
        return {}


def load_usage() -> dict:
    try:
        return {int(k): int(v) for k, v in json.loads(USAGE_FILE.read_text(encoding="utf-8")).items()}
    except Exception:
        return {}


def _atomic_write(path: Path, text: str):
    """Атомарний запис: спершу у .tmp, потім заміна. Падіння процесу посеред запису
    більше не залишить напівзаписаний (зламаний) JSON."""
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_text(text, encoding="utf-8")
    tmp.replace(path)


# Відхилені заявки — щоб відхилений користувач не міг слати заявки нескінченно
# (у тому числі після перезапуску бота).
REJECTED_FILE = FOLDER / "rejected.json"


def load_rejected() -> set:
    try:
        return {int(x) for x in json.loads(REJECTED_FILE.read_text(encoding="utf-8"))}
    except Exception:
        return set()


APPROVED: dict = load_approved()   # {telegram_id: {"name": "Имя (@user)", "since": "2026-07-22"}}
PENDING: dict = {}                  # {telegram_id: "Имя (@user)"} — ждут одобрения
USAGE: dict = load_usage()          # {telegram_id: всего_генераций}
REJECTED: set = load_rejected()     # {telegram_id} — відхилені заявки


def save_approved():
    _atomic_write(APPROVED_FILE,
                  json.dumps({str(k): v for k, v in APPROVED.items()}, ensure_ascii=False))


def save_usage():
    _atomic_write(USAGE_FILE, json.dumps({str(k): v for k, v in USAGE.items()}))


def save_rejected():
    _atomic_write(REJECTED_FILE, json.dumps(sorted(REJECTED)))


# Статистика по каждому пользователю: сколько сгенерил каждой моделью и сколько потратил.
STATS_FILE = FOLDER / "stats.json"


def load_stats() -> dict:
    try:
        return {int(k): v for k, v in json.loads(STATS_FILE.read_text(encoding="utf-8")).items()}
    except Exception:
        return {}


STATS: dict = load_stats()   # {telegram_id: {"gpt": n, "gpt_usd": y}} (+ старі nano-поля з попередніх версій)


def save_stats():
    _atomic_write(STATS_FILE,
                  json.dumps({str(k): v for k, v in STATS.items()}, ensure_ascii=False))


def bump_stats(uid: int, model_key: str, price: float):
    """One successful generation: +1 and +price for the given model key."""
    s = STATS.setdefault(uid, {})
    s[model_key] = s.get(model_key, 0) + 1
    s[f"{model_key}_usd"] = round(s.get(f"{model_key}_usd", 0.0) + price, 6)

# Дневной предохранитель от случайного перерасхода
DAILY_LIMIT = 100


def load_limit() -> tuple:
    """Читає денний лічильник із файлу. Якщо там інша доба (або файлу немає) —
    починаємо добу з нуля. Повертає (день, використано)."""
    today = date.today()
    try:
        data = json.loads(LIMIT_FILE.read_text(encoding="utf-8"))
        if data.get("day") == today.isoformat():
            return today, max(0, int(data.get("used", 0)))
    except Exception:
        pass
    return today, 0


_usage_day, _used_today = load_limit()


def save_limit():
    """Запис синхронний — так само, як і зміна лічильника, тож резерв
    лишається неподільним (див. _reserve)."""
    try:
        _atomic_write(LIMIT_FILE,
                      json.dumps({"day": _usage_day.isoformat(), "used": _used_today}))
    except Exception:
        # Не даємо збою запису зірвати вже оплачену генерацію: лічильник
        # у памʼяті лишається правильним до кінця роботи процесу.
        log.exception("не вдалося зберегти денний лічильник")


def _rollover():
    """Сбрасываем дневной счётчик при смене суток."""
    global _used_today, _usage_day
    if date.today() != _usage_day:
        _usage_day = date.today()
        _used_today = 0
        save_limit()


def _reserve(n: int) -> bool:
    """Атомарно (без await) резервує n генерацій у денному ліміті.
    Резерв робиться ДО запуску, тож два одночасні запуски не перевищать ліміт."""
    global _used_today
    _rollover()
    if _used_today + n > DAILY_LIMIT:
        return False
    _used_today += n
    save_limit()
    return True


def _refund(n: int):
    """Повертає в ліміт генерації, що не відбулися (помилка / скасування)."""
    global _used_today
    _rollover()
    _used_today = max(0, _used_today - n)
    save_limit()

# GPT Image 2 via PiAPI.
# 1056x1408 is exactly 3:4 and both sides are divisible by 16 (gpt-image-2 requirement),
# so fit_to_target() won't crop the head/feet. If PiAPI rejects it with HTTP 400,
# set GPT_SIZE=1024x1536 in .env (2:3, slightly cropped top/bottom).
GPT_MODEL_ID = "gpt-image-2"
GPT_SIZE = ENV.get("GPT_SIZE", "1056x1408")
GPT_QUALITY = ENV.get("GPT_QUALITY", "medium")   # low / medium / high
# УВАГА: 0.05 — заглушка. Впишіть реальну ціну з кабінету PiAPI у .env (GPT_PRICE=...),
# інакше витрати GPT у /profile рахуються неточно.
GPT_PRICE = float(ENV.get("GPT_PRICE", "0.05"))
# Поки ціну не задано, суми по GPT — оцінка по заглушці. Прапорець потрібен,
# щоб /profile чесно про це писав, а не видавав здогад за факт.
GPT_PRICE_IS_STUB = "GPT_PRICE" not in ENV
if GPT_PRICE_IS_STUB:
    logging.getLogger("bot").warning(
        "GPT_PRICE не задано в .env — використовується заглушка $0.05, облік витрат GPT неточний")

# Edit model (takes the product photo as input).
# key — stats key, price — per generation (button + spend accounting).
MODELS = {
    "gpt": {"key": "gpt", "id": GPT_MODEL_ID,
            "label": "🤖 GPT Image 2", "price": GPT_PRICE},
}


def model_price(model_id: str) -> float:
    """Estimated price per generation for the given model."""
    for m in MODELS.values():
        if m["id"] == model_id:
            return m["price"]
    return 0.0

# ---------------------------------------------------------------------------
# Пресеты сцен с рандомизацией.
# У каждой сцены — шаблон с {плейсхолдерами} и пулы вариаций (vary).
# На каждый прогон compose() случайно собирает новый промпт, поэтому кадры
# не однотипные. Блок KEEP (сохранить товар) остаётся неизменным всегда.
# ---------------------------------------------------------------------------

KEEP = ("Preserve EXACTLY the garment colour, pattern, print scale and placement, "
        "cut, length, neckline, seams, buttons, zippers, fabric texture and logo "
        "from the reference. Do not alter the garment. Do not add text or accessories.")

# Хвост на фотореализм. Без упоминаний «кожи/пор/несовершенств» — их блокируют
# контент-фильтры моделей. Реализм несут камера/ткань/свет.
QUALITY_BOOST = (
    "Hyper-realistic authentic photograph, shot on a full-frame DSLR with an 85mm "
    "lens, natural true-to-life lighting and soft natural contrast, realistic fabric "
    "weave and materials, lifelike natural detail, natural depth of field, natural "
    "colour grading. A real candid photo — not a 3D render, not an illustration, "
    "not CGI, no HDR, no oversharpening, no plastic or over-processed look."
)

# Доп. хвост для сцен с моделью — проф-съёмка. Формулировки нейтральные:
# «editorial/body language/posing» триггерят контент-фильтр Google.
STYLE_BOOST = (
    "Professional studio fashion photography, clean polished composition, "
    "flattering even lighting, catalog quality."
)

# Хвіст для сцен З МОДЕЛЛЮ (front/back/city): гарантує, що тіло повністю в кадрі
# (не обрізане знизу/зверху/збоку) і композиція виглядає природно й гармонійно.
NATURAL_FRAMING = (
    "The entire body from head to feet is fully visible within the frame, with "
    "no cropping of the head, hands or feet. Natural, harmonious body proportions "
    "and balanced composition, well-centred framing with comfortable margins."
)

# Хвіст для сцен БЕЗ МОДЕЛІ (flatlay/ghost): чистий фон без випадкових ілюзій
# кінцівок (складки тканини чи тіні, що нагадують руки/ноги/тіло людини).
CLEAN_BG_NO_LIMBS = (
    "The background is perfectly clean and empty, with absolutely no illusion of "
    "hands, arms, legs, feet or any other body parts formed by shadows, folds or "
    "reflections. Only the garment is visible against a clean, uncluttered background."
)

# --- Общие пулы вариаций ---
_CAMERA = [
    "shot on an 85mm lens at eye level",
    "shot on a 50mm lens from a slightly low angle",
    "shot on a 70mm lens, straight-on",
    "shot on a 105mm lens with subtle compression",
]
_EXPR = [
    "a calm neutral expression",
    "a soft natural smile",
    "a serene confident look",
    "a relaxed candid expression",
]
_MODEL = [
    "European female model in her mid-20s, slim, light-warm skin, dark blonde hair in a low bun, natural makeup",
    "European female model in her mid-20s, slim, fair skin, light-brown hair in a sleek low ponytail, minimal makeup",
    "European female model in her late-20s, slim, warm skin tone, soft wavy blonde hair, blue eyes, natural makeup",
    "European female model in her mid-20s, athletic-slim build, chestnut hair pulled back, neutral makeup",
]
_MODEL_BACK = [
    "European female model in her mid-20s, slim, dark blonde hair in a tight bun",
    "European female model in her mid-20s, slim, light-brown hair in a high ponytail swept aside",
    "European female model in her late-20s, slim, blue eyes, hair pinned up off the back",
]
_BG = ["light neutral-grey", "soft warm-white", "pale sand-beige", "cool light-grey", "clean off-white"]
_LIGHT_STUDIO = [
    "a soft large softbox from the front-left with gentle fill",
    "bright even high-key lighting with minimal shadows",
    "a warm key light from the right with a soft shadow on the left",
    "diffused overhead beauty lighting, clean and soft",
    "soft directional light creating a gentle body shadow",
]
_CROP = ["Full-body framing", "Full-body shot with slight headroom", "Three-quarter-length framing"]

# Короткі підписи кнопок для ручного вибору кадрування/фону/світла (паралельні
# масиви до _CROP / _BG / _LIGHT_STUDIO вище, той самий порядок і довжина).
CROP_LABELS = ["📐 Full length", "📐 Full length (headroom)", "📐 Three-quarter length"]
BG_LABELS = ["⬜ Light grey", "⬜ Warm white", "🟫 Sand beige", "⬜ Cool grey", "⬜ Off-white"]
LIGHT_LABELS = ["💡 Softbox from the left", "💡 Bright and even", "💡 Warm from the right",
                "💡 Soft overhead", "💡 Directional with shadow"]

# --- Пулы под конкретные сцены ---
# Женские позы — анфас (профессиональный fashion-позинг)
_POSE_FRONT_F = [
    "an elegant relaxed stance, weight on one leg, one hand resting on the hip",
    "a graceful editorial pose, one leg slightly in front of the other, chin lifted, confident gaze",
    "turning gently toward the camera, hands relaxed",
    "an elongated poised posture, one hand relaxed at the side, shoulders back",
    "a fashion-magazine stance, weight shifted, subtle turn of the shoulders, relaxed hands",
    "a walking runway pose mid-stride with natural movement and confident energy",
]
# Мужские позы — анфас
_POSE_FRONT_M = [
    "a confident wide stance, weight evenly balanced, shoulders back, hands relaxed at the sides",
    "one hand in a pocket, torso turned slightly, strong relaxed posture, direct gaze",
    "both hands in pockets, shoulders squared, chin level, an assured editorial stance",
    "arms crossed over the chest, grounded and confident, looking straight at the camera",
    "one hand adjusting a cuff, weight on one leg, sharp magazine-style posture",
    "a strong walking pose mid-stride with natural movement and runway energy",
]
# Женские позы — спина
_POSE_BACK_F = [
    "an elegant stance, weight on one leg, head turned over the shoulder with a soft gaze",
    "one hand on the hip, torso slightly angled, looking back over the shoulder",
    "standing tall, shoulders open, head gently turned to reveal the profile",
    "a poised upright posture, hands relaxed, glancing over the shoulder",
]
# Мужские позы — спина
_POSE_BACK_M = [
    "strong squared shoulders, head turned to reveal the profile",
    "a confident stance, one hand in a pocket, glancing over the shoulder",
    "weight balanced, head turned slightly, an assured editorial back pose",
    "shoulders open and relaxed, looking over the shoulder with a composed expression",
]
_ARRANGE = [
    "neatly arranged and centered", "laid flat and fully spread out",
    "styled with soft natural folds", "arranged with the sleeves gently posed",
]
_TOP_LIGHT = [
    "soft even top light", "gentle directional morning light with a soft shadow",
    "bright diffused daylight from above",
]
_FL_ANGLE = ["shot perfectly top-down", "shot from directly above with a slight tilt"]

# Короткі підписи для ручного вибору Flat lay (паралельно до _ARRANGE / _FL_ANGLE).
ARRANGE_LABELS = ["🧺 Neat and centred", "🧺 Laid out flat", "🧺 Soft folds", "🧺 Sleeves posed"]
ANGLE_LABELS = ["📷 Straight top-down", "📷 Top-down, tilted"]

_GHOST_ANGLE = ["seen straight from the front", "seen from a subtle three-quarter angle"]
GHOST_ANGLE_LABELS = ["👤 Straight from the front", "👤 Three-quarter angle"]
_LOCATION = [
    "a quiet European old-town street with low-rise facades",
    "a cobblestone alley with vintage shopfronts",
    "a sunlit riverside promenade",
    "a leafy boulevard with cafe terraces",
    "a clean modern city square",
    "a historic square with soft pastel buildings",
]
_DAYLIGHT = [
    "soft warm morning light", "golden-hour side light",
    "bright overcast daylight", "gentle late-afternoon sun",
]
LOCATION_LABELS = ["🏘 Old town", "🧱 Cobblestone alley", "🌅 Riverside promenade",
                    "🌳 Boulevard with cafés", "🏙 Modern square", "🏛 Historic square"]
DAYLIGHT_LABELS = ["🌤 Morning light", "🌇 Golden hour", "☁️ Overcast day", "🌆 Afternoon sun"]
# Позначки поз "У місті" — узагальнені (порядок відповідає POSE_POOLS[gender]["city"],
# де індекси 0-3 мають однаковий сенс і для жіночого, і для чоловічого пулу).
CITY_POSE_LABELS = ["🚶 Walking to camera", "🤚 Paused, hand in pocket",
                    "😌 Relaxed stance", "📸 Candid, caught mid-moment"]
# Позначки «пози волосся» для сцени «Спина» (3 варіанти в MODEL_POOLS[gender]["back"],
# однакові за кількістю в усіх 4 категоріях — узагальнені підписи, бо конкретний
# опис (пучок/хвіст/підколоте, або просто інша статура для чоловіків) різниться).
# Женские позы — город
_POSE_CITY_F = [
    "walking confidently toward the camera with a natural stride, natural street-style energy",
    "pausing mid-step, one hand in a pocket, glancing to the side with an effortless look",
    "a relaxed contrapposto, weight on one leg, looking off-camera",
    "turning gently as if caught candidly, confident and natural street-fashion feel",
]
# Мужские позы — город
_POSE_CITY_M = [
    "walking toward the camera with a confident stride, natural street-style energy",
    "standing with one hand in a pocket, torso angled, looking to the side with an assured look",
    "pausing mid-step, adjusting a jacket cuff, natural candid street pose",
    "a grounded relaxed stance, hands in pockets, confident and effortless",
]

# Мужские модели
_MODEL_M = [
    "European male model in his mid-20s, athletic-slim build, short dark hair, clean groomed look, light stubble",
    "European male model in his late-20s, slim build, short light-brown hair, clean-shaven, natural look",
    "European male model in his mid-20s, fit build, short styled hair and a short beard",
    "European male model in his late-20s, athletic build, brown hair, relaxed natural look",
    "European male model in his mid-20s, athletic-slim build, short blond hair, blue eyes, clean-shaven, natural look",
]
# Вигляд зі спини — ТОЙ САМИЙ порядок і кількість, що й у _MODEL_M, щоб model_idx
# давав ту саму людину (колір волосся, статура) і в «Анфас», і в «Спина».
_MODEL_BACK_M = [
    "European male model in his mid-20s, athletic-slim build, short dark hair",
    "European male model in his late-20s, slim build, short light-brown hair",
    "European male model in his mid-20s, fit build, short styled dark-brown hair",
    "European male model in his late-20s, athletic build, brown hair",
    "European male model in his mid-20s, athletic-slim build, short blond hair",
]

# Plus size моделі
_MODEL_F_PLUS = [
    "European plus-size female model in her mid-20s, curvy full figure, "
    "light-warm skin, dark blonde hair in a low bun, natural makeup",
    "European plus-size female model in her mid-20s, curvy full figure, "
    "fair skin, light-brown hair in a sleek low ponytail, minimal makeup",
    "European plus-size female model in her late-20s, curvy full figure, "
    "warm skin tone, soft wavy blonde hair, blue eyes, natural makeup",
    "European plus-size female model in her mid-20s, curvy full figure, "
    "chestnut hair pulled back, neutral makeup",
]
_MODEL_BACK_F_PLUS = [
    "European plus-size female model in her mid-20s, curvy full figure, "
    "dark blonde hair in a tight bun",
    "European plus-size female model in her mid-20s, curvy full figure, "
    "light-brown hair in a high ponytail swept aside",
    "European plus-size female model in her late-20s, curvy full figure, "
    "blue eyes, hair pinned up off the back",
]
_MODEL_M_PLUS = [
    "European plus-size male model in his mid-20s, broad heavyset build, "
    "short dark hair, clean groomed look, light stubble",
    "European plus-size male model in his late-20s, broad heavyset build, "
    "short light-brown hair, clean-shaven, natural look",
    "European plus-size male model in his mid-20s, broad heavyset build, "
    "short styled hair and a short beard",
    "European plus-size male model in his late-20s, broad heavyset build, "
    "brown hair, relaxed natural look",
    "European plus-size male model in his mid-20s, broad heavyset build, "
    "short blond hair, blue eyes, clean-shaven, natural look",
]
# Той самий порядок і кількість, що й у _MODEL_M_PLUS (див. коментар до _MODEL_BACK_M).
_MODEL_BACK_M_PLUS = [
    "European plus-size male model in his mid-20s, broad heavyset build, short dark hair",
    "European plus-size male model in his late-20s, broad heavyset build, short light-brown hair",
    "European plus-size male model in his mid-20s, broad heavyset build, short styled dark-brown hair",
    "European plus-size male model in his late-20s, broad heavyset build, brown hair",
    "European plus-size male model in his mid-20s, broad heavyset build, short blond hair",
]

# --- Спина (лише жіночі категорії): колір волосся й статура ЗАВЖДИ беруться з тієї
# самої моделі, що обрана на початку (за model_idx, тим самим індексом, що й у "front"-
# пулі), а ПОЗУ ВОЛОССЯ (пучок/хвіст/підколоте) можна обрати вручну окремо — щоб колір
# ніколи не «розʼїжджався» з тим, що людина вже вибрала. Для чоловіків це не потрібно
# (там немає окремого поняття «пози волосся») — вони й далі йдуть через MODEL_POOLS.
_FEMALE_AGE_BUILD = {
    "female":      ["mid-20s, slim", "mid-20s, slim", "late-20s, slim", "mid-20s, athletic-slim build"],
    "female_plus": ["mid-20s, curvy full figure", "mid-20s, curvy full figure",
                     "late-20s, curvy full figure", "mid-20s, curvy full figure"],
}
_FEMALE_HAIR_COLOR = ["dark blonde", "light-brown", "blonde", "chestnut"]   # той самий порядок, що в _MODEL/_MODEL_F_PLUS
_BACKSTYLE_POSITIONS = [
    "in a tight bun",
    "in a high ponytail swept aside",
    "pinned up off the back",
]
BACKSTYLE_POSITION_LABELS = ["🎗 Tight bun", "🎗 High ponytail, swept aside", "🎗 Pinned up"]


def back_style_text(gender: str, model_idx: int, pos_idx: int) -> str:
    """Опис моделі зі спини: колір волосся й статура — від обраної моделі (model_idx),
    поза волосся — від окремого ручного вибору (pos_idx). Лише для female/female_plus."""
    prefix = "European plus-size female model" if gender == "female_plus" else "European female model"
    age_build = _FEMALE_AGE_BUILD[gender][model_idx % 4]
    color = _FEMALE_HAIR_COLOR[model_idx % 4]
    position = _BACKSTYLE_POSITIONS[pos_idx % 3]
    return f"{prefix} in her {age_build}, {color} hair {position}"


def backstyle_pos_keyboard() -> InlineKeyboardMarkup:
    """Ручний вибір пози волосся для «Спина» (лише жіночі категорії)."""
    return _labeled_keyboard(BACKSTYLE_POSITION_LABELS, "backpos",
                             extra_button=("🎲 Random", "backpos:random"))


def fullbackpos_keyboard() -> InlineKeyboardMarkup:
    """Те саме, але для «Повного комплекту» — з опцією згенерувати всі три (3 картки Спини)."""
    return _labeled_keyboard(BACKSTYLE_POSITION_LABELS, "fullbackpos",
                             extra_button=("🎲 All three (3 cards)", "fullbackpos:all"))


# Пул описаний модели по полу и типу кадра (обычный / со спины)
MODEL_POOLS = {
    "female": {"front": _MODEL, "back": _MODEL_BACK},
    "male":   {"front": _MODEL_M, "back": _MODEL_BACK_M},
    "female_plus": {"front": _MODEL_F_PLUS, "back": _MODEL_BACK_F_PLUS},
    "male_plus":   {"front": _MODEL_M_PLUS, "back": _MODEL_BACK_M_PLUS},
}

# Підписи кнопок вибору конкретної моделі (за кольором волосся) — порядок
# відповідає порядку елементів у "front"-пулі кожної категорії вище.
MODEL_LABELS = {
    "female":      ["🟤 Dark blonde", "🟠 Light brown", "🟡 Blonde", "🟫 Chestnut"],
    "male":        ["⚫ Dark hair", "🟠 Light brown", "🧔 Chestnut, with beard", "🟫 Chestnut, no beard", "🟡 Blonde"],
    "female_plus": ["🟤 Dark blonde", "🟠 Light brown", "🟡 Blonde", "🟫 Chestnut"],
    "male_plus":   ["⚫ Dark hair", "🟠 Light brown", "🧔 Chestnut, with beard", "🟫 Chestnut, no beard", "🟡 Blonde"],
}

# «Родина» кольору волосся для кожної моделі (паралельно до MODEL_LABELS за індексом) —
# потрібна, щоб згрупувати моделі всіх 4 категорій на одному екрані вибору за кольором.
MODEL_FAMILY = {
    "female":      ["dark", "light_brown", "blonde_blue", "chestnut"],
    "male":        ["dark", "light_brown", "chestnut", "chestnut", "blonde_blue"],
    "female_plus": ["dark", "light_brown", "blonde_blue", "chestnut"],
    "male_plus":   ["dark", "light_brown", "chestnut", "chestnut", "blonde_blue"],
}
FAMILY_ORDER = ["dark", "light_brown", "chestnut", "blonde_blue"]
FAMILY_TITLES = {
    "dark":         "⚫ Dark hair",
    "light_brown":  "🟠 Light / dark blonde",
    "chestnut":     "🟫 Chestnut / brown",
    "blonde_blue":  "🟡 Blonde",
}
CATEGORY_ORDER = ["female", "male", "female_plus", "male_plus"]
CATEGORY_SHORT = {
    "female": "👩 Woman",
    "male": "👨 Man",
    "female_plus": "👩 Plus",
    "male_plus": "👨 Plus",
}

# Пул поз по полу и типу сцены (анфас / спина / город)
POSE_POOLS = {
    "female": {"front": _POSE_FRONT_F, "back": _POSE_BACK_F, "city": _POSE_CITY_F},
    "male":   {"front": _POSE_FRONT_M, "back": _POSE_BACK_M, "city": _POSE_CITY_M},
    "female_plus": {"front": _POSE_FRONT_F, "back": _POSE_BACK_F, "city": _POSE_CITY_F},
    "male_plus":   {"front": _POSE_FRONT_M, "back": _POSE_BACK_M, "city": _POSE_CITY_M},
}
# Короткое описание для промптов без пулов (город с реальным фоном)
MODEL_DESC = {
    "female": "European female model, mid-20s, slim",
    "male":   "European male model, mid-20s, athletic-slim build",
    "female_plus": "European plus-size female model, mid-20s, curvy full figure",
    "male_plus":   "European plus-size male model, mid-20s, broad heavyset build",
}

PRESETS = {
    "front": {
        "title": "🧍 Front · studio",
        "template": (
            "High-end e-commerce fashion photo. Dress a {model}, wearing the exact "
            "garment from the reference image. {crop}, {pose}, facing the camera with "
            "{expr}, standing on a seamless {bg} studio cyclorama. Lit with {light}. "
            "{camera}. Sharp focus on the garment texture, photorealistic, clean "
            "catalog quality. " + NATURAL_FRAMING + " " + KEEP
        ),
        "model_type": "front",
        "pose_set": "front",
        "vary": {"crop": _CROP, "expr": _EXPR, "bg": _BG,
                 "light": _LIGHT_STUDIO, "camera": _CAMERA},
    },
    "back": {
        "title": "🔙 Back · studio",
        "template": (
            "High-end e-commerce fashion photo. Dress a {model}, wearing the exact "
            "garment from the reference image. Full body, back to the camera, {pose}, "
            "standing on a seamless {bg} studio cyclorama. The garment back is fully "
            "visible and unobstructed by hair. Lit with {light}. {camera}. Sharp focus "
            "on the garment texture, photorealistic, clean catalog quality. "
            + NATURAL_FRAMING + " " + KEEP
        ),
        "model_type": "back",
        "pose_set": "back",
        "vary": {"bg": _BG, "light": _LIGHT_STUDIO, "camera": _CAMERA},
    },
    "flatlay": {
        "title": "📐 Flat lay",
        "template": (
            "Professional e-commerce flat-lay photo. The exact garment from the "
            "reference, {arrange}, {angle} on a seamless {bg} background. Lit with "
            "{light}. Clean minimal styling, subtle natural shadow, photorealistic "
            "catalog quality. " + CLEAN_BG_NO_LIMBS + " " + KEEP
        ),
        "vary": {"arrange": _ARRANGE, "angle": _FL_ANGLE,
                 "bg": _BG, "light": _TOP_LIGHT},
    },
    "ghost": {
        "title": "👤 Ghost mannequin",
        "template": (
            "Ghost-mannequin e-commerce product photo. The exact garment from the "
            "reference holds a natural three-dimensional body volume with NO visible "
            "mannequin and NO person, the hollow interior of the neckline visible at "
            "the back, {angle}. Seamless {bg} background, lit with {light}. "
            "Photorealistic. No mannequin, no person, no hanger. "
            + CLEAN_BG_NO_LIMBS + " " + KEEP
        ),
        "vary": {"angle": _GHOST_ANGLE, "bg": _BG, "light": _LIGHT_STUDIO},
    },
    "city": {
        "title": "🌆 In the city",
        "template": (
            "Clean street-fashion lookbook photo. Dress a {model}, wearing the "
            "exact garment from the reference image. Full-body, standing in the "
            "foreground on {location}, the background gently blurred with bokeh keeping "
            "full focus on the garment. {pose}. Lit with {light} that matches the "
            "ambient light on the model. {camera}. Photorealistic. "
            + NATURAL_FRAMING + " " + KEEP
        ),
        "model_type": "front",
        "pose_set": "city",
        "vary": {"location": _LOCATION, "light": _DAYLIGHT, "camera": _CAMERA},
    },
}


def compose(key: str, gender: str = "female", style: dict | None = None) -> str:
    """Случайно собирает промпт сцены из пулов вариаций — кадры не однотипные.

    style — опциональный «профиль сессии» для согласованности между кадрами
    (используется в «Повному комплекті», щоб усі кадри мали одну модель
    і один стиль). Формат: {"model_idx": int, "bg": str, "light": str,
    "camera": str, "pose": str, "__force__": {axis: value}}.
    Значення з style підставляється лише тоді, коли воно реально присутнє
    у пулі варіацій цієї конкретної сцени (інакше — звичайний випадковий
    вибір). Ключ "__force__" підставляє значення БЕЗ такої перевірки —
    для навмисних міжсценових підмін (напр. однаковий «фон» у Flat lay,
    де вісь називається інакше — «surface»).
    """
    p = PRESETS[key]
    force = (style or {}).get("__force__") or {}
    fields = {}
    for axis, opts in p["vary"].items():
        if axis in force:
            fields[axis] = force[axis]
        elif style and axis in style and style[axis] in opts:
            fields[axis] = style[axis]
        else:
            fields[axis] = random.choice(opts)
    mt = p.get("model_type")           # тип модели в кадре (front/back) или нет модели
    if mt:
        if mt == "back" and gender in ("female", "female_plus"):
            # «Спина» (жіночі категорії): колір/статура — від обраної моделі (model_idx),
            # поза волосся — від окремого ручного вибору (back_pos_idx). Розділено навмисно,
            # щоб колір волосся завжди збігався з тим, що обрано на початку.
            model_idx = style["model_idx"] if style and "model_idx" in style else random.randrange(4)
            pos_idx = style["back_pos_idx"] if style and "back_pos_idx" in style else random.randrange(3)
            fields["model"] = back_style_text(gender, model_idx, pos_idx)
        else:
            pool = MODEL_POOLS[gender][mt]
            if style and "model_idx" in style:
                fields["model"] = pool[style["model_idx"] % len(pool)]
            else:
                fields["model"] = random.choice(pool)
    ps = p.get("pose_set")             # пул поз по полу и типу сцены
    if ps:
        pose_pool = POSE_POOLS[gender][ps]
        if style and "pose" in style and style["pose"] in pose_pool:
            fields["pose"] = style["pose"]
        else:
            fields["pose"] = random.choice(pose_pool)
    tail = QUALITY_BOOST + (" " + STYLE_BOOST if ps else "")
    return p["template"].format(**fields) + " " + tail


# Промпт перекраса. Два входных фото: товар (первое) + образец цвета (второе).
# KEEP здесь НЕ используется — он запрещает менять цвет, а нам нужно наоборот.
RECOLOR_PROMPT = (
    "There are two input images. The FIRST image is a garment. The SECOND image is a "
    "colour sample. Recolour the garment from the FIRST image so its colour exactly "
    "matches the colour shown in the SECOND image. Change ONLY the garment colour. "
    "Keep everything else identical: the exact cut, shape, length, neckline, seams, "
    "buttons, zippers, fabric texture, folds, any print or pattern layout, the pose, "
    "background and lighting. Apply the new colour naturally with realistic shading, "
    "highlights and fabric texture. " + QUALITY_BOOST
)

# Цели перекраса: key -> (укр. лейбл кнопки, англ. описание цели для промпта).
# Порядок словаря = порядок кнопок в клавиатуре.
RECOLOR_TARGETS = {
    "tshirt": ("👕 T-shirt", "t-shirt"),
    "shirt":  ("👔 Shirt", "shirt or blouse"),
    "kofta":  ("🧶 Sweater", "sweater, hoodie or jumper"),
    "jacket": ("🧥 Jacket", "jacket or coat"),
    "pants":  ("👖 Trousers", "trousers or jeans"),
    "shorts": ("🩳 Shorts", "shorts"),
    "skirt":  ("👗 Skirt", "skirt"),
    "suit":   ("🤵 Suit", "suit"),
    "hat":    ("🧢 Hat", "hat or beanie"),
    "scarf":  ("🧣 Scarf", "scarf"),
    "bag":    ("👜 Bag", "bag"),
}


def recolor_prompt(target_desc) -> str:
    """Промпт перекраса. target_desc=None → красим весь товар (старое поведение)."""
    if not target_desc:
        return RECOLOR_PROMPT
    head = (
        "There are two input images. The FIRST image shows the product, which may "
        "contain several garments or items. The SECOND image is a colour sample. "
        f"Recolour ONLY the {target_desc} in the FIRST image so its colour exactly "
        "matches the colour shown in the SECOND image. Do NOT change the colour of any "
        "other garment, item, accessory, skin, hair or background — every other element "
        "must keep its original colour. Keep the recoloured item otherwise identical: "
        "the exact cut, shape, length, seams, buttons, zippers, fabric texture, folds, "
        "any print or pattern layout, the pose, background and lighting. Apply the new "
        "colour naturally with realistic shading, highlights and fabric texture. "
    )
    return head + QUALITY_BOOST

# Промпт «реальный фон города». Два входных фото: товар (первое) + фото улицы (второе).
# {model_desc} подставляется по полу.
CITYBG_PROMPT = (
    "There are two input images. The FIRST image is a garment. The SECOND image is a "
    "real city street location. Dress a {model_desc}, in the exact garment from the "
    "FIRST image, and place the model standing in the foreground of the real location "
    "from the SECOND image. Keep the garment exactly as in the first image — colour, "
    "cut, length, fabric texture and all details unchanged. Blend the model naturally "
    "into the scene with matching perspective and lighting that fits the location's "
    "daylight. Gently blur the background with bokeh so the garment stays in focus "
    "while the location remains recognisable. Full-body clean street-fashion "
    "photo. " + QUALITY_BOOST + " " + STYLE_BOOST
)

# ---------------------------------------------------------------------------
# Клиент PiAPI (GPT Image 2)
# ---------------------------------------------------------------------------

async def _piapi_submit(session: aiohttp.ClientSession, path: str, payload: dict) -> str:
    """POST JSON-задачи на PiAPI, возвращает task_id. path — относительный путь (с ведущим /)."""
    headers = {"X-API-Key": PIAPI_KEY, "Content-Type": "application/json"}
    async with session.post(f"{PIAPI_BASE}{path}",
                            json=payload, headers=headers,
                            timeout=aiohttp.ClientTimeout(total=60)) as r:
        raw = await r.text()
        if r.status != 200:
            # 401 — недійсний ключ, 402 — вичерпано баланс, 400 — поганий запит тощо
            raise RuntimeError(f"PiAPI HTTP {r.status}: {raw[:500]}")
        try:
            body = json.loads(raw)
        except Exception:
            raise RuntimeError(f"PiAPI повернув не-JSON відповідь (status {r.status}): {raw[:500]}")

    # Документований формат — task_id в корені; деякі маршрути PiAPI
    # загортають задачу в "data" — перевіряємо обидва варіанти.
    task = body.get("data") if isinstance(body.get("data"), dict) else body
    task_id = task.get("task_id") or task.get("id")
    if not task_id:
        raise RuntimeError(f"нет task_id в ответе PiAPI: {body}")
    return task_id


async def _piapi_wait(session: aiohttp.ClientSession, task_id: str) -> dict:
    """Опрашивает GET /task/{id} до completed/failed. Возвращает dict output. 100×3с ≈ 5 мин."""
    headers = {"X-API-Key": PIAPI_KEY}
    for _ in range(100):
        await asyncio.sleep(3)
        async with session.get(f"{PIAPI_BASE}/task/{task_id}",
                               headers=headers,
                               timeout=aiohttp.ClientTimeout(total=30)) as r:
            raw = await r.text()
            if r.status != 200:
                raise RuntimeError(f"PiAPI HTTP {r.status} (task): {raw[:500]}")
            try:
                body = json.loads(raw)
            except Exception:
                raise RuntimeError(f"PiAPI повернув не-JSON відповідь (task, status {r.status}): {raw[:500]}")

        # PiAPI може віддавати задачу в корені або в обгортці "data" — перевіряємо обидва варіанти.
        task = body.get("data") if isinstance(body.get("data"), dict) else body
        status = (task.get("status") or "").lower()

        if status in ("failed", "error"):
            out = task.get("output") if isinstance(task.get("output"), dict) else {}
            # Без повного тіла задачі: у ньому є вхідні картинки (base64 на сотні КБ),
            # які лише засмічують лог. Беремо тільки текст помилки і обрізаємо.
            err = out.get("error_body") or task.get("error") or out.get("error") or "невідома помилка"
            raise RuntimeError(f"генерация не удалась (PiAPI): {str(err)[:800]}")

        if status in ("completed", "succeeded", "success"):
            return task.get("output") or {}

        if status not in ("pending", "processing", "staged", "queued", "running", ""):
            raise RuntimeError(f"PiAPI: неизвестный статус задачи: {status}")

    raise TimeoutError("модель не ответила вовремя")


def _extract_image(output) -> str:
    """Pull the first image (URL or base64) out of a PiAPI task output.
    Different PiAPI models return different shapes, so check the common ones."""
    candidates = []
    if isinstance(output, dict):
        if output.get("image_url"):
            return output["image_url"]
        candidates = (output.get("image_urls") or output.get("data")
                      or output.get("images") or [])
    elif isinstance(output, list):
        candidates = output
    for item in candidates:
        if isinstance(item, str) and item:
            return item
        if isinstance(item, dict):
            if item.get("url"):
                return item["url"]
            if item.get("image_url"):
                return item["image_url"]
            if item.get("b64_json"):
                return "data:image/png;base64," + item["b64_json"]
    raise RuntimeError(f"PiAPI: no image in task output: {str(output)[:800]}")


async def _generate_gpt(session: aiohttp.ClientSession, imgs: list, prompt: str) -> str:
    """GPT Image 2 via PiAPI (async images/edits endpoint, polled through /task/{id})."""
    payload = {
        "model": GPT_MODEL_ID,
        "prompt": prompt,
        "size": GPT_SIZE,
        "quality": GPT_QUALITY,
        "n": 1,
        "image": imgs,
    }
    task_id = await _piapi_submit(session, "/images/edits/async", payload)
    return _extract_image(await _piapi_wait(session, task_id))


async def generate(session: aiohttp.ClientSession, model_id: str,
                   imgs: list, prompt: str) -> str:
    """Run the task, return the image URL (or data-URI)."""
    return await _generate_gpt(session, imgs, prompt)


async def download(session: aiohttp.ClientSession, url: str) -> bytes:
    if url.startswith("data:"):
        # PiAPI иногда возвращает картинку base64-строкой вместо URL.
        _, _, b64data = url.partition(",")
        return base64.b64decode(b64data)
    async with session.get(url, timeout=aiohttp.ClientTimeout(total=60)) as r:
        if r.status != 200:
            raise RuntimeError(f"не вдалося завантажити результат: HTTP {r.status}")
        return await r.read()


_FONT_PATHS = [
    "/usr/share/fonts/truetype/dejavu/DejaVuSerif-Bold.ttf",
    "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
]


def _load_font(size: int):
    for p in _FONT_PATHS:
        try:
            return ImageFont.truetype(p, size)
        except Exception:
            continue
    return ImageFont.load_default()


def add_watermark(im: Image.Image) -> Image.Image:
    """Полупрозрачный текстовый знак в правом нижнем углу."""
    if not WATERMARK:
        return im
    draw = ImageDraw.Draw(im, "RGBA")
    font = _load_font(max(24, im.width // 22))
    bbox = draw.textbbox((0, 0), WATERMARK, font=font)
    tw = bbox[2] - bbox[0]
    margin = im.width // 28
    x = im.width - tw - margin
    y = im.height - (bbox[3] - bbox[1]) - margin - bbox[1]
    draw.text((x + 2, y + 2), WATERMARK, font=font, fill=(0, 0, 0, 110))    # тень
    draw.text((x, y), WATERMARK, font=font, fill=(255, 255, 255, 190))      # текст
    return im


def fit_to_target(data: bytes) -> bytes:
    """К TARGET (масштаб + центр-кроп), водяной знак, без агрессивного шарпена."""
    im = Image.open(io.BytesIO(data)).convert("RGB")
    im = ImageOps.fit(im, TARGET, method=Image.LANCZOS)
    im = add_watermark(im)
    out = io.BytesIO()
    im.save(out, format="JPEG", quality=95, subsampling=0, optimize=True)
    return out.getvalue()


# --- Банк фонов города (Винница), разложенных по сезонам ---
BG_DIR = FOLDER / "backgrounds"

# Сезоны: ключ → подпапка в backgrounds/ и подпись кнопки в боте
SEASONS = {
    "summer": {"dir": "summer", "label": "🌿 Spring / Summer"},
    "winter": {"dir": "winter", "label": "🍂 Autumn / Winter"},
}


def _load_bg_dir(path) -> list:
    """Читает фоны из одной папки: чинит EXIF-поворот, ужимает и кодирует в data-URI."""
    uris = []
    if not path.exists():
        return uris
    for f in sorted(path.iterdir()):
        if f.suffix.lower() not in (".jpg", ".jpeg", ".png"):
            continue
        try:
            im = Image.open(f)
            im.draft("RGB", (1024, 1024))                     # уменьшенный JPEG-декод — экономит память
            im = ImageOps.exif_transpose(im).convert("RGB")   # чиним «лежачие» фото
            im.thumbnail((1024, 1024), Image.LANCZOS)          # ужимаем для компактного base64
            buf = io.BytesIO()
            im.save(buf, format="JPEG", quality=82)
            b64 = base64.b64encode(buf.getvalue()).decode()
            uris.append(f"data:image/jpeg;base64,{b64}")
        except Exception as e:
            log.warning("не удалось загрузить фон %s: %s", f.name, e)
    return uris


def load_backgrounds() -> dict:
    """Грузит фоны по сезонам из подпапок backgrounds/<сезон>/."""
    return {key: _load_bg_dir(BG_DIR / s["dir"]) for key, s in SEASONS.items()}


BACKGROUNDS = load_backgrounds()
log.info("Фонов города загружено: %s", {k: len(v) for k, v in BACKGROUNDS.items()})

# ---------------------------------------------------------------------------
# Бот
# ---------------------------------------------------------------------------

class Flow(StatesGroup):
    ready = State()
    await_color = State()   # ждём второе фото — образец цвета для перекраса


bot = Bot(token=TG_TOKEN, default=DefaultBotProperties(parse_mode="HTML"))
dp = Dispatcher(storage=MemoryStorage())


@dp.update.outer_middleware()
async def log_user(handler, event, data):
    """Логируем каждого, кто обращается к боту — чтобы находить пользователей."""
    u = data.get("event_from_user")
    if u:
        mark = "APPROVED" if (u.id == ADMIN_ID or u.id in APPROVED) else "guest"
        log.info("USER id=%s @%s '%s' [%s]", u.id, u.username, u.full_name, mark)
    return await handler(event, data)


def allowed(user_id: int) -> bool:
    return user_id == ADMIN_ID or user_id in APPROVED


@dp.callback_query.outer_middleware()
async def guard_callbacks(handler, event: CallbackQuery, data):
    """Кнопки від користувача без доступу: одразу закриваємо «годинник» на кнопці
    (раніше обробники робили return без cb.answer(), і кнопка крутилась ~15 с)."""
    if not allowed(event.from_user.id):
        try:
            await event.answer("No access.", show_alert=False)
        except Exception:
            pass
        return None
    return await handler(event, data)


def user_label(u) -> str:
    """Ім'я + @юзернейм як є (сирий текст). У HTML-повідомлення вставляти лише
    через escape(...) — див. users_text() та request_access()."""
    un = f"@{u.username}" if u.username else "—"
    return f"{u.full_name} ({un})"


async def request_access(message: Message):
    """Неодобренный пользователь: отправляем заявку админу, просим подождать."""
    u = message.from_user
    if u.id in REJECTED:
        await message.answer("Sorry, access was denied.")
        return
    if u.id in PENDING:
        await message.answer("⏳ Your request is being reviewed. Please wait.")
        return
    un = escape(f"@{u.username}") if u.username else "—"
    PENDING[u.id] = user_label(u)
    await message.answer(
        "👋 Welcome to <b>DP Photo</b>!\n\n"
        "Access to the bot is granted by the owner. "
        "Your request has been sent — please wait."
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="✅ Approve", callback_data=f"mod:ok:{u.id}"),
        InlineKeyboardButton(text="❌ Reject", callback_data=f"mod:no:{u.id}"),
    ]])
    try:
        await bot.send_message(
            ADMIN_ID,
            f"🆕 <b>New access request</b>\n"
            f"Name: {escape(u.full_name)}\n"
            f"Username: {un}\n"
            f"ID: <code>{u.id}</code>",
            reply_markup=kb,
        )
    except Exception:
        log.exception("не удалось отправить заявку админу")


def _to_jpeg_data_uri(raw: bytes) -> str:
    """Будь-яке зображення → JPEG data-URI (з урахуванням EXIF-повороту, до 2048 px)."""
    im = Image.open(io.BytesIO(raw))
    im = ImageOps.exif_transpose(im).convert("RGB")
    im.thumbnail((2048, 2048), Image.LANCZOS)
    buf = io.BytesIO()
    im.save(buf, format="JPEG", quality=92)
    return "data:image/jpeg;base64," + base64.b64encode(buf.getvalue()).decode()


async def tg_data_uri(file_id: str) -> str:
    """Завантажує фото з Telegram САМ і віддає моделі як data-URI.

    Раніше модель отримувала посилання api.telegram.org/file/bot<TOKEN>/..., тобто
    токен бота йшов у PiAPI та в логи. Тепер токен не залишає сервер.

    УВАГА: PiAPI приймає data-URI не на кожному маршруті, і це вже ламало
    бота. GPT Image 2 (/images/edits/async) бере його в полі "image". Якщо
    колись додаватимете іншу мережу — спершу перевірте, чи її ендпоінт
    узагалі приймає base64, а не лише публічні http-посилання."""
    buf = io.BytesIO()
    await bot.download(file_id, destination=buf)
    return await asyncio.to_thread(_to_jpeg_data_uri, buf.getvalue())


GENDER_LABEL = {
    "female": "👩 Women's",
    "male": "👨 Men's",
    "female_plus": "👩 Plus size (woman)",
    "male_plus": "👨 Plus size (man)",
}

# Полноэкранный эффект Telegram (Bot API 7.3) на сообщении «Готово» — 🎉
EFFECT_DONE = "5046509860389126442"

# Постоянная нижняя панель быстрых действий
BTN_NEW, BTN_PROFILE, BTN_HELP = "📷 New photo", "👤 Profile", "❓ Help"
MAIN_KB = ReplyKeyboardMarkup(
    keyboard=[[KeyboardButton(text=BTN_NEW),
               KeyboardButton(text=BTN_PROFILE),
               KeyboardButton(text=BTN_HELP)]],
    resize_keyboard=True, is_persistent=True,
)


def presets_keyboard() -> InlineKeyboardMarkup:
    # Кадры — в 2 колонки (компактнее); особые режимы — на всю ширину
    items = list(PRESETS.items())
    rows = [[InlineKeyboardButton(text=p["title"], callback_data=f"preset:{key}")
             for key, p in items[i:i + 2]] for i in range(0, len(items), 2)]
    rows.append([InlineKeyboardButton(text="🎁 Full set · 5 shots",
                                      callback_data="fullset")])
    rows.append([InlineKeyboardButton(text="🎨 Recolour", callback_data="recolor")])
    if any(BACKGROUNDS.values()):
        rows.append([InlineKeyboardButton(text="📍 Vinnytsia · real backdrop",
                                          callback_data="vinnytsia")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def season_keyboard() -> InlineKeyboardMarkup:
    """Выбор сезона фона — показываем только сезоны, для которых есть фото."""
    rows = [[InlineKeyboardButton(text=s["label"], callback_data=f"season:{key}")]
            for key, s in SEASONS.items() if BACKGROUNDS.get(key)]
    return InlineKeyboardMarkup(inline_keyboard=rows)


def models_keyboard(count: int = 1, show_count: bool = True) -> InlineKeyboardMarkup:
    rows = []
    for key, m in MODELS.items():
        rows.append([InlineKeyboardButton(
            text=f"{m['label']}   ·   ${m['price']:.3f}",
            callback_data=f"model:{key}")])
    if len(MODELS) > 1:
        total = sum(m["price"] for m in MODELS.values())
        rows.append([InlineKeyboardButton(text=f"⚡ Both   ·   ${total:.3f}",
                                          callback_data="model:all")])
    if show_count:
        # Ряд выбора количества вариантов — текущий отмечен галочкой
        rows.append([InlineKeyboardButton(
            text=("✅ " if count == n else "") + f"×{n}",
            callback_data=f"cnt:{n}") for n in (1, 2, 4)])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def crop_keyboard() -> InlineKeyboardMarkup:
    """Ручний вибір кадрування (використовується лише в сцені «Анфас»)."""
    return _labeled_keyboard(CROP_LABELS, "mancrop",
                             extra_button=("🎲 All (3 crops)", "mancrop:all"), per_row=1)


def frontposes_keyboard() -> InlineKeyboardMarkup:
    """Скільки поз згенерувати для «Анфас» (1-6)."""
    return _number_keyboard(1, 6, "frontposes")


def backposes_keyboard() -> InlineKeyboardMarkup:
    """Скільки поз згенерувати для «Спина» (1-6)."""
    return _number_keyboard(1, 6, "backposes")


def bg_keyboard() -> InlineKeyboardMarkup:
    """Ручний вибір фону — спільний для сцен, де є {bg} (Анфас, Спина)."""
    btns = [InlineKeyboardButton(text=lbl, callback_data=f"manbg:{i}")
            for i, lbl in enumerate(BG_LABELS)]
    rows = [btns[i:i + 2] for i in range(0, len(btns), 2)]
    return InlineKeyboardMarkup(inline_keyboard=rows)


def light_keyboard() -> InlineKeyboardMarkup:
    """Ручний вибір світла — спільний для сцен, де є {light} (Анфас, Спина).
    Останній крок ручного налаштування — далі йде вибір нейромережі."""
    btns = [InlineKeyboardButton(text=lbl, callback_data=f"manlight:{i}")
            for i, lbl in enumerate(LIGHT_LABELS)]
    rows = [btns[i:i + 2] for i in range(0, len(btns), 2)]
    return InlineKeyboardMarkup(inline_keyboard=rows)


def arrange_keyboard() -> InlineKeyboardMarkup:
    """Ручний вибір розкладки одягу (Flat lay)."""
    return _labeled_keyboard(ARRANGE_LABELS, "flatarrange",
                             extra_button=("🎲 All (4 layouts)", "flatarrange:all"), per_row=1)


def angle_keyboard() -> InlineKeyboardMarkup:
    """Ручний вибір ракурсу зйомки (Flat lay)."""
    return _labeled_keyboard(ANGLE_LABELS, "flatangle",
                             extra_button=("🎲 Both", "flatangle:all"), per_row=1)


def flatbg_keyboard() -> InlineKeyboardMarkup:
    """Ручний вибір фону (Flat lay) — той самий фон, що й у «Спина студія»."""
    return _labeled_keyboard(BG_LABELS, "flatbg")


def ghost_angle_keyboard() -> InlineKeyboardMarkup:
    """Ручний вибір ракурсу (Невидимий манекен)."""
    return _labeled_keyboard(GHOST_ANGLE_LABELS, "ghostangle",
                             extra_button=("🎲 Both", "ghostangle:all"), per_row=1)


def _labeled_keyboard(labels: list, prefix: str, extra_button: tuple | None = None,
                      per_row: int = 2) -> InlineKeyboardMarkup:
    """Допоміжна фабрика клавіатур «підпис → callback_data:{idx}», по `per_row` в ряд.
    extra_button — опційна остання кнопка на весь ряд, напр. (\"🎲 Всі...\", \"prefix:all\")."""
    btns = [InlineKeyboardButton(text=lbl, callback_data=f"{prefix}:{i}")
            for i, lbl in enumerate(labels)]
    rows = [btns[i:i + per_row] for i in range(0, len(btns), per_row)]
    if extra_button:
        text, cbdata = extra_button
        rows.append([InlineKeyboardButton(text=text, callback_data=cbdata)])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def _number_keyboard(lo: int, hi: int, prefix: str, per_row: int = 3) -> InlineKeyboardMarkup:
    """Клавіатура з кнопками-числами lo..hi (включно), callback_data:{n}."""
    btns = [InlineKeyboardButton(text=str(n), callback_data=f"{prefix}:{n}")
            for n in range(lo, hi + 1)]
    rows = [btns[i:i + per_row] for i in range(0, len(btns), per_row)]
    return InlineKeyboardMarkup(inline_keyboard=rows)


# --- «У місті» (окремий кадр): локація → світло → поза/всі пози ---
def citylocation_keyboard() -> InlineKeyboardMarkup:
    return _labeled_keyboard(LOCATION_LABELS, "citylocation")


def citylight_keyboard() -> InlineKeyboardMarkup:
    return _labeled_keyboard(DAYLIGHT_LABELS, "citylight")


def citypose_keyboard() -> InlineKeyboardMarkup:
    return _labeled_keyboard(CITY_POSE_LABELS, "citypose",
                             extra_button=("🎲 All poses (4 cards)", "citypose:all"))


# --- «Повний комплект»: кількість поз (Анфас) → спільний фон → локація/світло/кількість поз (Місто) ---
def fullposes_keyboard() -> InlineKeyboardMarkup:
    return _number_keyboard(1, 6, "fullposes")


def fullbg_keyboard() -> InlineKeyboardMarkup:
    return _labeled_keyboard(BG_LABELS, "fullbg")


def fulllight_keyboard() -> InlineKeyboardMarkup:
    """Спільне світло для Анфас/Спина/Flat lay/Манекен у «Повному комплекті»."""
    return _labeled_keyboard(LIGHT_LABELS, "fulllight")


def fulllocation_keyboard() -> InlineKeyboardMarkup:
    return _labeled_keyboard(LOCATION_LABELS, "fulllocation")


def fullcitylight_keyboard() -> InlineKeyboardMarkup:
    return _labeled_keyboard(DAYLIGHT_LABELS, "fullcitylight")


def fullcitycount_keyboard() -> InlineKeyboardMarkup:
    return _number_keyboard(1, 4, "fullcitycount", per_row=4)


def all_models_keyboard() -> InlineKeyboardMarkup:
    """Усі моделі всіх 4 категорій одразу на одному екрані, згруповані за
    кольором волосся. Заголовок кожної групи — некликабельна кнопка (noop).

    Категорії з одним варіантом у родині (напр. Жінка в «Блонд») стоять
    парами по 2 в ряд, як звичайно. Категорії з кількома варіантами в одній
    родині (напр. Чоловік у «Каштанове/шатен» — з бородою і без) отримують
    ВЛАСНИЙ окремий ряд, щоб довгі підписи не ламали вирівнювання поруч із
    короткими кнопками інших категорій."""
    rows = []
    for fam in FAMILY_ORDER:
        rows.append([InlineKeyboardButton(text=f"── {FAMILY_TITLES[fam]} ──",
                                          callback_data="noop")])
        singles = []
        multi_rows = []
        for cat in CATEGORY_ORDER:
            idxs = [i for i, f in enumerate(MODEL_FAMILY[cat]) if f == fam]
            if not idxs:
                continue
            if len(idxs) == 1:
                idx = idxs[0]
                singles.append(InlineKeyboardButton(
                    text=CATEGORY_SHORT[cat], callback_data=f"modelpick:{cat}:{idx}"))
            else:
                row_btns = []
                for pos, idx in enumerate(idxs):
                    full = MODEL_LABELS[cat][idx].lower()
                    if "no beard" in full:
                        tag = "no beard"
                    elif "beard" in full:
                        tag = "with beard"
                    else:
                        tag = f"#{pos + 1}"
                    row_btns.append(InlineKeyboardButton(
                        text=f"{CATEGORY_SHORT[cat]} · {tag}", callback_data=f"modelpick:{cat}:{idx}"))
                multi_rows.append(row_btns)
        rows.extend(singles[i:i + 2] for i in range(0, len(singles), 2))
        rows.extend(multi_rows)
    rows.append([InlineKeyboardButton(text="🎲 Fully random",
                                      callback_data="modelpick:random:random")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def recolor_target_keyboard() -> InlineKeyboardMarkup:
    """Что именно перекрашивать: типы вещей + «весь товар», по 2 кнопки в ряд."""
    btns = [InlineKeyboardButton(text=lbl, callback_data=f"rtgt:{k}")
            for k, (lbl, _) in RECOLOR_TARGETS.items()]
    btns.append(InlineKeyboardButton(text="🎨 The whole product", callback_data="rtgt:all"))
    rows = [btns[i:i + 2] for i in range(0, len(btns), 2)]
    return InlineKeyboardMarkup(inline_keyboard=rows)


async def react_fire(message: Message):
    """Мгновенная реакция 🔥 на присланное фото товара — оживляет диалог."""
    try:
        await message.react([ReactionTypeEmoji(emoji="🔥")])
    except Exception:
        pass


async def save_photo(message: Message, state: FSMContext, file_ids: list):
    """Новый товар (одно фото или пачка). Сбрасываем режим и параметры."""
    await state.update_data(file_ids=file_ids, mode="scene", color_file_id=None,
                            recolor_target=None, count=1, full_set=False,
                            preset=None, season=None, gender="female", model_idx=None,
                            manual_style=None, all_crop=False, all_arrange=False, all_angle=False, all_poses=False)
    await state.set_state(Flow.ready)
    n = len(file_ids)
    note = "📷 Photo received\n\n" if n == 1 else f"📷 Photos received: <b>{n}</b>\n\n"
    await message.answer(note + "<b>Choose a model</b>", reply_markup=all_models_keyboard())


async def handle_incoming_photo(message: Message, state: FSMContext, file_id: str):
    """Одиночное фото: либо образец цвета для перекраса, либо новый товар."""
    cur = await state.get_state()
    data = await state.get_data()
    if cur == Flow.await_color.state and data.get("mode") == "recolor":
        await state.update_data(color_file_id=file_id)
        await state.set_state(Flow.ready)
        await message.answer("🎨 Colour sample received\n\n<b>What should be recoloured?</b>",
                             reply_markup=recolor_target_keyboard())
    else:
        await save_photo(message, state, [file_id])


# --- Агрегатор альбомов: Telegram шлёт каждое фото пачки отдельным апдейтом ---
_album_buf: dict = {}   # media_group_id -> {"ids":[...], "msg":Message, "state":FSMContext, "task":Task}


async def _album_timeout(mgid: str):
    try:
        await asyncio.sleep(1.2)   # ждём тишины — значит пачка догрузилась
    except asyncio.CancelledError:
        return
    entry = _album_buf.pop(mgid, None)
    if entry:
        await save_photo(entry["msg"], entry["state"], entry["ids"])


async def collect_album(message: Message, state: FSMContext, file_id: str):
    """Копим file_id одной пачки и обрабатываем, когда она догрузилась."""
    mgid = message.media_group_id
    entry = _album_buf.get(mgid)
    if entry is None:
        entry = _album_buf[mgid] = {"ids": [], "msg": message, "state": state, "task": None}
    entry["ids"].append(file_id)
    entry["msg"] = message
    if entry["task"]:
        entry["task"].cancel()
    entry["task"] = asyncio.create_task(_album_timeout(mgid))


@dp.message(CommandStart())
async def on_start(message: Message, state: FSMContext):
    if not allowed(message.from_user.id):
        await request_access(message)
        return
    await state.clear()
    await message.answer(
        "✨ <b>DP PHOTO</b> ✨\n"
        "<i>Studio clothing shots from a single photo</i>\n"
        "──────────────\n"
        "Send a photo of your product and in a few seconds you'll get a ready "
        "marketplace card: on a model, flat lay or out on a city street.\n\n"
        "📷 <b>Send a product photo to get started.</b>",
        reply_markup=MAIN_KB,
    )


HELP_TEXT = (
    "✨ <b>DP PHOTO</b> — how to use it\n"
    "──────────────\n"
    "1️⃣ Send a product photo\n"
    "2️⃣ Choose a model (hair colour, women's / men's / plus size)\n"
    "3️⃣ Choose a shot and set it up (poses, background, lighting…)\n"
    "4️⃣ Choose an AI model — generation starts right away\n"
    "→ you get the finished card as a file\n"
    "⛔ A generation can be cancelled with the button under the progress bar\n\n"
    "<b>Shots</b>\n"
    "🧍 Front · 🔙 Back · 📐 Flat lay · 👤 Mannequin · 🌆 City\n\n"
    "<b>Special modes</b>\n"
    "🎁 <b>Full set</b> — all 5 shots in one tap (album)\n"
    "🎨 <b>Recolour</b> — product photo + colour sample\n"
    "📍 <b>Vinnytsia</b> — your product against a real city backdrop\n\n"
    "<b>Handy extras</b>\n"
    "🔥 Batch upload — send several products at once\n"
    "×1 / ×2 / ×4 or “How many poses?” — how many variants to generate\n"
    "🔄 One more — repeat the last generation\n\n"
    "<b>AI model</b>\n"
    "🤖 GPT Image 2 — strong on scenes and lighting\n\n"
    "/profile — my profile · /id — Telegram ID"
)


@dp.message(Command("help"))
async def on_help(message: Message):
    if not allowed(message.from_user.id):
        return
    await message.answer(HELP_TEXT)


@dp.message(Command("id"))
async def on_id(message: Message):
    if not allowed(message.from_user.id):
        return
    await message.answer(f"Your ID: <code>{message.from_user.id}</code>")


# --- Кнопки постоянной нижней панели ---
@dp.message(F.text == BTN_HELP)
async def kb_help(message: Message):
    if not allowed(message.from_user.id):
        await request_access(message)
        return
    await message.answer(HELP_TEXT)


@dp.message(F.text == BTN_NEW)
async def kb_new(message: Message, state: FSMContext):
    if not allowed(message.from_user.id):
        await request_access(message)
        return
    await state.clear()
    await message.answer("📷 <b>Send a product photo</b> to get started.")


# --- Профиль пользователя: статистика генераций и расхода ---
def profile_text(uid: int) -> str:
    s = STATS.get(uid, {})
    gpt_n, gpt_usd = s.get("gpt", 0), s.get("gpt_usd", 0.0)
    # Мережа тепер одна, тож розбивки по моделях немає. Але у витратах
    # лишаються й давні генерації знятої моделі (поля nano_* у stats.json):
    # гроші були витрачені насправді, і ховати їх із підсумку не можна.
    spend = gpt_usd + s.get("nano_usd", 0.0)
    total = USAGE.get(uid, 0)   # всего карток за всё время (враховує й давні генерації)
    return (
        "👤 <b>My profile</b>\n"
        "──────────────\n"
        f"📸 Cards generated: <b>{total}</b>\n"
        f"💵 Spent on the API: <b>${spend:.4f}</b>\n\n"
        "<i>Spending is counted from the bot update on 23.07.</i>"
        + ("\n<i>⚠️ The amount is an estimate — set GPT_PRICE in .env "
           "for exact figures.</i>" if GPT_PRICE_IS_STUB and gpt_n else "")
    )


@dp.message(Command("profile"))
@dp.message(F.text == BTN_PROFILE)
async def on_profile(message: Message):
    if not allowed(message.from_user.id):
        return
    await message.answer(profile_text(message.from_user.id))


@dp.message(F.photo)
async def on_photo(message: Message, state: FSMContext):
    if not allowed(message.from_user.id):
        await request_access(message)
        return
    await react_fire(message)
    if message.media_group_id:                 # часть пачки — копим и ждём остальные
        await collect_album(message, state, message.photo[-1].file_id)
    else:
        await handle_incoming_photo(message, state, message.photo[-1].file_id)


@dp.message(F.document)
async def on_document(message: Message, state: FSMContext):
    """Фото, присланное файлом (без сжатия) — тоже принимаем."""
    if not allowed(message.from_user.id):
        await request_access(message)
        return
    if not (message.document.mime_type or "").startswith("image/"):
        await message.answer("An image is required. Please send a product photo.")
        return
    await react_fire(message)
    if message.media_group_id:
        await collect_album(message, state, message.document.file_id)
    else:
        await handle_incoming_photo(message, state, message.document.file_id)


@dp.callback_query(F.data.startswith("mod:"))
async def on_moderation(cb: CallbackQuery):
    """Админ одобряет/отклоняет заявку на доступ."""
    if cb.from_user.id != ADMIN_ID:
        await cb.answer()
        return
    _, action, uid_s = cb.data.split(":")
    uid = int(uid_s)
    label = PENDING.pop(uid, str(uid))
    if action == "ok":
        APPROVED[uid] = {"name": label, "since": date.today().isoformat()}
        save_approved()
        if uid in REJECTED:
            REJECTED.discard(uid)
            save_rejected()
        await cb.message.edit_text(cb.message.html_text + "\n\n✅ <b>Approved</b>")
        try:
            await bot.send_message(
                uid, "✅ <b>Access granted!</b>\n\nSend a product photo to get started.")
        except Exception:
            pass
    else:
        REJECTED.add(uid)          # більше не зможе слати заявки (до розблокування в /users)
        save_rejected()
        await cb.message.edit_text(cb.message.html_text + "\n\n❌ <b>Rejected</b>")
        try:
            await bot.send_message(uid, "Sorry, access was denied.")
        except Exception:
            pass
    await cb.answer("Done")


def users_text() -> str:
    total_gens = sum(USAGE.values())
    head = (
        "👥 <b>DP Photo users</b>\n"
        "──────────────\n"
        f"👤 With access: <b>{len(APPROVED) + 1}</b> (owner + {len(APPROVED)} approved)\n"
        f"⏳ Pending: <b>{len(PENDING)}</b>\n"
        f"📸 Generations today: <b>{_used_today}</b> / {DAILY_LIMIT}\n"
        f"📊 Generations in total: <b>{total_gens}</b>\n"
    )
    # Владелец всегда в списке первым
    rows = [f"• <b>Owner</b> · <i>you</i>\n  <code>{ADMIN_ID}</code> · {USAGE.get(ADMIN_ID, 0)} gen."]
    for uid, info in APPROVED.items():
        if uid == ADMIN_ID:
            continue
        name = escape(str(info.get("name", uid)))
        since = info.get("since", "—")
        gens = USAGE.get(uid, 0)
        rows.append(f"• <b>{name}</b>\n  <code>{uid}</code> · since {since} · {gens} gen.")
    body = "\n\n<b>List:</b>\n" + "\n".join(rows)
    if len(rows) > 1:
        body += "\n\nTap ❌ to revoke access:"
    pend = ""
    if PENDING:
        pend = "\n\n⏳ <b>Awaiting approval:</b>\n" + "\n".join(
            f"• {escape(str(lbl))} — <code>{uid}</code>" for uid, lbl in PENDING.items())
    rej = ""
    if REJECTED:
        rej = (f"\n\n🚫 <b>Rejected:</b> {len(REJECTED)}\n"
               "Tap ♻️ to let someone send a request again.")
    return head + body + pend + rej


def users_keyboard() -> InlineKeyboardMarkup | None:
    # Отзыв доступа у всех, кроме самого админа
    rows = [[InlineKeyboardButton(text=f"❌ {info.get('name', str(uid))[:26]}",
                                  callback_data=f"revoke:{uid}")]
            for uid, info in APPROVED.items() if uid != ADMIN_ID]
    rows += [[InlineKeyboardButton(text=f"♻️ Unblock {uid}", callback_data=f"unreject:{uid}")]
             for uid in sorted(REJECTED)]
    return InlineKeyboardMarkup(inline_keyboard=rows) if rows else None


@dp.message(Command("users"))
async def on_users(message: Message):
    if message.from_user.id != ADMIN_ID:
        return
    _rollover()
    await message.answer(users_text(), reply_markup=users_keyboard())


@dp.callback_query(F.data.startswith("revoke:"))
async def on_revoke(cb: CallbackQuery):
    if cb.from_user.id != ADMIN_ID:
        await cb.answer()
        return
    uid = int(cb.data.split(":")[1])
    APPROVED.pop(uid, None)
    save_approved()
    try:
        await bot.send_message(uid, "🚫 Your access to the bot has been revoked.")
    except Exception:
        pass
    await cb.message.edit_text(users_text(), reply_markup=users_keyboard())
    await cb.answer("Access revoked")


@dp.callback_query(F.data.startswith("unreject:"))
async def on_unreject(cb: CallbackQuery):
    """Адмін знімає відмову — людина знову може подати заявку."""
    if cb.from_user.id != ADMIN_ID:
        await cb.answer()
        return
    uid = int(cb.data.split(":")[1])
    REJECTED.discard(uid)
    save_rejected()
    await cb.message.edit_text(users_text(), reply_markup=users_keyboard())
    await cb.answer("They can send a request again")


@dp.callback_query(F.data == "noop")
async def on_noop(cb: CallbackQuery):
    """Заголовок-роздільник групи в списку моделей — не клікабельний."""
    await cb.answer()


@dp.callback_query(F.data.startswith("modelpick:"))
async def on_modelpick(cb: CallbackQuery, state: FSMContext):
    """Вибір конкретної моделі (за кольором волосся) або випадкової."""
    if not allowed(cb.from_user.id):
        return
    _, g, idx_s = cb.data.split(":", 2)
    if g == "random":
        g = random.choice(CATEGORY_ORDER)   # «Повністю випадкова» — випадкова категорія
        idx_s = "random"
    if g not in GENDER_LABEL:
        await cb.answer()
        return
    labels = MODEL_LABELS.get(g, [])
    if idx_s == "random":
        model_idx = None
        label = "🎲 Random"
    else:
        try:
            idx = int(idx_s)
        except ValueError:
            await cb.answer()
            return
        if idx < 0 or idx >= len(labels):
            await cb.answer()
            return
        model_idx = idx
        label = labels[idx]
    await state.update_data(gender=g, model_idx=model_idx)
    await cb.answer()
    await cb.message.answer(
        f"Category: {GENDER_LABEL[g]} · Model: {label}\n\n<b>Choose a shot</b>",
        reply_markup=presets_keyboard())


@dp.callback_query(F.data == "recolor")
async def on_recolor(cb: CallbackQuery, state: FSMContext):
    if not allowed(cb.from_user.id):
        return
    data = await state.get_data()
    if not data.get("file_ids"):
        await cb.answer("Send a product photo first.", show_alert=True)
        return
    await state.update_data(mode="recolor", full_set=False, recolor_target=None, manual_style=None, all_crop=False, all_arrange=False, all_angle=False, all_poses=False)
    await state.set_state(Flow.await_color)
    await cb.answer()
    await cb.message.answer(
        "🎨 <b>Recolour</b>\n"
        "──────────────\n"
        "Send a colour sample — a photo of fabric, a product in the colour you want "
        "or simply a solid block of that colour."
    )


@dp.callback_query(F.data.startswith("rtgt:"))
async def on_recolor_target(cb: CallbackQuery, state: FSMContext):
    """Выбор, что именно перекрашивать. Сохраняем англ. описание цели в state."""
    if not allowed(cb.from_user.id):
        return
    key = cb.data.split(":", 1)[1]
    if key == "all":
        await state.update_data(recolor_target=None)
        label = "🎨 the whole product"
    else:
        t = RECOLOR_TARGETS.get(key)
        if not t:
            await cb.answer()
            return
        await state.update_data(recolor_target=t[1])   # англ. описание для промпта
        label = t[0]
    data = await state.get_data()
    await cb.answer()
    await cb.message.answer(
        f"🎯 Recolouring: <b>{label}</b>\n\n<b>Start the generation</b>",
        reply_markup=models_keyboard(data.get("count", 1)),
    )


@dp.callback_query(F.data == "vinnytsia")
async def on_citybg(cb: CallbackQuery, state: FSMContext):
    if not allowed(cb.from_user.id):
        return
    data = await state.get_data()
    if not data.get("file_ids"):
        await cb.answer("Send a product photo first.", show_alert=True)
        return
    await state.update_data(mode="citybg", full_set=False, manual_style=None, all_crop=False, all_arrange=False, all_angle=False, all_poses=False)
    await cb.answer()
    await cb.message.answer(
        "📍 <b>Vinnytsia</b>\n<i>Your product against a real city backdrop</i>\n\n<b>Choose a season</b>",
        reply_markup=season_keyboard(),
    )


@dp.callback_query(F.data.startswith("season:"))
async def on_season(cb: CallbackQuery, state: FSMContext):
    if not allowed(cb.from_user.id):
        return
    key = cb.data.split(":", 1)[1]
    if key not in SEASONS or not BACKGROUNDS.get(key):
        await cb.answer()
        return
    await state.update_data(mode="citybg", season=key, full_set=False)
    data = await state.get_data()
    await cb.answer()
    await cb.message.answer(
        f"📍 <b>Vinnytsia</b> · {SEASONS[key]['label']}\n\n<b>Start the generation</b>",
        reply_markup=models_keyboard(data.get("count", 1)),
    )


@dp.callback_query(F.data.startswith("preset:"))
async def on_preset(cb: CallbackQuery, state: FSMContext):
    if not allowed(cb.from_user.id):
        return
    key = cb.data.split(":", 1)[1]
    if key not in PRESETS:
        await cb.answer()
        return
    if key == "front":
        # «Анфас»: спершу кількість поз, потім кадрування → фон → світло → мережа.
        await state.update_data(preset=key, mode="scene", full_set=False,
                                manual_style={}, all_crop=False, count=1)
        await cb.answer()
        await cb.message.answer(
            f"{PRESETS[key]['title']}\n\n<b>How many poses?</b> (1-6)", reply_markup=frontposes_keyboard())
        return
    if key == "back":
        # «Спина»: спершу кількість поз, потім фон → світло → поза волосся → мережа.
        await state.update_data(preset=key, mode="scene", full_set=False, manual_style={}, count=1)
        await cb.answer()
        await cb.message.answer(
            f"{PRESETS[key]['title']}\n\n<b>How many poses?</b> (1-6)", reply_markup=backposes_keyboard())
        return
    if key == "flatlay":
        # «Flat lay»: ручний вибір розкладки → ракурсу → поверхні (або всіх поверхонь), потім мережа.
        await state.update_data(preset=key, mode="scene", full_set=False, count=1,
                                manual_style={}, all_crop=False, all_arrange=False, all_angle=False)
        await cb.answer()
        await cb.message.answer(
            f"{PRESETS[key]['title']}\n\n<b>Choose a layout</b>", reply_markup=arrange_keyboard())
        return
    if key == "ghost":
        # «Невидимий манекен»: ручний вибір ракурсу → фону → світла, потім мережа.
        await state.update_data(preset=key, mode="scene", full_set=False, count=1, manual_style={})
        await cb.answer()
        await cb.message.answer(
            f"{PRESETS[key]['title']}\n\n<b>Choose an angle</b>", reply_markup=ghost_angle_keyboard())
        return
    if key == "city":
        # «У місті»: ручний вибір локації → світла → пози (або всіх поз), потім мережа.
        await state.update_data(preset=key, mode="scene", full_set=False, count=1,
                                manual_style={}, all_poses=False)
        await cb.answer()
        await cb.message.answer(
            f"{PRESETS[key]['title']}\n\n<b>Choose a location</b>", reply_markup=citylocation_keyboard())
        return
    await state.update_data(preset=key, mode="scene", full_set=False, count=1, manual_style=None, all_crop=False, all_arrange=False, all_angle=False, all_poses=False)  # выбор сцены выходит из перекраса
    data = await state.get_data()
    await cb.answer()
    await cb.message.answer(
        f"{PRESETS[key]['title']}\n\n<b>Start the generation</b>",
        reply_markup=models_keyboard(data.get("count", 1)),
    )


@dp.callback_query(F.data.startswith("frontposes:"))
async def on_frontposes(cb: CallbackQuery, state: FSMContext):
    if not allowed(cb.from_user.id):
        return
    try:
        n = int(cb.data.split(":", 1)[1])
    except ValueError:
        await cb.answer()
        return
    if n < 1 or n > 6:
        await cb.answer()
        return
    await state.update_data(count=n)
    await cb.answer()
    await cb.message.answer(
        f"Poses: <b>{n}</b>\n\n<b>Choose the framing</b>", reply_markup=crop_keyboard())


@dp.callback_query(F.data.startswith("backposes:"))
async def on_backposes(cb: CallbackQuery, state: FSMContext):
    if not allowed(cb.from_user.id):
        return
    try:
        n = int(cb.data.split(":", 1)[1])
    except ValueError:
        await cb.answer()
        return
    if n < 1 or n > 6:
        await cb.answer()
        return
    await state.update_data(count=n)
    await cb.answer()
    await cb.message.answer(
        f"Poses: <b>{n}</b>\n\n<b>Choose a background</b>", reply_markup=bg_keyboard())


@dp.callback_query(F.data.startswith("mancrop:"))
async def on_mancrop(cb: CallbackQuery, state: FSMContext):
    if not allowed(cb.from_user.id):
        return
    val = cb.data.split(":", 1)[1]
    data = await state.get_data()
    manual = dict(data.get("manual_style") or {})
    if val == "all":
        manual.pop("crop", None)
        await state.update_data(manual_style=manual, all_crop=True)
        label = "🎲 All (3 crops)"
    else:
        try:
            idx = int(val)
        except ValueError:
            await cb.answer()
            return
        if idx < 0 or idx >= len(_CROP):
            await cb.answer()
            return
        manual["crop"] = _CROP[idx]
        await state.update_data(manual_style=manual, all_crop=False)
        label = CROP_LABELS[idx]
    await cb.answer()
    await cb.message.answer(
        f"Framing: {label}\n\n<b>Choose a background</b>", reply_markup=bg_keyboard())


@dp.callback_query(F.data.startswith("manbg:"))
async def on_manbg(cb: CallbackQuery, state: FSMContext):
    if not allowed(cb.from_user.id):
        return
    try:
        idx = int(cb.data.split(":", 1)[1])
    except ValueError:
        await cb.answer()
        return
    if idx < 0 or idx >= len(_BG):
        await cb.answer()
        return
    data = await state.get_data()
    manual = dict(data.get("manual_style") or {})
    manual["bg"] = _BG[idx]
    await state.update_data(manual_style=manual)
    await cb.answer()
    await cb.message.answer(
        f"Background: {BG_LABELS[idx]}\n\n<b>Choose the lighting</b>", reply_markup=light_keyboard())


@dp.callback_query(F.data.startswith("manlight:"))
async def on_manlight(cb: CallbackQuery, state: FSMContext):
    """Останній крок ручного налаштування (Анфас/Спина) — далі йде вибір нейромережі
    (для «Спина» + жіноча категорія — ще один крок: поза волосся)."""
    if not allowed(cb.from_user.id):
        return
    try:
        idx = int(cb.data.split(":", 1)[1])
    except ValueError:
        await cb.answer()
        return
    if idx < 0 or idx >= len(_LIGHT_STUDIO):
        await cb.answer()
        return
    data = await state.get_data()
    manual = dict(data.get("manual_style") or {})
    manual["light"] = _LIGHT_STUDIO[idx]
    await state.update_data(manual_style=manual)
    await cb.answer()
    if data.get("preset") == "back" and data.get("gender") in ("female", "female_plus"):
        await cb.message.answer(
            f"Lighting: {LIGHT_LABELS[idx]}\n\n<b>Choose a hair style</b>",
            reply_markup=backstyle_pos_keyboard())
        return
    # Для «Анфас»/«Спина» кількість поз уже задана окремим кроком — ховаємо лічильник ×1/×2/×4.
    show_count = data.get("preset") not in ("front", "back")
    await cb.message.answer(
        f"Lighting: {LIGHT_LABELS[idx]}\n\n<b>Start the generation</b>",
        reply_markup=models_keyboard(data.get("count", 1), show_count=show_count),
    )


@dp.callback_query(F.data.startswith("ghostangle:"))
async def on_ghostangle(cb: CallbackQuery, state: FSMContext):
    if not allowed(cb.from_user.id):
        return
    val = cb.data.split(":", 1)[1]
    data = await state.get_data()
    manual = dict(data.get("manual_style") or {})
    if val == "all":
        manual.pop("angle", None)
        await state.update_data(manual_style=manual, all_angle=True)
        label = "🎲 Both"
    else:
        try:
            idx = int(val)
        except ValueError:
            await cb.answer()
            return
        if idx < 0 or idx >= len(_GHOST_ANGLE):
            await cb.answer()
            return
        manual["angle"] = _GHOST_ANGLE[idx]
        await state.update_data(manual_style=manual, all_angle=False)
        label = GHOST_ANGLE_LABELS[idx]
    await cb.answer()
    await cb.message.answer(
        f"Angle: {label}\n\n<b>Choose a background</b>", reply_markup=bg_keyboard())


@dp.callback_query(F.data.startswith("backpos:"))
async def on_backpos(cb: CallbackQuery, state: FSMContext):
    """Останній крок «Спина» (жіночі категорії) — поза волосся; колір бере від обраної моделі."""
    if not allowed(cb.from_user.id):
        return
    val = cb.data.split(":", 1)[1]
    data = await state.get_data()
    manual = dict(data.get("manual_style") or {})
    if val == "random":
        manual.pop("back_pos_idx", None)
        label = "🎲 Random"
    else:
        try:
            idx = int(val)
        except ValueError:
            await cb.answer()
            return
        if idx < 0 or idx >= len(BACKSTYLE_POSITION_LABELS):
            await cb.answer()
            return
        manual["back_pos_idx"] = idx
        label = BACKSTYLE_POSITION_LABELS[idx]
    await state.update_data(manual_style=manual)
    await cb.answer()
    await cb.message.answer(
        f"Hair style: {label}\n\n<b>Start the generation</b>",
        reply_markup=models_keyboard(data.get("count", 1), show_count=False),
    )


@dp.callback_query(F.data.startswith("citylocation:"))
async def on_citylocation(cb: CallbackQuery, state: FSMContext):
    if not allowed(cb.from_user.id):
        return
    try:
        idx = int(cb.data.split(":", 1)[1])
    except ValueError:
        await cb.answer()
        return
    if idx < 0 or idx >= len(_LOCATION):
        await cb.answer()
        return
    data = await state.get_data()
    manual = dict(data.get("manual_style") or {})
    manual["location"] = _LOCATION[idx]
    await state.update_data(manual_style=manual)
    await cb.answer()
    await cb.message.answer(
        f"Location: {LOCATION_LABELS[idx]}\n\n<b>Choose the lighting</b>", reply_markup=citylight_keyboard())


@dp.callback_query(F.data.startswith("citylight:"))
async def on_citylight(cb: CallbackQuery, state: FSMContext):
    if not allowed(cb.from_user.id):
        return
    try:
        idx = int(cb.data.split(":", 1)[1])
    except ValueError:
        await cb.answer()
        return
    if idx < 0 or idx >= len(_DAYLIGHT):
        await cb.answer()
        return
    data = await state.get_data()
    manual = dict(data.get("manual_style") or {})
    manual["light"] = _DAYLIGHT[idx]
    await state.update_data(manual_style=manual)
    await cb.answer()
    await cb.message.answer(
        f"Lighting: {DAYLIGHT_LABELS[idx]}\n\n<b>Choose a pose</b>", reply_markup=citypose_keyboard())


@dp.callback_query(F.data.startswith("citypose:"))
async def on_citypose(cb: CallbackQuery, state: FSMContext):
    """Останній крок «У місті» — конкретна поза або «всі пози» (4 картки)."""
    if not allowed(cb.from_user.id):
        return
    val = cb.data.split(":", 1)[1]
    data = await state.get_data()
    gender = data.get("gender", "female")
    manual = dict(data.get("manual_style") or {})
    pose_pool = POSE_POOLS[gender]["city"]
    if val == "all":
        manual.pop("pose", None)
        await state.update_data(manual_style=manual, all_poses=True)
        label = "🎲 All poses (4 cards)"
    else:
        try:
            idx = int(val)
        except ValueError:
            await cb.answer()
            return
        if idx < 0 or idx >= len(CITY_POSE_LABELS):
            await cb.answer()
            return
        manual["pose"] = pose_pool[idx % len(pose_pool)]
        await state.update_data(manual_style=manual, all_poses=False)
        label = CITY_POSE_LABELS[idx]
    await cb.answer()
    await cb.message.answer(
        f"Pose: {label}\n\n<b>Start the generation</b>",
        reply_markup=models_keyboard(data.get("count", 1)),
    )


@dp.callback_query(F.data.startswith("flatarrange:"))
async def on_flatarrange(cb: CallbackQuery, state: FSMContext):
    if not allowed(cb.from_user.id):
        return
    val = cb.data.split(":", 1)[1]
    data = await state.get_data()
    manual = dict(data.get("manual_style") or {})
    if val == "all":
        manual.pop("arrange", None)
        await state.update_data(manual_style=manual, all_arrange=True)
        label = "🎲 All (4 layouts)"
    else:
        try:
            idx = int(val)
        except ValueError:
            await cb.answer()
            return
        if idx < 0 or idx >= len(_ARRANGE):
            await cb.answer()
            return
        manual["arrange"] = _ARRANGE[idx]
        await state.update_data(manual_style=manual, all_arrange=False)
        label = ARRANGE_LABELS[idx]
    await cb.answer()
    await cb.message.answer(
        f"Layout: {label}\n\n<b>Choose an angle</b>", reply_markup=angle_keyboard())


@dp.callback_query(F.data.startswith("flatangle:"))
async def on_flatangle(cb: CallbackQuery, state: FSMContext):
    if not allowed(cb.from_user.id):
        return
    val = cb.data.split(":", 1)[1]
    data = await state.get_data()
    manual = dict(data.get("manual_style") or {})
    if val == "all":
        manual.pop("angle", None)
        await state.update_data(manual_style=manual, all_angle=True)
        label = "🎲 Both"
    else:
        try:
            idx = int(val)
        except ValueError:
            await cb.answer()
            return
        if idx < 0 or idx >= len(_FL_ANGLE):
            await cb.answer()
            return
        manual["angle"] = _FL_ANGLE[idx]
        await state.update_data(manual_style=manual, all_angle=False)
        label = ANGLE_LABELS[idx]
    await cb.answer()
    await cb.message.answer(
        f"Angle: {label}\n\n<b>Choose a background</b>", reply_markup=flatbg_keyboard())


@dp.callback_query(F.data.startswith("flatbg:"))
async def on_flatbg(cb: CallbackQuery, state: FSMContext):
    """Останній крок Flat lay — конкретний фон."""
    if not allowed(cb.from_user.id):
        return
    try:
        idx = int(cb.data.split(":", 1)[1])
    except ValueError:
        await cb.answer()
        return
    if idx < 0 or idx >= len(_BG):
        await cb.answer()
        return
    data = await state.get_data()
    manual = dict(data.get("manual_style") or {})
    manual["bg"] = _BG[idx]
    await state.update_data(manual_style=manual)
    label = BG_LABELS[idx]
    await cb.answer()
    await cb.message.answer(
        f"Background: {label}\n\n<b>Start the generation</b>",
        reply_markup=models_keyboard(data.get("count", 1)),
    )


@dp.callback_query(F.data == "fullset")
async def on_fullset(cb: CallbackQuery, state: FSMContext):
    """Повний комплект — товар у всіх 5 кадрах одним тапом.
    Майстер із 5 кроків: кількість поз (Анфас) → спільний фон → локація → світло → кількість поз (Місто)."""
    if not allowed(cb.from_user.id):
        return
    data = await state.get_data()
    if not data.get("file_ids"):
        await cb.answer("Send a product photo first.", show_alert=True)
        return
    await state.update_data(
        mode="scene", full_set=True, manual_style=None, all_crop=False, all_arrange=False, all_angle=False, all_poses=False,
        full_front_poses=None, full_bg_idx=None, full_light_idx=None, full_location_idx=None,
        full_city_light_idx=None, full_city_poses=None, full_back_pos=None,
    )
    await cb.answer()
    n = len(data.get("file_ids") or [])
    note = "" if n <= 1 else f"{n} products · "
    await cb.message.answer(
        "🎁 <b>Full set</b>\n"
        f"{note}5 shots: front, back, flat lay, mannequin, city\n\n"
        "<b>How many poses for Front?</b> (1-6)",
        reply_markup=fullposes_keyboard(),
    )


@dp.callback_query(F.data.startswith("fullposes:"))
async def on_fullposes(cb: CallbackQuery, state: FSMContext):
    if not allowed(cb.from_user.id):
        return
    try:
        n = int(cb.data.split(":", 1)[1])
    except ValueError:
        await cb.answer()
        return
    if n < 1 or n > 6:
        await cb.answer()
        return
    await state.update_data(full_front_poses=n)
    await cb.answer()
    data = await state.get_data()
    if data.get("gender") in ("female", "female_plus"):
        await cb.message.answer(
            f"Front poses: <b>{n}</b>\n\n<b>Choose the hair style for Back</b>",
            reply_markup=fullbackpos_keyboard(),
        )
        return
    await cb.message.answer(
        f"Front poses: <b>{n}</b>\n\n"
        "<b>Choose a shared background</b> (Front, Back, Flat lay, Mannequin)",
        reply_markup=fullbg_keyboard(),
    )


@dp.callback_query(F.data.startswith("fullbackpos:"))
async def on_fullbackpos(cb: CallbackQuery, state: FSMContext):
    if not allowed(cb.from_user.id):
        return
    val = cb.data.split(":", 1)[1]
    if val == "all":
        await state.update_data(full_back_pos="all")
        label = "🎲 All three (3 cards)"
    else:
        try:
            idx = int(val)
        except ValueError:
            await cb.answer()
            return
        if idx < 0 or idx >= len(BACKSTYLE_POSITION_LABELS):
            await cb.answer()
            return
        await state.update_data(full_back_pos=idx)
        label = BACKSTYLE_POSITION_LABELS[idx]
    await cb.answer()
    await cb.message.answer(
        f"Hair style (Back): {label}\n\n"
        "<b>Choose a shared background</b> (Front, Back, Flat lay, Mannequin)",
        reply_markup=fullbg_keyboard(),
    )


@dp.callback_query(F.data.startswith("fullbg:"))
async def on_fullbg(cb: CallbackQuery, state: FSMContext):
    if not allowed(cb.from_user.id):
        return
    try:
        idx = int(cb.data.split(":", 1)[1])
    except ValueError:
        await cb.answer()
        return
    if idx < 0 or idx >= len(_BG):
        await cb.answer()
        return
    await state.update_data(full_bg_idx=idx)
    await cb.answer()
    await cb.message.answer(
        f"Shared background: {BG_LABELS[idx]}\n\n"
        "<b>Choose shared lighting</b> (Front, Back, Flat lay, Mannequin)",
        reply_markup=fulllight_keyboard(),
    )


@dp.callback_query(F.data.startswith("fulllight:"))
async def on_fulllight(cb: CallbackQuery, state: FSMContext):
    if not allowed(cb.from_user.id):
        return
    try:
        idx = int(cb.data.split(":", 1)[1])
    except ValueError:
        await cb.answer()
        return
    if idx < 0 or idx >= len(_LIGHT_STUDIO):
        await cb.answer()
        return
    await state.update_data(full_light_idx=idx)
    await cb.answer()
    await cb.message.answer(
        f"Shared lighting: {LIGHT_LABELS[idx]}\n\n<b>Choose a location for “In the city”</b>",
        reply_markup=fulllocation_keyboard(),
    )


@dp.callback_query(F.data.startswith("fulllocation:"))
async def on_fulllocation(cb: CallbackQuery, state: FSMContext):
    if not allowed(cb.from_user.id):
        return
    try:
        idx = int(cb.data.split(":", 1)[1])
    except ValueError:
        await cb.answer()
        return
    if idx < 0 or idx >= len(_LOCATION):
        await cb.answer()
        return
    await state.update_data(full_location_idx=idx)
    await cb.answer()
    await cb.message.answer(
        f"Location: {LOCATION_LABELS[idx]}\n\n<b>Choose the lighting for “In the city”</b>",
        reply_markup=fullcitylight_keyboard(),
    )


@dp.callback_query(F.data.startswith("fullcitylight:"))
async def on_fullcitylight(cb: CallbackQuery, state: FSMContext):
    if not allowed(cb.from_user.id):
        return
    try:
        idx = int(cb.data.split(":", 1)[1])
    except ValueError:
        await cb.answer()
        return
    if idx < 0 or idx >= len(_DAYLIGHT):
        await cb.answer()
        return
    await state.update_data(full_city_light_idx=idx)
    await cb.answer()
    await cb.message.answer(
        f"Lighting: {DAYLIGHT_LABELS[idx]}\n\n<b>How many different poses for “In the city”?</b> (1-4)",
        reply_markup=fullcitycount_keyboard(),
    )


@dp.callback_query(F.data.startswith("fullcitycount:"))
async def on_fullcitycount(cb: CallbackQuery, state: FSMContext):
    """Останній крок майстра «Повний комплект» — далі йде вибір нейромережі."""
    if not allowed(cb.from_user.id):
        return
    try:
        n = int(cb.data.split(":", 1)[1])
    except ValueError:
        await cb.answer()
        return
    if n < 1 or n > 4:
        await cb.answer()
        return
    await state.update_data(full_city_poses=n)
    await cb.answer()
    await cb.message.answer(
        f"“In the city” photos: <b>{n}</b>\n\n<b>Start the generation</b>",
        reply_markup=models_keyboard(show_count=False),
    )


@dp.callback_query(F.data.startswith("cnt:"))
async def on_count(cb: CallbackQuery, state: FSMContext):
    """Переключение количества вариантов ×1/×2/×4 (обновляем ту же клавиатуру)."""
    if not allowed(cb.from_user.id):
        return
    try:
        n = int(cb.data.split(":", 1)[1])
    except ValueError:
        n = 1
    if n not in (1, 2, 4):
        await cb.answer()
        return
    await state.update_data(count=n)
    await cb.answer(f"×{n}")
    try:
        await cb.message.edit_reply_markup(
            reply_markup=models_keyboard(n))
    except Exception:
        pass


def result_keyboard() -> InlineKeyboardMarkup:
    """После результата: быстро повторить + выбор нового кадра."""
    rows = [[InlineKeyboardButton(text="🔄 One more", callback_data="again")]]
    rows += presets_keyboard().inline_keyboard
    return InlineKeyboardMarkup(inline_keyboard=rows)


def progress_bar(done: int, total: int, tick: int, width: int = 10) -> str:
    """Полоса прогресса: заполненные ▰, активная ячейка мигает, пустые ▱."""
    cells = min(max(total, 1), width)
    filled = int(cells * done / total) if total else cells
    out = "▰" * filled
    if filled < cells:                       # активная ячейка «работает» — мигает
        out += "▰" if tick % 2 else "▱"
        out += "▱" * (cells - filled - 1)
    return out


def _distinct_poses(scene: str, gender: str, n: int, style: dict | None) -> list | None:
    """n РІЗНИХ поз для сцени (без повторів, поки вистачає пулу; далі — нове коло
    у випадковому порядку). None — якщо в сцені немає поз або поза вже обрана вручну."""
    ps = PRESETS.get(scene, {}).get("pose_set")
    if not ps or (style and "pose" in style):
        return None
    pool = POSE_POOLS[gender][ps]
    out = []
    while len(out) < n:
        out += random.sample(pool, min(n - len(out), len(pool)))
    return out


def _build_jobs(data: dict, models: list, gender: str) -> list:
    """Собирает плоский список заданий генерации под текущий режим и параметры."""
    file_ids = data.get("file_ids") or []
    mode = data.get("mode", "scene")
    count = int(data.get("count", 1) or 1)
    full_set = bool(data.get("full_set"))
    color_id = data.get("color_file_id")
    user_model_idx = data.get("model_idx")   # обрана користувачем модель (None = випадкова)

    # -----------------------------------------------------------------
    # «Повний комплект» — окрема логіка (майстер із 5 налаштувань):
    # кількість поз для Анфас (1-6), спільний фон (Анфас/Спина/Flat lay/
    # Манекен), локація + світло + кількість поз для «У місті» (1-4).
    # -----------------------------------------------------------------
    if full_set:
        front_poses = int(data.get("full_front_poses") or 1)
        city_poses = int(data.get("full_city_poses") or 1)
        bg_idx = data.get("full_bg_idx")
        light_idx = data.get("full_light_idx")
        location_idx = data.get("full_location_idx")
        city_light_idx = data.get("full_city_light_idx")
        back_pos = data.get("full_back_pos")   # int (конкретна поза), "all" або None — лише female/female_plus

        if user_model_idx is not None:
            model_idx = user_model_idx
        else:
            idx_max = max(len(MODEL_POOLS[gender]["front"]), len(MODEL_POOLS[gender]["back"]))
            model_idx = random.randrange(idx_max)
        base_style = {"model_idx": model_idx, "camera": random.choice(_CAMERA)}
        back_reps = 3 if back_pos == "all" else 1
        reps_by_scene = {"front": front_poses, "back": back_reps, "flatlay": 1, "ghost": 1, "city": city_poses}

        jobs = []
        for pid in file_ids:
            for k in PRESETS.keys():
                local_style = dict(base_style)
                if bg_idx is not None and k in ("front", "back", "flatlay", "ghost"):
                    local_style["bg"] = _BG[bg_idx]   # тепер Flat lay теж має звичайну вісь "bg"
                if light_idx is not None:
                    if k in ("front", "back", "ghost"):
                        local_style["light"] = _LIGHT_STUDIO[light_idx]
                    elif k == "flatlay":
                        # у Flat lay світло — інший, коротший пул (_TOP_LIGHT); той самий
                        # індекс, щоб «однакове світло» відчувалось узгоджено між сценами.
                        local_style["__force__"] = {"light": _TOP_LIGHT[light_idx % len(_TOP_LIGHT)]}
                if k == "city":
                    if location_idx is not None:
                        local_style["location"] = _LOCATION[location_idx]
                    if city_light_idx is not None:
                        local_style["light"] = _DAYLIGHT[city_light_idx]
                if k == "back" and isinstance(back_pos, int):
                    local_style["back_pos_idx"] = back_pos
                poses = _distinct_poses(k, gender, reps_by_scene[k], local_style)
                for rep_i in range(reps_by_scene[k]):
                    rep_style = dict(local_style)
                    if poses:
                        rep_style["pose"] = poses[rep_i]   # кожен повтор — інша поза, без дублів
                    if k == "back" and back_pos == "all":
                        rep_style["back_pos_idx"] = rep_i   # кожен повтор — інша поза волосся, той самий колір
                    for m in models:
                        spec, prompt = [("tg", pid)], compose(k, gender, rep_style)
                        title, tag = PRESETS[k]["title"], k
                        jobs.append({"spec": spec, "prompt": prompt, "title": title, "tag": tag,
                                     "model": m, "price": model_price(m["id"])})
        return jobs

    # -----------------------------------------------------------------
    # Звичайний режим: перефарбування / «Вінниця» / один обраний кадр.
    # -----------------------------------------------------------------
    if mode in ("recolor", "citybg"):
        scene_keys = [mode]
    else:
        scene_keys = [data.get("preset")] if data.get("preset") else []

    reps = count
    pool = BACKGROUNDS.get(data.get("season")) or []

    style = {"model_idx": user_model_idx} if user_model_idx is not None else None

    # Ручний вибір (Анфас/Спина/Flat lay/Манекен/Місто) — доповнює style,
    # не заміняючи вже підготовлені значення (напр. model_idx).
    manual_style = data.get("manual_style") or {}
    if manual_style:
        style = dict(style or {})
        style.update(manual_style)

    # Опис моделі для режиму «Вінниця»: якщо обрана конкретна модель —
    # використовуємо саме її повний опис замість загального MODEL_DESC.
    if user_model_idx is not None:
        front_pool = MODEL_POOLS[gender]["front"]
        citybg_model_desc = front_pool[user_model_idx % len(front_pool)]
    else:
        citybg_model_desc = MODEL_DESC[gender]

    jobs = []
    for pid in file_ids:
        for k in scene_keys:
            # «🎲 Всі / Обидва» — може бути кілька одночасно активних осей для однієї
            # сцени (напр. Flat lay: і розкладка, і ракурс) → декартів добуток усіх
            # активних осей замість однієї випадкової комбінації.
            expand_axes = []
            if k == "front" and data.get("all_crop"):
                expand_axes.append(("crop", _CROP))
            if k == "flatlay" and data.get("all_arrange"):
                expand_axes.append(("arrange", _ARRANGE))
            if k in ("flatlay", "ghost") and data.get("all_angle"):
                expand_axes.append(("angle", _FL_ANGLE if k == "flatlay" else _GHOST_ANGLE))
            if k == "city" and data.get("all_poses"):
                expand_axes.append(("pose", POSE_POOLS[gender]["city"]))
            combos = list(itertools.product(*[vals for _, vals in expand_axes])) if expand_axes else [()]
            for combo in combos:
                local_style = style
                if combo:
                    local_style = dict(style or {})
                    for (axis, _), val in zip(expand_axes, combo):
                        local_style[axis] = val
                poses = _distinct_poses(k, gender, reps, local_style) if k in PRESETS else None
                for rep_i in range(reps):
                    rep_style = local_style
                    if poses:
                        rep_style = dict(local_style or {})
                        rep_style["pose"] = poses[rep_i]   # «Скільки поз?» — справді різні пози
                    for m in models:
                        if k == "recolor":
                            spec = [("tg", pid), ("tg", color_id)]
                            prompt = recolor_prompt(data.get("recolor_target"))
                            title, tag = "🎨 Recolour", "recolor"
                        elif k == "citybg":
                            spec = [("tg", pid), ("raw", random.choice(pool))]
                            prompt = CITYBG_PROMPT.format(model_desc=citybg_model_desc)
                            title, tag = f"📍 Vinnytsia · {SEASONS[data['season']]['label']}", "vinnytsia"
                        else:
                            spec, prompt = [("tg", pid)], compose(k, gender, rep_style)
                            title, tag = PRESETS[k]["title"], k
                        jobs.append({"spec": spec, "prompt": prompt, "title": title, "tag": tag,
                                     "model": m, "price": model_price(m["id"])})
    return jobs


# Running generations: {user_id: {"id": run_id, "tasks": [Task], "cancelled": bool}}.
# One generation per user at a time, so the cancel button always points to the right run.
RUNNING: dict = {}


def cancel_keyboard(run_id: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="⛔ Cancel", callback_data=f"cancel:{run_id}")]])


async def run_generation(message: Message, user_id: int, state: FSMContext, choice: str):
    """Общая генерация: выбор нейросети, «Ще варіант», «Повний комплект», пакет фото."""
    data = await state.get_data()
    file_ids = data.get("file_ids") or []
    mode = data.get("mode", "scene")
    gender = data.get("gender", "female")
    count = int(data.get("count", 1) or 1)
    full_set = bool(data.get("full_set"))

    if not file_ids:
        await message.answer("Send a product photo first.")
        return
    if choice == "all":
        models = list(MODELS.values())
    elif choice in MODELS:
        models = [MODELS[choice]]
    else:
        await message.answer("That AI model is unavailable right now. Choose another one.")
        return

    # Предвалидация режимов
    if mode == "recolor" and not full_set and not data.get("color_file_id"):
        await message.answer("Send the colour photo first.")
        return
    if mode == "citybg" and not full_set and not (BACKGROUNDS.get(data.get("season")) or []):
        await message.answer("Choose a backdrop season.")
        return
    if not full_set and mode == "scene" and not data.get("preset"):
        await message.answer("Choose a shot first.")
        return

    jobs = _build_jobs(data, models, gender)
    total = len(jobs)
    if not total:
        return

    if user_id in RUNNING:
        await message.answer("⏳ A generation is still running. Wait for it to finish or cancel it with ⛔.")
        return
    run_id = uuid.uuid4().hex[:8]
    run = {"id": run_id, "tasks": [], "cancelled": False, "ok": 0}

    # Подпись процесса
    n_prod = len(file_ids)
    if full_set:
        base = "🎁 Full set · 5 shots"
    elif mode == "recolor":
        base = "🎨 Recolour"
    elif mode == "citybg":
        base = f"📍 Vinnytsia · {SEASONS[data['season']]['label']}"
    else:
        base = PRESETS[data["preset"]]["title"]
    parts = [base]
    if n_prod > 1:
        parts.append(f"{n_prod} products")
    if count > 1 and not full_set:
        parts.append(f"×{count}")
    est = sum(j["price"] for j in jobs)   # оценка стоимости прогона
    parts.append(f"≈ ${est:.3f}")
    subtitle = " · ".join(parts)

    # Денний ліміт резервується одразу на весь прогін (без await між перевіркою і
    # резервом), тож одночасні запуски різних людей разом не перевищать DAILY_LIMIT.
    # Невикористане (помилки / скасування) повертається у finally.
    if not _reserve(total):
        left = max(0, DAILY_LIMIT - _used_today)
        await message.answer(f"Daily limit reached: {total} needed, {left} left.")
        return

    # Registered right before the try: no await between the check above and here,
    # so a double click can't start two runs, and the finally always unlocks the user.
    RUNNING[user_id] = run
    try:
        await _run_generation_body(message, user_id, run, jobs, total, subtitle)
    finally:
        RUNNING.pop(user_id, None)
        _refund(total - run["ok"])


async def _run_generation_body(message: Message, user_id: int, run: dict,
                               jobs: list, total: int, subtitle: str):
    """Runs the jobs, shows progress with a cancel button, delivers results."""
    done = 0
    cancel_kb = cancel_keyboard(run["id"])
    status = await message.answer(
        f"🎨 <b>Generating…</b>\n{subtitle}\n{progress_bar(0, total, 0)}  <i>0/{total}</i>",
        reply_markup=cancel_kb)
    stop = asyncio.Event()

    async def spin():
        i = 1
        while not stop.is_set():
            try:
                # 2.5s: every edit is a Telegram API call, faster risks 429 (flood limit)
                await asyncio.sleep(2.5)
                if stop.is_set():
                    break
                if run["cancelled"]:
                    await status.edit_text(
                        f"⛔ <b>Cancelling…</b>\n{subtitle}\n"
                        "<i>No new tasks will start; those already sent to PiAPI may still be billed.</i>")
                    break
                # reply_markup must be passed on every edit, otherwise the button disappears
                await status.edit_text(
                    f"🎨 <b>Generating…</b>\n{subtitle}\n"
                    f"{progress_bar(done, total, i)}  <i>done {done}/{total}</i>",
                    reply_markup=cancel_kb)
            except Exception:
                pass
            i += 1

    spin_task = asyncio.create_task(spin())
    results: list = [None] * total
    sem = asyncio.Semaphore(4)   # не хамим апи параллелью

    # Кеш фото з Telegram на цей прогін: одне й те саме фото товару для 15 кадрів
    # «Повного комплекту» завантажується ОДИН раз, а не 15.
    photo_cache: dict = {}

    def photo_uri(file_id: str):
        if file_id not in photo_cache:
            photo_cache[file_id] = asyncio.ensure_future(tg_data_uri(file_id))
        return photo_cache[file_id]

    async with aiohttp.ClientSession() as session, \
            ChatActionSender.upload_document(bot=bot, chat_id=message.chat.id):
        async def run_one(idx: int, job: dict):
            nonlocal done
            async with sem:
                try:
                    # asyncio.shield: скасування однієї задачі не повинно зламати
                    # спільне (кешоване) завантаження фото для інших задач.
                    imgs = [await asyncio.shield(photo_uri(v)) if kind == "tg" else v
                            for kind, v in job["spec"]]
                    out = await generate(session, job["model"]["id"], imgs,
                                         job["prompt"])
                    raw = await download(session, out)
                    # Pillow (LANCZOS + JPEG q95) — у потоці, щоб не блокувати бота для всіх.
                    img = await asyncio.to_thread(fit_to_target, raw)
                    run["ok"] += 1
                    USAGE[user_id] = USAGE.get(user_id, 0) + 1
                    bump_stats(user_id, job["model"]["key"], job["price"])   # учёт по модели + расход
                    results[idx] = (img, job)
                except Exception as e:
                    log.exception("ошибка генерации %s", job["model"]["id"])
                    results[idx] = (None, job, str(e))
                finally:
                    done += 1

        run["tasks"] = [asyncio.create_task(run_one(i, j)) for i, j in enumerate(jobs)]
        # return_exceptions=True: a cancelled task must not abort the whole gather
        await asyncio.gather(*run["tasks"], return_exceptions=True)

    stop.set()
    try:
        await spin_task
    except Exception:
        pass
    for fut in photo_cache.values():   # прибираємо незавершені / неспоживані завантаження
        if not fut.done():
            fut.cancel()
        elif not fut.cancelled():
            fut.exception()            # «забираємо» помилку, щоб не було warning у лозі
    save_usage()
    save_stats()
    try:
        await status.delete()
    except Exception:
        pass

    # Доставка: один результат — документом, несколько — альбомами по 10
    ok = [r for r in results if r and r[0] is not None]
    errs = [r for r in results if r and r[0] is None]

    def cap(job):
        return f"<b>{job['model']['label']}</b> · {job['title']}"

    async def deliver(items):
        if len(items) == 1:
            img, job = items[0]
            await message.answer_document(
                BufferedInputFile(img, f"{job['tag']}_{job['model']['key']}.jpg"),
                caption=cap(job))
        else:
            media = [InputMediaDocument(
                media=BufferedInputFile(img, f"{job['tag']}_{job['model']['key']}_{j}.jpg"),
                caption=cap(job), parse_mode="HTML") for j, (img, job) in enumerate(items)]
            await bot.send_media_group(message.chat.id, media=media)

    for start in range(0, len(ok), 10):
        await deliver(ok[start:start + 10])

    if run["cancelled"]:
        await message.answer(
            f"⛔ <b>Generation cancelled.</b>\nFinished: <b>{len(ok)}</b> of {total}"
            + (" — sent above." if ok else ".")
            + "\n<i>Tasks that already reached PiAPI may still be billed.</i>")
    elif errs:
        await message.answer(f"⚠️ Failed: <b>{len(errs)}</b> of {total}. Please try again.")
    done_text = ("Choose the next shot 👇" if run["cancelled"]
                 else "✅ <b>Done</b>\n\nChoose the next shot 👇")
    try:   # полноэкранный эффект 🎉 (может не поддерживаться — тогда без него)
        await message.answer(done_text, reply_markup=result_keyboard(),
                             message_effect_id=EFFECT_DONE)
    except Exception:
        await message.answer(done_text, reply_markup=result_keyboard())


@dp.callback_query(F.data.startswith("model:"))
async def on_model(cb: CallbackQuery, state: FSMContext):
    if not allowed(cb.from_user.id):
        return
    choice = cb.data.split(":", 1)[1]
    await state.update_data(last_choice=choice)  # запомним для «Ще варіант»
    await cb.answer()
    await run_generation(cb.message, cb.from_user.id, state, choice)


@dp.callback_query(F.data.startswith("cancel:"))
async def on_cancel(cb: CallbackQuery):
    """Cancel button under the progress message."""
    if not allowed(cb.from_user.id):
        await cb.answer()
        return
    run_id = cb.data.split(":", 1)[1]
    run = RUNNING.get(cb.from_user.id)
    if not run or run["id"] != run_id:
        await cb.answer("This generation has already finished.")
        return
    if run["cancelled"]:
        await cb.answer("Already cancelling…")
        return
    run["cancelled"] = True
    for t in run["tasks"]:
        t.cancel()
    log.info("generation %s cancelled by user %s", run_id, cb.from_user.id)
    await cb.answer("Cancelling…")


@dp.callback_query(F.data == "again")
async def on_again(cb: CallbackQuery, state: FSMContext):
    if not allowed(cb.from_user.id):
        return
    data = await state.get_data()
    choice = data.get("last_choice")
    if not choice or not data.get("file_ids"):
        await cb.answer("There is no previous generation.", show_alert=True)
        return
    await cb.answer("Generating one more…")
    await run_generation(cb.message, cb.from_user.id, state, choice)


@dp.message()
async def on_other(message: Message):
    """Любое прочее сообщение (текст, стикер): подсказываем, что делать."""
    if not allowed(message.from_user.id):
        await request_access(message)
        return
    await message.answer("📷 Send a <b>product photo</b> to get started.\n/help — how it works")


async def setup_profile():
    """Меню команд и описание бота — то, что видно в интерфейсе Telegram."""
    common = [
        BotCommand(command="start", description="✨ Start"),
        BotCommand(command="help", description="❓ How to use it"),
        BotCommand(command="profile", description="👤 My profile"),
        BotCommand(command="id", description="🆔 My Telegram ID"),
    ]
    await bot.set_my_commands(common, scope=BotCommandScopeDefault())
    # Админу — дополнительно /users (видно только в его чате)
    if ADMIN_ID:
        try:
            await bot.set_my_commands(
                common + [BotCommand(command="users", description="👥 Users and access")],
                scope=BotCommandScopeChat(chat_id=ADMIN_ID),
            )
        except Exception:
            log.exception("не удалось задать админ-меню")
    await bot.set_my_short_description(
        "Studio clothing cards from a single product photo — on a model, flat lay, in the city."
    )
    await bot.set_my_description(
        "✨ DP Photo — studio clothing shots from a single photo.\n\n"
        "Send a photo of your product and get a ready marketplace card: "
        "on a model (front/back), flat lay, ghost mannequin or out on a city street. "
        "Recolouring from a colour sample is included too.\n\n"
        "Tap “Start”."
    )


def acquire_lock() -> bool:
    """Бере єдиний замок на папку бота. False — отже вже є живий примірник."""
    global _lock_fd
    try:
        _lock_fd = open(LOCK_FILE, "w")
        fcntl.flock(_lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        _lock_fd.write(str(os.getpid()))
        _lock_fd.flush()
        return True
    except BlockingIOError:
        return False
    except Exception:
        # Замок — запобіжник, а не умова роботи: якщо файлова система його не
        # підтримує, краще запустити бота, ніж не запустити зовсім.
        log.exception("не вдалося взяти замок, працюємо без нього")
        return True


async def main():
    if not acquire_lock():
        log.error("Бот уже запущено (%s). Другий примірник не стартує — "
                  "інакше обидва вибивали б один одного з getUpdates.", LOCK_FILE)
        return
    log.info("Бот запускается. Модели: %s", ", ".join(m["id"] for m in MODELS.values()))
    await bot.delete_webhook(drop_pending_updates=True)
    await setup_profile()
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
