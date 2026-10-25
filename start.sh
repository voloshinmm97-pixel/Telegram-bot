#!/bin/bash
# Піднімає бота, якщо він не працює. Ставиться в cron: @reboot і раз на 2 хв.
#
# Остаточну гарантію «один бот» дає не цей скрипт, а замок усередині bot.py
# (flock на bot.lock): навіть якщо перевірка нижче помилиться, другий
# примірник вийде сам і не битиметься за getUpdates.

export PATH=/usr/local/bin:/usr/bin:/bin

# Папка самого скрипта, а не зашитий шлях: так він однаково працює на сервері
# і локально й не ламається, якщо проєкт переїде.
DIR="$(cd "$(dirname "$0")" && pwd)" || exit 1
cd "$DIR" || exit 1

# Інтерпретатор беремо з PATH; /usr/bin/python3 — запасний варіант.
PY="$(command -v python3 || echo /usr/bin/python3)"

# Шукаємо за іменем файлу, а не за рядком 'python3 -u bot.py'. Старий шаблон
# мовчки не знаходив живий процес там, де інтерпретатор зветься інакше —
# на macOS це /…/Python.app/Contents/MacOS/Python -u bot.py. Через це
# watchdog вважав бота мертвим і піднімав ще один примірник.
if pgrep -f "bot\.py" >/dev/null; then
    exit 0
fi

# setsid відвʼязує процес від термінала, але існує лише в Linux (util-linux).
# На macOS його немає, і зі старим рядком скрипт мовчки не запускав нічого:
# помилка йшла в bot.log, а cron її не показував. nohup + & тут достатньо.
SETSID=""
command -v setsid >/dev/null 2>&1 && SETSID="setsid"

$SETSID nohup "$PY" -u bot.py >> bot.log 2>&1 < /dev/null &
