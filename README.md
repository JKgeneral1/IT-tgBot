# IntraDesk Telegram Bot

Бот для работы с системой **IntraDesk** прямо в Telegram: создание заявок, комментарии (с файлами), автосмена статусов, поддержка **личных чатов и групп** (включая темы/форумы).

---

## ⚙️ Возможности

* Создание заявок из Telegram (личка и группы)
* Привязка темы/топика группы к заявке: все сообщения внутри — комментарии в заявку
* Загрузка вложений (фото/файлы) в комментарии
* Автоперевод в статус «Открыта» при новом комментарии из статусов «Отложена/Требует уточнения»
* Вебхук для получения событий из IntraDesk (опционально)
* Подробное логирование

---

## 📦 Состав проекта

```
.
├─ modules/                 # Модули бота
│  ├─ config.py
│  ├─ db.py
│  ├─ handlers.py
│  ├─ intradesk_api.py
│  ├─ logger.py
│  ├─ status_cache.py
│  ├─ telegram_bot.py
│  ├─ telegram_utils.py
│  └─ utils.py
├─ main.py                  # Точка входа Telegram-бота (webhook)
├─ idk_webhook.py           # Вебхук-приёмник от IntraDesk (опционально)
├─ config_example.ini       # Пример конфига (скопируйте в config.ini)
├─ config.ini               # Рабочий конфиг (создаёте сами)
├─ Dockerfile
├─ docker-compose.yml
└─ requirements.txt
```

---

## 🖥️ Требования к серверу

* VPS с Ubuntu 22.04+ (или Debian 11+)
* Домен с валидным SSL-сертификатом (для Telegram вебхука)
* Docker и Docker Compose
* Рекомендуемая память: 2 ГБ RAM

Установка Docker (если ещё не установлен):

```bash
curl -fsSL https://get.docker.com | sh
sudo apt install -y docker-compose
```

---

## 🚀 Быстрый запуск (без Git)

1. **Загрузите архив** проекта на сервер (zip/tar.gz) и распакуйте:

```bash
mkdir -p /opt/it-bot && cd /opt/it-bot
# загрузите архив в эту папку любым удобным способом
unzip it-bot.zip
```

2. **Создайте конфиг** `config.ini` на основе примера:

```bash
cp config_example.ini config.ini
nano config.ini
```

Заполните поля:

* `Telegram.token` — токен бота из @BotFather
* `Webhook.public_base` — ваш домен с HTTPS (например, [https://bot.example.com](https://bot.example.com))
* `Webhook.path` — уникальный путь вебхука, напр. `tg/supersecret123`
* `IntraDesk.api_key`, `IntraDesk.auth_token` — ключи из IntraDesk

3. **Соберите и запустите** контейнеры:

```bash
docker-compose build
docker-compose up -d
```

4. **Проверьте логи** (по желанию):

```bash
docker-compose logs -f bot
```

5. **Установите вебхук Telegram** (замените токен и адрес):

```bash
curl -F "url=https://<ваш-домен>/<путь-из-конфига>" \
     https://api.telegram.org/bot<TELEGRAM_BOT_TOKEN>/setWebhook
```

Проверка:

```bash
curl https://api.telegram.org/bot<TELEGRAM_BOT_TOKEN>/getWebhookInfo
```

---

## 🔧 Настройка @BotFather (для работы в группах)

Чтобы бот видел сообщения в группах:

1. В @BotFather → `/mybots` → выбрать бота
2. **Bot Settings** → **Group Privacy** → **Turn off** (выключить Privacy Mode)
3. В группе дать боту права читать сообщения. Если используются темы/форумы — также право **Manage Topics**.

---

## 🔄 Обновление/перезапуск

```bash
# Перезапуск
docker-compose restart

# Обновление c заменой файлов архива
docker-compose down
# замените файлы проекта на новые
docker-compose build
docker-compose up -d
```

---

## 🗄️ О базе данных (SQLite)

В папке данных появляются файлы:

* `tickets.db` — основная БД
* `tickets.db-shm` — служебная shared-memory (режим WAL)
* `tickets.db-wal` — журнал изменений

Удалять **нельзя**, если база нужна. Для «чистого старта» допустимо удалить все три файла — бот создаст пустую базу при следующем запуске.

## 🧰 Диагностика

* Контейнеры запущены? → `docker ps`
* Логи бота → `docker-compose logs -f bot`
* Вебхук установлен? → `getWebhookInfo`
* SSL действителен? Домен совпадает с `Webhook.public_base`?

