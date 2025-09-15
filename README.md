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
├─ main.py                  # Точка входа Telegram-бота (webhook)
├─ idk_webhook.py           # Вебхук-приёмник от IntraDesk (опционально)
├─ config_template.ini      # Шаблон конфига (скопируйте в config.ini)
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

## 🚀 Развёртывание из репозитория

1. **Клонируем проект** с GitHub:

```bash
git clone git@github.com:JKgeneral1/IT-tgBot.git
cd IT-tgBot
```

2. **Создаём конфиг** `config.ini` на основе шаблона:

```bash
cp config_template.ini config.ini
nano config.ini
```

Заполните поля:

* `Telegram.token` — токен бота из @BotFather
* `Webhook.public_base` — ваш домен с HTTPS (например, `https://bot.example.com`)
* `Webhook.path` — уникальный путь вебхука, напр. `tg/supersecret123`
* `IntraDesk.api_key`, `IntraDesk.auth_token` — ключи из IntraDesk

3. **Собираем и запускаем** контейнеры:

```bash
docker-compose up -d --build
```

4. **Проверяем логи**:

```bash
docker-compose logs -f bot
```

5. **Устанавливаем вебхук Telegram** (замените токен и адрес):

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
3. В группе дать боту права читать сообщения.
   Если используются темы/форумы — также право **Manage Topics**.

---

## 🔄 Обновление/перезапуск

```bash
# Обновление кода
git pull
docker-compose up -d --build

# Перезапуск контейнеров
docker-compose restart
```

---

## 🗄️ О базе данных (SQLite)

Файлы базы хранятся в каталоге `/data`:

* `tickets.db` — основная БД
* `tickets.db-shm` — служебная (shared-memory)
* `tickets.db-wal` — журнал изменений

Удалять **нельзя**, если база нужна.
Для «чистого старта» допустимо удалить все три файла — бот создаст пустую БД при следующем запуске.

---

## 🧰 Диагностика

* Контейнеры запущены? → `docker ps`
* Логи бота → `docker-compose logs -f bot`
* Вебхук установлен? → `getWebhookInfo`
* SSL действителен? Домен совпадает с `Webhook.public_base`?
