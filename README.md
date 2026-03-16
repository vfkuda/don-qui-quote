# don-qui-quote

Rust-проект с двумя приложениями:
- `quote_server` — генератор и стример котировок.
- `quote_client` — клиент с подпиской на котировки.

## Сборка
```bash
cargo build
```

## Проверка
### unit + smoke
```bash
cargo test
```

### только smoke
```bash
cargo test --test smoketest
```

## Протоколы

### TCP control-plane
Клиент отправляет команду:
```text
STREAM udp://<ip>:<port> <tickers_csv>
```
Пример:
```text
STREAM udp://127.0.0.1:34254 AAPL,TSLA
```

Ответы сервера:
- `OK`
- `ERR <message>`
- при дублирующей подписке (`тот же ip:port`): `ERR already streaming`

### UDP data-plane
Один UDP пакет содержит одно обновление для клиента со всеми его тикерами:
```json
{"quotes":[{"ticker":"AAPL","price":123.45,"volume":3456,"timestamp":1700000000000},{"ticker":"TSLA","price":234.56,"volume":2100,"timestamp":1700000000000}]}
```

### Keep-alive
- клиент -> сервер: `PING`
- сервер -> клиент: `PONG`
- если `PING` не приходит дольше `5s`, подписка удаляется.
- keep-alive используется для liveliness сервера, и для детекта деградации соединения на клиенте.
- клиент отслеживает отсутствие `PONG` в течение тайм-аута; и при необходимости переходит в повторную подписку 

## server <--> client interaction
see @flow.puml

## Константы
- `DEFAULT_TICK_INTERVAL = 1s`
- `PING_INTERVAL = 2s`
- `PING_TIMEOUT = 5s`

### Формат файла тикеров
Один тикер на строку:
```text
AAPL
GOOGL
TSLA
```
Пустые строки игнорируются.

## Запуск клиента
Поддерживаемые аргументы:
- `--server-addr`
- `--udp-port`
- `--tickers-file`

Пример1:
```bash
cargo run --bin quote_client -- \
  --server-addr 127.0.0.1:7000 \
  --udp-port 34254 \
  --tickers-file ./client1.tic
```

Пример2:
```bash
just runc1
```

## Запуск server'a
Поддерживаемые аргументы:
- --tcp-addr <TCP_ADDR>  [default: 127.0.0.1:7000]
- --udp-addr <UDP_ADDR>  [default: 127.0.0.1:7001]
- --tickers <TICKERS>    [default: AAPL,MSFT,GOOGL,AMZN,NVDA,META,TSLA]

Пример1:
```bash
cargo run --bin quote_server -- --tcp-addr 127.0.0.1:7000 --udp-addr 127.0.0.1:7001 --tickers AAPL,MSFT,GOOGL,AMZN,NVDA,META,TSLA
```

Пример2:****
```bash
just runs
```
