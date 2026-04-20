# dock-app.oracle-log-exporter

Log Exporter dla Oracle 11g, uruchamiany w Dockerze, napisany w .NET 10.

Rozwiązanie:
- łączy się do Oracle wskazanym użytkownikiem,
- odczytuje metadane tabeli i dynamicznie eksportuje dostępne kolumny,
- pozwala ignorować wybrane kolumny,
- działa przyrostowo (bez duplikowania rekordów),
- zapisuje dane do pliku JSONL (jedna linia = jeden rekord JSON).

## Jak działa brak duplikatów

Eksporter używa watermarka zapisanego w pliku stanu:
- `LastTimestampUtc` (ostatnia wartość kolumny czasowej),
- `LastRowId` (tie-breaker dla rekordów z tym samym timestampem).

Zapytanie pobiera tylko rekordy:
- `timestamp > LastTimestampUtc`
- lub `timestamp = LastTimestampUtc AND ROWID > LastRowId`

Dzięki temu nawet przy wielu rekordach z tym samym znacznikiem czasu nie ma ponownego eksportu tych samych danych.

## Wymagania

- Docker + Docker Compose
- Dostęp sieciowy z kontenera do Oracle 11g
- Tabela z kolumną czasową wskazaną w `EXPORT_TIMESTAMP_COLUMN`

## Konfiguracja

1. Skopiuj plik środowiskowy:

```bash
cp .env.example .env
```

2. Uzupełnij `.env`:

- `ORACLE_HOST` - host Oracle
- `ORACLE_PORT` - port (domyślnie 1521)
- `ORACLE_SERVICE_NAME` - service name bazy
- `ORACLE_USER` / `ORACLE_PASSWORD` - użytkownik i hasło
- `EXPORT_TABLE` - `TABLE` albo `OWNER.TABLE`
- `EXPORT_TIMESTAMP_COLUMN` - kolumna czasowa używana do incremental fetch
- `EXPORT_IGNORED_COLUMNS` - lista CSV kolumn pomijanych
- `EXPORT_ADDITIONAL_WHERE` - opcjonalny dodatkowy filtr SQL
- `EXPORT_POLL_INTERVAL_SECONDS` - co ile sekund odpytywać bazę (np. 300 = 5 minut)
- `EXPORT_FETCH_BATCH_SIZE` - maksymalna liczba rekordów na cykl
- `EXPORT_INITIAL_LOOKBACK_MINUTES` - ile minut wstecz przy pierwszym uruchomieniu

## Uruchomienie

```bash
docker compose up -d --build
```

Logi:

```bash
docker compose logs -f oracle-log-exporter
```

Stop:

```bash
docker compose down
```

## Wyniki eksportu

- Dane: `./output/oracle-log-YYYYMMDD.jsonl`
- Stan watermarka: `./state/export-state.json`

## Struktura projektu

- `src/OracleLogExporter/Program.cs` - główna logika eksportera
- `src/OracleLogExporter/OracleLogExporter.csproj` - projekt .NET 10
- `Dockerfile` - obraz kontenera
- `docker-compose.yml` - uruchomienie usługi
- `.env.example` - szablon konfiguracji

## Uwagi praktyczne

- Użytkownik Oracle musi mieć uprawnienia do odczytu tabeli i metadanych (`ALL_TAB_COLUMNS`).
- Dla bardzo wysokiego wolumenu danych warto zwiększyć `EXPORT_FETCH_BATCH_SIZE` oraz skrócić interwał pollingu.
- `EXPORT_ADDITIONAL_WHERE` jest wstrzykiwany do zapytania jako fragment SQL - używaj wyłącznie zaufanej konfiguracji administracyjnej.
