# Instrukcja uruchomienia projektu

## Krok 0: Przygotowanie danych

W głównym katalogu projektu utwórz folder `data`. W nim umieść:

- plik `airports.csv`
- folder `flights`, zawierający dane z lotów (pliki `.csv`)

## Krok 1: Uruchomienie środowiska Kafka i MongoDB (terminal 1)

```bash
docker compose up zookeeper kafka producer mongodb
```

Poczekaj aż zobaczysz utworzenie nowego tematu Kafki, oraz wyświetlane wiadomości wysyłane przez producenta.

## Krok 2: Uruchomienie Spark (terminal 2)

```bash
docker compose up spark
```

## Krok 3: Sprawdzanie danych w MongoDB (terminal 3)

1. Wejdź do kontenera MongoDB:

```bash
docker exec -it mongodb bash
```

2. Uruchom powłokę MongoDB:

```bash
mongosh
```

3. Wybierz bazę danych:

```js
use flight_data
```

4. Wyświetl dane z kolekcji:

```js
db.flight_anomalies.find().pretty()
db.daily_state_aggregates.find().pretty()
```

Dla zadanych parametrów wykrywania anomalii, mogą one występować bardzo rzadko lub po dłuższym przetwarzaniu, więc kolekcja `flight_anomalies` może być pusta.

# Zawartość projektu

## Obrazy kontenerów użyte w projekcie:
- `confluentinc/cp-kafka:7.4.0`
- `bitnami/spark:3.5.1`
- `mongo:6.0`
- `confluentinc/cp-zookeeper:7.4.0`

## Usuwanie i tworzenie tematów Kafki

Po odpaleniu kontenera kafki odpalany zostaje skrypt `reset_kafka.sh` który:
- Usuwa temat `flights`:
```bash
kafka-topics.sh --delete --topic flights --bootstrap-server $BROKER || echo "Topic 'flights' did not exist or couldn't be deleted. Continuing..."
```
  
- Tworzy nowy temat `flights`:
```bash
kafka-topics.sh --create --topic flights --bootstrap-server $BROKER --partitions 1 --replication-factor 1
```

Skrypt zawiera mechanizm powtarzający próbę stworzenia tematu do 10 razy, aż do skutku.

## Wysyłanie danych przez producenta Kafki

Producent Kafki odczytuje dane o lotach z plików, linia po linii, a następnie wysyła je na temat `flights`.

## Przetwarzanie danych w Spark Streaming

Spark subskrybuje temat `flights`,  z którego odczytuje dane o lotach. Odczytuje również z pliku `airports.csv` dane o lotniskach, potrzebne do agregacji.
W skrypcie zachodzą następujące agregacje:
1. Liczba odlotów na dzień i stan pochodzenia (origin state)
2. Suma dodatnich opóźnień odlotów na dzień i stan pochodzenia
3. Liczba przylotów na dzień i stan docelowy (destination state)
4. Suma dodatnich opóźnień przylotów na dzień i stan docelowy

## Zapisywanie stanu opóźnień oraz anomalii w `MongoDB`

Dzięki konektorowi Spark-MongoDB dane z przetworzeń są zapisywane bezpośrednio do bazy MongoDB.
Agregacje zapisywane są metodą `write_to_mongo_upsert`, która aktualizuje istniejące rekordy lub tworzy nowe w kolekcji `daily_state_aggregates`.
Anomalie są wykrywane i zapisywane metodą `process_anomaly_batch`, która najpierw analizuje dane do anomalii, a następnie w przypadku wystąpienia takiej anomalii, zapisuje je w kolekcji `flight_anomalies`.

