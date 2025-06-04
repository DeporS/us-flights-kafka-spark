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

Dla zadanych parametrów wykrywania anomalii, mogą one występować bardzo rzadko, więc kolekcja `flight_anomalies` może być pusta.

