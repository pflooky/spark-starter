# spark-starter
Spark Starter

### Build (Scala)
```
./gradlew clean build
```

### Run (Scala)
```
./gradlew runApp
```

### Setup and Run (Python/PySpark)

1.  **Create a virtual environment (optional but recommended):**
    ```bash
    python3 -m venv .venv
    source .venv/bin/activate  # On Windows use `.venv\Scripts\activate`
    ```

2.  **Install dependencies:**
    ```bash
    pip install -r requirements.txt
    ```

3.  **Run the PySpark application:**
    ```bash
    python src/main/python/spark_epl_standings.py
    ```

4.  **Deactivate the virtual environment (if you used one):**
    ```bash
    deactivate
    ```

