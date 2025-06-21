## Архитектура проекта
```mermaid
raph LR
    A[Внешние источники данных] -->|API/Files| B(Airflow)
    B --> C{MinIO S3}
    C -->|Raw Data| D[Airflow ETL]
    D -->|Transformed Data| C
    C -->|Processed Data| E[(PostgreSQL DWH)]
    E --> F[metabase]
