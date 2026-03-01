# 📊 Financial Market ETL Pipeline

Pipeline ETL automatizado que extrae precios de mercados financieros desde Yahoo Finance, los transforma y carga en Azure SQL Database, ejecutándose diariamente de forma serverless mediante Azure Functions.

---

## 🏗️ Arquitectura

```
[Yahoo Finance API]
        ↓
[Python ETL - Extract → Transform → Load]
        ↓
[Azure SQL Database]
        ↑
[Azure Functions - Timer Trigger diario]
        ↑
[Azure Key Vault - Credenciales seguras]
```

---

## ⚙️ Stack Tecnológico

| Capa | Tecnología |
|---|---|
| Lenguaje | Python 3.11 |
| Extracción | yfinance |
| Transformación | pandas |
| Base de datos local | SQL Server Express (.\SQLEXPRESS) |
| Base de datos cloud | Azure SQL Database |
| ORM / Conexión | SQLAlchemy + pyodbc |
| Automatización cloud | Azure Functions (Timer Trigger) |
| Seguridad | Azure Key Vault |
| Reintentos | tenacity |
| Testing | pytest |

---

## 📁 Estructura del Proyecto

```
financial-etl-pipeline/
├── etl/
│   ├── extract.py          # Extracción desde Yahoo Finance con reintentos automáticos
│   ├── transform.py        # Limpieza, validación y cálculo de métricas
│   └── load.py             # Carga idempotente en SQL (local o Azure)
├── config/
│   └── settings.py         # Configuración centralizada y detección de entorno
├── tests/
│   └── test_transform.py   # Tests unitarios de transformación
├── function_app.py         # Azure Function con Timer Trigger
├── main.py                 # Orquestador principal del pipeline
├── requirements.txt        # Dependencias Python
├── host.json               # Configuración de Azure Functions
├── local.settings.json     # Variables de entorno locales (no subir a GitHub)
└── .env                    # Credenciales locales (no subir a GitHub)
```

---

## 🗄️ Modelo de Datos

Modelo dimensional con tres tablas:

```
dim_activo              dim_fecha               fact_precios
──────────────          ──────────────          ──────────────────
activo_id (PK)          fecha_id (PK)           precio_id (PK)
ticker                  fecha                   activo_id (FK)
nombre                  anio                    fecha_id (FK)
tipo                    mes                     precio_open
sector                  dia                     precio_close
                        dia_semana              precio_high
                                                precio_low
                                                volumen
                                                variacion_pct
```

**Activos monitoreados:**
- `AAPL` — Apple Inc.
- `MSFT` — Microsoft Corp.
- `BTC-USD` — Bitcoin
- `GC=F` — Oro (Gold Futures)

---

## 🚀 Instalación y Uso Local

### Prerrequisitos

- Python 3.11+
- SQL Server Express (`.\SQLEXPRESS`) o Azure SQL Database
- ODBC Driver 17 (local) o ODBC Driver 18 (Azure SQL)
- Azure CLI (para despliegue en la nube)
- Azure Functions Core Tools

### 1. Clonar el repositorio

```bash
git clone https://github.com/tu-usuario/financial-etl-pipeline.git
cd financial-etl-pipeline
```

### 2. Instalar dependencias

```bash
pip install -r requirements.txt
```

### 3. Configurar variables de entorno

Crea un archivo `.env` en la raíz del proyecto:

```bash
# Para SQL Server local
DB_CONNECTION_STRING=mssql+pyodbc://.\SQLEXPRESS/financial_pipeline?driver=ODBC+Driver+17+for+SQL+Server&trusted_connection=yes

# Para Azure SQL
# DB_CONNECTION_STRING=mssql+pyodbc://usuario:password@servidor.database.windows.net/financial-pipeline?driver=ODBC+Driver+18+for+SQL+Server&Encrypt=yes&TrustServerCertificate=no
```

### 4. Crear las tablas en SQL Server

Ejecuta el script en SSMS:

```sql
CREATE TABLE dim_activo (
    activo_id   INT IDENTITY(1,1) PRIMARY KEY,
    ticker      VARCHAR(20)  NOT NULL UNIQUE,
    nombre      VARCHAR(100) NOT NULL,
    tipo        VARCHAR(50)  NOT NULL,
    sector      VARCHAR(100) NULL
);

CREATE TABLE dim_fecha (
    fecha_id    INT IDENTITY(1,1) PRIMARY KEY,
    fecha       DATE         NOT NULL UNIQUE,
    anio        INT          NOT NULL,
    mes         INT          NOT NULL,
    dia         INT          NOT NULL,
    dia_semana  VARCHAR(20)  NOT NULL
);

CREATE TABLE fact_precios (
    precio_id       INT IDENTITY(1,1) PRIMARY KEY,
    activo_id       INT  NOT NULL REFERENCES dim_activo(activo_id),
    fecha_id        INT  NOT NULL REFERENCES dim_fecha(fecha_id),
    precio_open     DECIMAL(18,4) NOT NULL,
    precio_close    DECIMAL(18,4) NOT NULL,
    precio_high     DECIMAL(18,4) NOT NULL,
    precio_low      DECIMAL(18,4) NOT NULL,
    volumen         BIGINT        NOT NULL,
    variacion_pct   DECIMAL(8,4)  NULL,
    CONSTRAINT uq_activo_fecha UNIQUE (activo_id, fecha_id)
);
```

### 5. Ejecutar el pipeline

```bash
python main.py
```

---

## 🧪 Tests

```bash
pytest tests/ -v
```

Los tests cubren las funciones de transformación de forma aislada, sin depender de conexiones externas:

- Limpieza de fechas con timezone
- Renombre de columnas
- Cálculo de variación porcentual
- Validaciones de calidad de datos

---

## ☁️ Despliegue en Azure

### Recursos necesarios

| Recurso | Nombre |
|---|---|
| Resource Group | `rg-financial-etl` |
| SQL Server | `sql-financial-etl` |
| Azure SQL Database | `financial-pipeline` |
| Key Vault | `kv-financial-etl` |
| Storage Account | `stfinancialetl` |
| Function App | `func-financial-etl` |

### Desplegar la Function App

```bash
func azure functionapp publish func-financial-etl
```

### Horario de ejecución

La función corre automáticamente con este cron (UTC):

```
0 30 19 * * *   →   19:30 UTC = 16:30 hora Santiago
```

---

## 🔐 Seguridad

- Las credenciales de base de datos se almacenan en **Azure Key Vault**
- La Function App accede al Key Vault mediante **Managed Identity** (sin contraseñas en el código)
- El archivo `.env` y `local.settings.json` están excluidos del repositorio via `.gitignore`

---

## 📋 Características del Pipeline

| Característica | Implementación |
|---|---|
| **Idempotencia** | `UNIQUE CONSTRAINT (activo_id, fecha_id)` evita duplicados |
| **Reintentos automáticos** | `tenacity` con exponential backoff (3 intentos) |
| **Logging estructurado** | Logging centralizado con rotación de archivos (5MB × 3) |
| **Validación de datos** | Precios positivos, high ≥ low, sin fechas duplicadas |
| **Bulk insert** | Inserción masiva en lugar de fila por fila |
| **Multi-entorno** | Detecta automáticamente si corre local o en Azure |

---

## 📈 Consultas de ejemplo

```sql
-- Últimos precios por activo
SELECT TOP 20
    a.ticker,
    f.fecha,
    p.precio_close,
    p.variacion_pct
FROM fact_precios p
JOIN dim_activo a ON p.activo_id = a.activo_id
JOIN dim_fecha  f ON p.fecha_id  = f.fecha_id
ORDER BY f.fecha DESC, a.ticker;

-- Variación promedio mensual por activo
SELECT
    a.ticker,
    f.anio,
    f.mes,
    AVG(p.variacion_pct) AS variacion_promedio,
    MAX(p.precio_close)  AS precio_max,
    MIN(p.precio_close)  AS precio_min
FROM fact_precios p
JOIN dim_activo a ON p.activo_id = a.activo_id
JOIN dim_fecha  f ON p.fecha_id  = f.fecha_id
GROUP BY a.ticker, f.anio, f.mes
ORDER BY f.anio DESC, f.mes DESC;
```

---

## 🗺️ Roadmap

- [x] Fase 1 — Pipeline ETL local con SQL Server
- [x] Fase 2 — Robustez: reintentos, logging, variables de entorno, tests
- [x] Fase 3 — Automatización en Azure (Functions + SQL + Key Vault)
- [ ] Fase 4 — Reportería: dashboard en Power BI o reporte automático PDF/Excel

---

## 👤 Autor

**Ignacio Azua**  
[LinkedIn](https://linkedin.com/in/iaaz) · [GitHub](https://github.com/iazua)
