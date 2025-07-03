# Etapa 2: Ejercicios Prácticos para Colektia

Desarrolla los siguientes ejercicios usando Python. Puedes escribir las respuestas directamente aquí o enviar un archivo .py con el código y comentarios necesarios.

1. **Agrupación compleja con Pandas**
    
    A partir de un DataFrame df con las columnas cliente_id, producto, fecha_registro y monto, obtén el total gastado por cada cliente por producto, ordenado de mayor a menor monto dentro de cada cliente.
    
2. **Transformación con Polars**
    
    Crea un LazyFrame en Polars que lea un CSV, filtre las filas donde la columna estado sea "activo", cree una nueva columna llamada anio_registro que extraiga el año de una columna fecha_registro, y finalmente agrupe por anio_registro contando la cantidad de registros por año.
    
3. **ETL simplificado**
    
    Simula un pequeño proceso ETL:
    
    - Lee un archivo CSV (puedes simularlo con un DataFrame de ejemplo).
    - Aplica una transformación (como limpiar nulos y estandarizar columnas a minúsculas).
    - Guarda el resultado en un archivo Parquet.
4. **Resolución de problema con Pandas**
    
    Dado un DataFrame con fechas desordenadas, encuentra el rango de fechas (mínima y máxima), y calcula la cantidad de días entre ambas.
    
5. **Git Workflow básico (pregunta abierta de código + texto)**
    
    Simula un escenario donde estás trabajando en una nueva funcionalidad en una rama llamada feature/nueva-funcionalidad. Escribe los comandos que usarías desde que creas la rama, hasta que haces push y abres un pull request. Explica cada paso brevemente.

---

## Requisitos

- Python 3.8 o +
- Paquetes pip
- Entorno virtual venv

---

## Configuración del entorno

### 1. Crear y activar un entorno virtual

```bash
python -m venv env
```
- En Linux/macOS:

    ```bash
    source env/bin/activate
    ```

- En Windows:

    ```bash
    .\env\Scripts\activate
    ```

### 2. Instalar dependencias

```bash
pip install requirements.txt
```

### 3. Ejecutar ejercicios

- Ejecutar script con los ejercicios 1, 2 y 4:

    ```bash
    python exercises.py
    ```

- Ejecutar script con el ejercicio 3, con el ETL:

    ```bash
    python etl_exercise.py
    ```