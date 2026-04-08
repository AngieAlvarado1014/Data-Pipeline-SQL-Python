import pandas as pd
import numpy as np

# Datos del E-commerce
data = {
    'id_transaccion': [1, 2, 2, 4, 5],
    'fecha_venta': ['2023-10-01', '02/10/2023', '2023-10-02', None, '2023.10.05'],
    'id_producto': [101, 102, 102, 103, 101],
    'cantidad': [1, 2, 2, -1, 3],
    'puntos_fidelidad': [10, np.nan, np.nan, 5, 15]
}

df_sucio = pd.DataFrame(data)
print("¡Datos listos para limpiar!")
df_sucio['fecha_venta'] = df_sucio['fecha_venta'].str.replace('.','-', regex=False)

# 1. Copia limpia y sin duplicados
df_limpio = df_sucio.drop_duplicates().copy()

# 2. Limpieza de fechas y eliminación de registros sin fecha 
df_limpio['fecha_venta'] = pd.to_datetime(df_limpio['fecha_venta'], errors='coerce')
df_limpio = df_limpio.dropna(subset=['fecha_venta'])

# 3. Limpieza de cantidades (Solo cantidades positivas)
df_limpio = df_limpio[df_limpio['cantidad'] > 0]
df_limpio['puntos_fidelidad'] = df_limpio['puntos_fidelidad'].fillna(0).astype(int)

# 4. Resetear el índice 
df_limpio = df_limpio.reset_index(drop=True)

print(df_limpio)

from sqlalchemy import create_engine

# 5. Creamos el 'motor' de la base de datos (se creará un archivo llamado 'empresa.db')
engine = create_engine('sqlite:///empresa.db')

# 6. Guardamos nuestro DataFrame limpio como una tabla llamada 'ventas_final'
df_limpio.to_sql('ventas_final', con=engine, if_exists='replace', index=False)

print("¡Tabla 'ventas_final' creada exitosamente en SQL!")

# Datos del catálogo
productos_data = {
    'id_producto': [101, 102, 103],
    'nombre_producto': ['Laptop Pro 15', 'Mouse Inalámbrico', 'Monitor 4K'],
    'precio_unitario': [1200.0, 25.50, 350.0]
}

df_productos = pd.DataFrame(productos_data)

# 6. La guardamos en la misma base de datos
df_productos.to_sql('productos', con=engine, if_exists='replace', index=False)

query = """
SELECT 
    v.id_transaccion, 
    p.nombre_producto, 
    v.cantidad, 
    p.precio_unitario,
    (v.cantidad * p.precio_unitario) AS total_venta
FROM ventas_final v
JOIN productos p ON v.id_producto = p.id_producto
"""

df_resultado = pd.read_sql(query, con=engine)
print(df_resultado)