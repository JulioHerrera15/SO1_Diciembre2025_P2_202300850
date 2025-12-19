import random
from locust import HttpUser, task, between

class GeneradorVentas(HttpUser):
    wait_time = between(1, 3)

    # Definimos el inventario correcto para que no se mezclen
    inventario = {
        0: ["Laptop", "Televisor", "Mouse", "Monitor", "Tablet", "Audifonos"], # Electrónica
        1: ["Camisa", "Zapato", "Pantalon", "Vestido", "Chamarra"],            # Ropa
        2: ["Licuadora", "Jabon", "Escoba", "Sarten", "Microondas"],           # Hogar
        3: ["Perfume", "Crema", "Labial", "Shampoo", "Maquillaje"]             # Belleza
    }

    @task
    def enviar_venta(self):
        # 1. Elegir una categoría al azar (0, 1, 2 o 3)
        cat_id = random.choice(list(self.inventario.keys()))
        
        # 2. Elegir un producto QUE PERTENEZCA a esa categoría
        nombre_producto = random.choice(self.inventario[cat_id])
        
        producto_id = f"{nombre_producto}_{random.randint(1000, 9999)}"

        # Resto de datos...
        precio = round(random.uniform(10.0, 500.0), 2)
        cantidad = random.randint(1, 5)

        payload = {
            "categoria": cat_id,
            "producto_id": producto_id,
            "precio": precio,
            "cantidad_vendida": cantidad
        }

        self.client.post("/venta", json=payload)