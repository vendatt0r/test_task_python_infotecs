import asyncio
import sqlite3
import json
import httpx
from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel
from typing import List, Optional
from datetime import datetime
from apscheduler.schedulers.asyncio import AsyncIOScheduler
import uvicorn


app = FastAPI()


db_name = "weather.db"


def initialize_db():
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()


    cursor.execute("""
    CREATE TABLE IF NOT EXISTS users (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        username TEXT NOT NULL UNIQUE
    )
    """)


    cursor.execute("""
    CREATE TABLE IF NOT EXISTS cities (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER NOT NULL,
        name TEXT NOT NULL,
        latitude REAL NOT NULL,
        longitude REAL NOT NULL,
        weather_data TEXT,
        last_updated DATETIME,
        FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
    )
    """)

    conn.commit()
    conn.close()


initialize_db()


BASE_URL = "https://api.open-meteo.com/v1/forecast"


scheduler = AsyncIOScheduler()


class WeatherResponse(BaseModel):
    temperature: Optional[float]
    wind_speed: Optional[float]
    pressure: Optional[float]

@app.post("/register_user")
async def register_user(username: str):
    """
    Регистрирует нового пользователя. Возвращает ID пользователя.

    - **username**: Имя пользователя (должно быть уникальным).
    """
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()

    try:
        cursor.execute("INSERT INTO users (username) VALUES (?)", (username,))
        conn.commit()
        user_id = cursor.lastrowid
    except sqlite3.IntegrityError:
        conn.close()
        raise HTTPException(status_code=400, detail="Имя пользователя уже существует")

    conn.close()
    return {"user_id": user_id}


async def fetch_weather(lat: float, lon: float):
    """
    Запрашивает погодные данные для заданных координат.

    - **lat**: Широта.
    - **lon**: Долгота.
    """
    params = {
        "latitude": lat,
        "longitude": lon,
        "hourly": ["temperature_2m", "relative_humidity_2m", "wind_speed_10m", "precipitation"],
        "timezone": "auto"
    }
    async with httpx.AsyncClient() as client:
        response = await client.get(BASE_URL, params=params)
        if response.status_code == 200:
            return response.json()
        return None



@app.get("/current_weather")
async def get_current_weather(lat: float = Query(...), lon: float = Query(...)):
    """
    Получает текущую погоду для заданных координат.

    - **lat**: Широта.
    - **lon**: Долгота.
    """
    weather = await fetch_weather(lat, lon)
    if weather:
        return weather
    raise HTTPException(status_code=500, detail="Не удалось получить данные о погоде.")


@app.post("/add_city")
async def add_city(user_id: int, name: str, lat: float, lon: float):
    """
    Добавляет новый город для пользователя.

    - **user_id**: ID пользователя.
    - **name**: Название города.
    - **lat**: Широта.
    - **lon**: Долгота.
    """
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()


    cursor.execute("SELECT id FROM users WHERE id = ?", (user_id,))
    if not cursor.fetchone():
        conn.close()
        raise HTTPException(status_code=404, detail="Пользователь не найден")

    cursor.execute("INSERT INTO cities (user_id, name, latitude, longitude, last_updated) VALUES (?, ?, ?, ?, ?)",
                   (user_id, name, lat, lon, None))
    conn.commit()
    conn.close()
    return {"message": f"Город {name} успешно добавлен для пользователя {user_id}."}



@app.get("/cities")
async def get_cities(user_id: int):
    """
    Получает список городов для указанного пользователя.

    - **user_id**: ID пользователя.
    """
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()

    cursor.execute("SELECT name, latitude, longitude FROM cities WHERE user_id = ?", (user_id,))
    rows = cursor.fetchall()
    conn.close()

    return [{"name": row[0], "latitude": row[1], "longitude": row[2]} for row in rows]



@app.get("/forecast")
async def get_forecast(user_id: int, city: str, time: str, parameters: List[str] = Query(...)):
    """
    Получает прогноз погоды для города в указанное время.

    - **user_id**: ID пользователя.
    - **city**: Название города.
    - **time**: Время, для которого нужен прогноз, в формате **HH:MM**.
    - **parameters**: Список параметров погоды (например, "temperature", "windspeed").
    """
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()

    cursor.execute("SELECT latitude, longitude, weather_data FROM cities WHERE user_id = ? AND name = ?",
                   (user_id, city))
    row = cursor.fetchone()
    conn.close()

    if not row:
        raise HTTPException(status_code=404, detail="Город не найден для этого пользователя.")

    lat, lon, weather_data = row
    if not weather_data:
        raise HTTPException(status_code=404, detail="Данные о погоде отсутствуют.")

    weather_data = json.loads(weather_data)


    try:
        target_time = datetime.strptime(time, "%H:%M").time()
    except ValueError:
        raise HTTPException(status_code=400, detail="Неверный формат времени. Используйте HH:MM.")

    hourly_times = weather_data.get("hourly", {}).get("time", [])
    closest_index = None
    min_diff = float("inf")

    for i, time_str in enumerate(hourly_times):
        data_time = datetime.fromisoformat(time_str).time()
        time_diff = abs((datetime.combine(datetime.today(), data_time) -
                         datetime.combine(datetime.today(), target_time)).total_seconds())

        if time_diff < min_diff:
            min_diff = time_diff
            closest_index = i

    if closest_index is None:
        raise HTTPException(status_code=404, detail="Не найдено данных о погоде для указанного времени.")


    result = {param: weather_data["hourly"].get(param, [None])[closest_index] for param in parameters}

    return result





async def update_weather_data():
    """
    Обновляет данные о погоде для всех городов.
    """
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()
    cursor.execute("SELECT id, latitude, longitude FROM cities")
    rows = cursor.fetchall()

    for city_id, lat, lon in rows:
        weather_data = await fetch_weather(lat, lon)
        if weather_data:
            cursor.execute(
                "UPDATE cities SET weather_data = ?, last_updated = ? WHERE id = ?",
                (json.dumps(weather_data), datetime.now(), city_id)
            )
    conn.commit()
    conn.close()





scheduler.add_job(update_weather_data, "interval", minutes=15)



@app.on_event("startup")
async def startup_event():
    scheduler.start()


@app.on_event("shutdown")
async def shutdown_event():
    scheduler.shutdown()


if __name__ == "__main__":
    uvicorn.run("script:app", host="127.0.0.1", port=8000)
