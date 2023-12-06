#!/usr/bin/env python
import os
import json
from pathlib import Path
import math
from datetime import datetime, timedelta
from multiprocessing import Pool, cpu_count
import requests
import cv2
import numpy as np
from decimal import Decimal


with open("google_api_keys.txt") as file:
    GOOGLE_API_KEYS = set([line.strip() for line in file.read().split("\n") if line.strip().startswith("AIza")])


def google_api_key_feeder():
    for api_key in GOOGLE_API_KEYS:
        yield api_key
    yield

new_api_key = google_api_key_feeder()


# TILES_PATH = Path(os.getenv("TILES_PATH", "tiles"))
TILES_PATH = Path(os.getenv("TILES_PATH", "/mnt/e/tiles"))
TILE_FILE_PATTERN = "{file_format}_{tile_width}_{tile_height}/{zoom}/{x}/{y}.{file_format}"

OUTPUT_PATH = Path(os.getenv("TILES_PATH", "output"))
OUTPUT_FILE_PATTERN = "{title}_{zoom}.{file_format}"

TASKS_PATH = Path(os.getenv("TASKS_PATH", "tasks.json"))

DEFAULT_TASK = {
    "a": "",
    "b": "",
    "zoom": 15,
    "map_type": "satellite",
    "title": "",
    "status": "NEW",
    "session": None,
    "expiry": None,
    "tile_width": None,
    "tile_height": None,
    "image_format": None,
}

tommorow = datetime.now() + timedelta(days=1)


def create_tile_session(task):
    print("create session...")
    URL = f"https://tile.googleapis.com/v1/createSession?key={GOOGLE_API_KEY}"
    response = requests.post(URL, json={"mapType": task["map_type"], "language": "ru-RU", "region": "RU"})

    assert response.status_code == 200, f"{response.text}"

    data = response.json()

    task["session"] = data["session"]
    task["expiry"] = int(data["expiry"])
    task["tile_width"] = data["tileWidth"]
    task["tile_height"] = data["tileHeight"]
    task["image_format"] = data["imageFormat"].lower()
    return task


def write_tasks(tasks):
    with open(TASKS_PATH, "w") as file:
        file.write(json.dumps(tasks, indent=2, ensure_ascii=False))


def get_tasks():
    with open(TASKS_PATH) as file:
        tasks = json.loads(file.read())

    prepared_tasks = []

    for task in tasks:
        task = dict(DEFAULT_TASK, **task)

        if not task["session"] or datetime.fromtimestamp(task["expiry"]) < tommorow:
            task = create_tile_session(task)

        def lat_lng_to_tile_coord(lat_lng: str):
            lat, lng = [Decimal(degree.strip()) for degree in lat_lng.split(",")]

            mercator = Decimal(-math.log(math.tan((Decimal("0.25") + lat / Decimal("360")) * Decimal(math.pi))))

            point_x = task["tile_width"] * (lng / Decimal("360") + Decimal("0.5"))
            point_y = task["tile_height"] / Decimal("2") * (Decimal("1") + mercator / Decimal(math.pi))

            scale = Decimal(math.pow(Decimal("2"), Decimal(str(task["zoom"]))))

            x = math.floor(point_x * scale / task["tile_width"])
            y = math.floor(point_y * scale / task["tile_height"])

            return f"{x},{y}"

        if "." in task["a"]:
            task["a"] = lat_lng_to_tile_coord(task["a"])
        if "." in task["b"]:
            task["b"] = lat_lng_to_tile_coord(task["b"])

        prepared_tasks.append(task)

    write_tasks(prepared_tasks)
    return prepared_tasks


def multi_download_tile_wrapper(args):
    return download_tile(*args)


def download_tile(x, y, task):
    URL = (
        f"https://tile.googleapis.com/v1/2dtiles/{task['zoom']}/{x}/{y}?session={task['session']}&key={GOOGLE_API_KEY}"
    )

    tile_file_path = TILES_PATH / TILE_FILE_PATTERN.format(
        file_format=task["image_format"],
        tile_width=task["tile_width"],
        tile_height=task["tile_height"],
        zoom=task["zoom"],
        x=x,
        y=y,
    )

    tile_file_path.parent.mkdir(parents=True, exist_ok=True)

    if not tile_file_path.exists() or not tile_file_path.stat().st_size:
        response = requests.get(URL)

        assert response.status_code == 200, f"{response.text}"

        with open(tile_file_path, "wb") as f:
            f.write(response.content)
    return tile_file_path


def process_task(task):
    if task["status"] == "DONE":
        return task

    x1, y1 = [int(v) for v in task["a"].split(",")]
    x2, y2 = [int(v) for v in task["b"].split(",")]

    min_x = min(x1, x2)
    min_y = min(y1, y2)
    max_x = max(x1, x2)
    max_y = max(y1, y2)

    x_range = [x for x in range(min_x, max_x + 1)]
    y_range = [y for y in range(min_y, max_y + 1)]

    h_stack = []

    for h_index, x in enumerate(x_range, start=1):
        with Pool(cpu_count()) as p:
            v_stack = p.map(multi_download_tile_wrapper, [(x, y, task) for y in y_range])

        np_vstack = np.vstack([cv2.imread(str(tile_file_path)) for tile_file_path in v_stack])

        percent = h_index / (len(x_range) / Decimal(100))
        print(f"Processed rows: {h_index}/{len(x_range)}\t({percent:<5.2f} %)", end="\r")

        h_stack.append(np_vstack)

    collage = np.hstack(h_stack)

    output_file_path = OUTPUT_PATH / OUTPUT_FILE_PATTERN.format(
        title=task["title"] or "output",
        zoom=task["zoom"],
        file_format=task["image_format"],
    )
    output_file_path.parent.mkdir(parents=True, exist_ok=True)

    print(f"Writing {output_file_path}...")
    cv2.imwrite(str(output_file_path), collage)
    print("Success!")

    task["status"] = "DONE"
    return task


if __name__ == "__main__":
    tasks = get_tasks()
    for index, task in enumerate(tasks):
        tasks[index] = process_task(task)
        write_tasks(tasks)
