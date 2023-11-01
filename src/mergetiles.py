#!/usr/bin/env python
from PIL import Image
import os


def get_immediate_subdirectories(dir):
    return [name for name in os.listdir(dir) if os.path.isdir(os.path.join(dir, name))]


def get_immediate_files(dir):
    return [name for name in os.listdir(dir) if os.path.isfile(os.path.join(dir, name))]


base_dir = "./output/1698402923435/17/"
destination_file = "perm.png"

base_directory_content = get_immediate_subdirectories(base_dir)
horizontal_tiles_count = len(base_directory_content)

assert horizontal_tiles_count, "Error! Base directory is empty."

first_directory_content = get_immediate_files(base_dir + "/" + base_directory_content[0] + "/")
vertical_tiles_count = len(first_directory_content)

assert horizontal_tiles_count, "Error! First tile directory is empty. Please check tile files."

tile_name, tile_extension = os.path.splitext(
    base_dir + "/" + base_directory_content[0] + "/" + first_directory_content[0]
)
first_tile = Image.open(tile_name + tile_extension)
tile_size = first_tile.size[0]

width = tile_size * vertical_tiles_count
height = tile_size * horizontal_tiles_count

if width * height > 2**29:
    height = int(2**29 / width)

print(f"Creating image {width=} {height=}...")
Image.MAX_IMAGE_PIXELS = None
image = Image.new("RGB", (width, height))
print("Image created! Adding tiles...")

horizontal_index = 0
for dir in sorted(base_directory_content, key=lambda x: int(x)):
    x = tile_size * horizontal_index
    vertical_index = 0
    for file in sorted(get_immediate_files(base_dir + dir), key=lambda x: int(x.split(".")[0])):
        print(f"x: {vertical_index}/{vertical_tiles_count}\ty: {horizontal_index}/{horizontal_tiles_count}")
        tile_path = base_dir + dir + "/" + file
        tile = Image.open(tile_path)
        y = tile_size * vertical_index
        image.paste(tile, (x, y))
        vertical_index += 1
    horizontal_index += 1

print(f"Writing {destination_file}...")
image.save(destination_file, "PNG")
print("Writen successfully!")
