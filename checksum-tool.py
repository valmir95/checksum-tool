#!/usr/bin/env python
import json
import os
import hashlib
import time
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed

checksum_list_file = "all-checksums.json"
checksum_list_status_file = "status-checksums.json"
max_chunk_size = 16 * 1024
ignored_dirs = ["$RECYCLE.BIN", ".git"]
ignored_file_names = [
    checksum_list_file,
    checksum_list_status_file,
    os.path.basename(__file__),
]


def is_dir_ignored(current_abs_path):
    return any(
        path_element in ignored_dirs for path_element in current_abs_path.split("\\")
    )


def in_dictlist(key, value, my_dictlist):
    return next((entry for entry in my_dictlist if entry[key] == value), False)


def delete_by_key_value(key, value, array):
    for i, obj in enumerate(array):
        if obj.get(key) == value:
            return array.pop(i)
    return None


def compute_hash(file_path):
    print(f"Computing hash for: {file_path}")
    file_hash = hashlib.blake2b()
    adaptive_chunk_size = max_chunk_size

    with open(file_path, "rb") as file:
        file_size = os.path.getsize(file_path)

        if file_size > (1 << 24):  # 16 MiB
            adaptive_chunk_size = 1 << 20  # 1 MiB
        elif file_size > (1 << 20):  # 1 MiB
            adaptive_chunk_size = 1 << 16  # 64 KiB

        for chunk in iter(lambda: file.read(adaptive_chunk_size), b""):
            file_hash.update(chunk)

    return file_hash.hexdigest()


def compare_checksums(root_dir):
    with open(checksum_list_file, "r") as f:
        stored_checksum_objs = json.load(f)
        stored_checksums = {
            obj["file_path"]: obj["checksum"] for obj in stored_checksum_objs
        }

    matched, changed, new, missing, relocated = [], [], [], [], []

    def process_file(file_path):
        current_checksum = compute_hash(file_path)
        stored_checksum = stored_checksums.get(file_path)
        if stored_checksum:
            result = {
                "file": file_path,
                "stored_checksum": stored_checksum,
                "current_checksum": current_checksum,
            }
            if current_checksum == stored_checksum:
                matched.append(result)
            else:
                changed.append(result)
        else:
            new.append({"file": file_path, "current_checksum": current_checksum})

    filtered_files = filter_files(root_dir)

    with ThreadPoolExecutor() as executor:
        futures = [
            executor.submit(process_file, file_path) for file_path in filtered_files
        ]
        for future in as_completed(futures):
            try:
                future.result()
            except Exception as exc:
                print(f"An exception occurred while processing a file: {exc}")

    for checksum_obj in stored_checksum_objs:
        file_path, checksum = checksum_obj["file_path"], checksum_obj["checksum"]
        if not in_dictlist("file", file_path, matched + changed + new):
            missing.append(
                {
                    "file": file_path,
                    "stored_checksum": checksum,
                    "current_checksum": checksum,
                }
            )

        new_entry, missing_entry = (
            in_dictlist("current_checksum", checksum, new),
            in_dictlist("current_checksum", checksum, missing),
        )

        if new_entry and missing_entry:
            """
            See if there is any reason to have a relocated section. For now it's disabled.
            relocated.append(
                {
                    "file": file_path,
                    "stored_checksum": checksum,
                    "current_checksum": checksum,
                    "new_location": new_entry["file"],
                }
            )
            """
            delete_by_key_value("file", new_entry["file"], new)
            delete_by_key_value("file", missing_entry["file"], missing)

    with open(checksum_list_status_file, "w") as f:
        json.dump({"changed": changed, "new": new, "missing": missing}, f)


def filter_files(root_dir):
    filtered_files = []

    for root, dirs, files in os.walk(root_dir):
        current_dir = os.path.basename(root)
        if is_dir_ignored(root):
            print(f"The directory: {current_dir} is ignored. Skipping...")
            continue
        for file in files:
            file_path = os.path.join(root, file)
            if file in ignored_file_names:
                print(f"File: {file} is skipped because hidden or ignored")
                continue
            filtered_files.append(file_path)

    return filtered_files


def compute_checksums(root_dir):
    checksums = []

    with ThreadPoolExecutor() as executor:
        future_to_file_path = {
            executor.submit(compute_hash, file_path): file_path
            for file_path in filter_files(root_dir)
        }

        for future in as_completed(future_to_file_path):
            file_path = future_to_file_path[future]
            try:
                checksum = future.result()
            except Exception as exc:
                print(f"{file_path} generated an exception: {exc}")
            else:
                checksums.append({"file_path": file_path, "checksum": checksum})

    with open(checksum_list_file, "w") as f:
        json.dump(checksums, f)


root_dir = os.getcwd()
if not os.path.exists(checksum_list_file):
    no_file_exist_input = input(
        "Checksum file does not exist, so it needs to be generated. Proceed? y/N: "
    )
    if no_file_exist_input.lower() != "y":
        print("Negative answer. Exiting.")
        sys.exit()
    print("Checksumming of all files is now starting...")
    start_time = time.perf_counter()
    compute_checksums(root_dir)
    end_time = time.perf_counter()
    elapsed_time = end_time - start_time
    print(f"Elapsed time: {elapsed_time:.6f} seconds for checksumming all files.")

else:
    valid_operations = ["generate", "compare"]
    operation_choice = ""
    while operation_choice.lower() not in valid_operations:
        operation_choice = input(
            "Generate new checksum file list, or compare to current checksum file list? generate/compare: "
        )
    if operation_choice == "generate":
        print("Checksumming of all files is now starting...")
        start_time = time.perf_counter()
        compute_checksums(root_dir)
        end_time = time.perf_counter()
        elapsed_time = end_time - start_time
        print(f"Elapsed time: {elapsed_time:.6f} seconds for checksumming all files.")
        sys.exit()
    print("Comparing checksums...")
    start_time = time.perf_counter()
    compare_checksums(root_dir)
    end_time = time.perf_counter()
    elapsed_time = end_time - start_time
    print(f"Elapsed time: {elapsed_time:.6f} seconds for checksumming all files.")
