import argparse
import sys

from nc_py_api import CONFIG
from numpy import count_nonzero
from PIL import Image, ImageOps

from python.bundle_info import bundle_info
from python.db_requests import get_tasks
from python.images import pil_to_hash
from python.log import logger as log
from python.task import process_task

if __name__ == "__main__":
    # Логируем запуск в файл для трассировки
    with open('/tmp/mediadc_trace.log', 'a') as f:
        f.write(f"[TRACE] PYTHON main.py started with args: {sys.argv}\n")

    log.info("[TRACE] main.py started with args: %s", sys.argv)
    parser = argparse.ArgumentParser(description="Module for performing objects operations.", add_help=True)
    group = parser.add_mutually_exclusive_group()
    group.add_argument(
        "-t",
        dest="mdc_tasks_id",
        type=int,
        action="append",
        help="Process MediaDC task with specified ID. Can be specified multiply times.",
    )
    group.add_argument(
        "--info", dest="bundle_info", action="store_true", help="Print information about bundled packages."
    )
    group.add_argument(
        "--test", dest="test", type=str, action="append", help="Performs a comparison of two files. Specify twice."
    )
    args = parser.parse_args()

    if args.bundle_info:
        log.info("[TRACE] main.py executing bundle_info")
        bundle_info()
    elif args.mdc_tasks_id:
        log.info("[TRACE] main.py processing tasks: %s", args.mdc_tasks_id)
        if not CONFIG["valid"]:
            log.error("Unable to parse config or connect to database. Does `occ` works?")
            sys.exit(1)
        tasks_to_process = get_tasks()
        log.info("[TRACE] main.py found %d total tasks in database", len(tasks_to_process))
        tasks_to_process = list(filter(lambda row: row["id"] in args.mdc_tasks_id, tasks_to_process))
        missing_tasks = list(filter(lambda r: not any(row["id"] == r for row in tasks_to_process), args.mdc_tasks_id))
        for x in missing_tasks:
            log.warning("Cant find task with id=%u", x)
        log.info("[TRACE] main.py will process %d tasks", len(tasks_to_process))
        for i in tasks_to_process:
            log.info("[TRACE] main.py starting task id=%u", i["id"])
            process_task(i)
            log.info("[TRACE] main.py completed task id=%u", i["id"])
    elif args.test:
        log.info("[TRACE] main.py executing test comparison")
        for algo in ("phash", "dhash", "whash", "average"):
            img_hashes = [
                pil_to_hash(algo, 16, ImageOps.exif_transpose(Image.open(args.test[0]))).flatten(),
                pil_to_hash(algo, 16, ImageOps.exif_transpose(Image.open(args.test[1]))).flatten(),
            ]
            print(f"hamming distance({algo}): {count_nonzero(img_hashes[0] != img_hashes[1])}")
    else:
        log.info("[TRACE] main.py showing help")
        parser.print_help()
    log.info("[TRACE] main.py exiting")
    sys.exit(0)
