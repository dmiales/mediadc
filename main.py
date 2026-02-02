# КРИТИЧЕСКОЕ ЛОГИРОВАНИЕ В САМОМ НАЧАЛЕ - ДО ВСЕХ ИМПОРТОВ
import sys
import os

# СНАЧАЛА пишем в stderr (это всегда работает)
sys.stderr.write("=" * 80 + "\n")
sys.stderr.write("[TRACE] PYTHON main.py SCRIPT STARTED\n")
sys.stderr.write(f"[TRACE] PYTHON args: {sys.argv}\n")
sys.stderr.write(f"[TRACE] PYTHON Python version: {sys.version}\n")
try:
    sys.stderr.write(f"[TRACE] PYTHON working directory: {os.getcwd()}\n")
except:
    sys.stderr.write("[TRACE] PYTHON cannot get working directory\n")
sys.stderr.write(f"[TRACE] PYTHON sys.path: {sys.path}\n")
sys.stderr.flush()

# Затем пробуем записать в файл
try:
    with open('/tmp/mediadc_trace.log', 'a') as f:
        f.write(f"[TRACE] PYTHON main.py SCRIPT STARTED with args: {sys.argv}\n")
        f.write(f"[TRACE] PYTHON main.py working directory: {os.getcwd()}\n")
        f.write(f"[TRACE] PYTHON main.py Python version: {sys.version}\n")
        f.flush()  # Принудительно записываем
    sys.stderr.write("[TRACE] PYTHON Successfully wrote to /tmp/mediadc_trace.log\n")
except Exception as e:
    # Если даже запись в файл не работает, пробуем stderr
    sys.stderr.write(f"[TRACE] PYTHON ERROR writing to log file: {e}\n")
sys.stderr.flush()

import argparse

# Импорты с обработкой ошибок
sys.stderr.write("[TRACE] PYTHON main.py importing nc_py_api\n")
sys.stderr.flush()
try:
    with open('/tmp/mediadc_trace.log', 'a') as f:
        f.write("[TRACE] PYTHON main.py importing nc_py_api\n")
        f.flush()
    from nc_py_api import CONFIG
    sys.stderr.write("[TRACE] PYTHON Successfully imported nc_py_api\n")
except Exception as e:
    sys.stderr.write(f"[TRACE] PYTHON ERROR importing nc_py_api: {type(e).__name__}: {e}\n")
    import traceback
    sys.stderr.write(f"[TRACE] PYTHON TRACEBACK:\n{traceback.format_exc()}\n")
    try:
        with open('/tmp/mediadc_trace.log', 'a') as f:
            f.write(f"[TRACE] PYTHON ERROR importing nc_py_api: {e}\n")
            f.flush()
    except:
        pass
    sys.stderr.flush()
    raise
sys.stderr.flush()

sys.stderr.write("[TRACE] PYTHON main.py importing numpy, PIL\n")
sys.stderr.flush()
try:
    with open('/tmp/mediadc_trace.log', 'a') as f:
        f.write("[TRACE] PYTHON main.py importing numpy, PIL\n")
        f.flush()
    from numpy import count_nonzero
    from PIL import Image, ImageOps
    sys.stderr.write("[TRACE] PYTHON Successfully imported numpy, PIL\n")
except Exception as e:
    sys.stderr.write(f"[TRACE] PYTHON ERROR importing numpy/PIL: {type(e).__name__}: {e}\n")
    import traceback
    sys.stderr.write(f"[TRACE] PYTHON TRACEBACK:\n{traceback.format_exc()}\n")
    try:
        with open('/tmp/mediadc_trace.log', 'a') as f:
            f.write(f"[TRACE] PYTHON ERROR importing numpy/PIL: {e}\n")
            f.flush()
    except:
        pass
    sys.stderr.flush()
    raise
sys.stderr.flush()

sys.stderr.write("[TRACE] PYTHON main.py importing python modules\n")
sys.stderr.flush()
try:
    with open('/tmp/mediadc_trace.log', 'a') as f:
        f.write("[TRACE] PYTHON main.py importing python modules\n")
        f.flush()
    from python.bundle_info import bundle_info
    from python.db_requests import get_tasks
    from python.images import pil_to_hash
    from python.log import logger as log
    from python.task import process_task
    sys.stderr.write("[TRACE] PYTHON Successfully imported all python modules\n")
except Exception as e:
    sys.stderr.write(f"[TRACE] PYTHON ERROR importing python modules: {type(e).__name__}: {e}\n")
    import traceback
    sys.stderr.write(f"[TRACE] PYTHON TRACEBACK:\n{traceback.format_exc()}\n")
    try:
        with open('/tmp/mediadc_trace.log', 'a') as f:
            f.write(f"[TRACE] PYTHON ERROR importing python modules: {e}\n")
            f.flush()
    except:
        pass
    sys.stderr.flush()
    raise
sys.stderr.flush()

if __name__ == "__main__":
    # Логируем запуск в файл для трассировки
    try:
        with open('/tmp/mediadc_trace.log', 'a') as f:
            f.write(f"[TRACE] PYTHON main.py reached __main__ block\n")
            f.flush()
    except:
        pass
    
    log.error("aaaaaaa1aa2")

    with open('/tmp/mediadc_trace.log', 'a') as f:
        f.write(f"[TRACE] PYTHON main.py started with args: {sys.argv}\n")
        f.flush()

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
    try:
        args = parser.parse_args()
    except Exception as e:
        with open('/tmp/mediadc_trace.log', 'a') as f:
            f.write(f"[TRACE] PYTHON ERROR in argparse: {e}\n")
            f.flush()
        sys.exit(1)

    try:
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
    except Exception as e:
        with open('/tmp/mediadc_trace.log', 'a') as f:
            f.write(f"[TRACE] PYTHON ERROR in main block: {type(e).__name__}: {e}\n")
            import traceback
            f.write(f"[TRACE] PYTHON TRACEBACK:\n{traceback.format_exc()}\n")
            f.flush()
        sys.stderr.write(f"[TRACE] PYTHON FATAL ERROR: {e}\n")
        sys.stderr.flush()
        sys.exit(1)
    
    try:
        log.info("[TRACE] main.py exiting")
    except:
        pass
    sys.exit(0)
