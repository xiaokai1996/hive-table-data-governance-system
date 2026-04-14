import argparse
import os
import subprocess
import sys
from datetime import datetime, timezone

SCRIPTS_DIR = os.path.dirname(os.path.abspath(__file__))


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--request-id", default=None)
    parser.add_argument("--owner", default="demo_user")
    return parser.parse_args()


def default_request_id():
    return datetime.now(timezone.utc).strftime("req_%Y%m%d%H%M%S")


def run_script(script_name, script_args):
    script_path = os.path.join(SCRIPTS_DIR, script_name)
    print(f"Starting: {script_name}")

    try:
        result = subprocess.run(
            [sys.executable, script_path, *script_args],
            check=True,
            capture_output=True,
            text=True,
        )
        if result.stdout:
            print(result.stdout.strip())
        print(f"Finished: {script_name}")
        return True
    except subprocess.CalledProcessError as error:
        print(f"Failed: {script_name}")
        if error.stdout:
            print(error.stdout)
        if error.stderr:
            print(error.stderr)
        return False


def main():
    args = parse_args()
    request_id = args.request_id or default_request_id()
    pipeline = [
        ("step0_ingest_request.py", ["--request-id", request_id, "--owner", args.owner]),
        ("step1_render.py", ["--request-id", request_id]),
        ("step2_ocr.py", ["--request-id", request_id]),
        ("step3_aggregate.py", ["--request-id", request_id]),
        ("step4_validation.py", ["--request-id", request_id]),
    ]

    print(f"Pipeline started for request_id={request_id}")
    for script_name, script_args in pipeline:
        if not run_script(script_name, script_args):
            print(f"Pipeline stopped at {script_name}")
            sys.exit(1)
    print(f"Pipeline finished for request_id={request_id}")


if __name__ == "__main__":
    main()
