import subprocess
import logging
import re
import json

# Constants
START_INDEX = 391
END_INDEX = 434
TORRENT_FILE = "89d24ff9d5fbc1efcdaf9d7689d72b7548f699fc.torrent"
STATE_FILE = "state.json"

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def run_bash_script(script, args):
    result = subprocess.run([script] + args, capture_output=True, text=True)
    return result.returncode == 0, result.stdout, result.stderr


def download_file(index):
    success, stdout, _ = run_bash_script("./download.sh", [str(index), TORRENT_FILE])
    if success:
        # Extracting the part of the output after 'Download Results:'
        results_section = stdout.split('Download Results:')[1] if 'Download Results:' in stdout else ''

        # Finding the file path after the last '|' character
        match = re.search(r'\|\s*(/.*?\.zst)\s*$', results_section, re.MULTILINE)
        if match:
            return True, match.group(1)
    return False, None


def check_integrity(file_path):
    return run_bash_script("./check_integrity.sh", [file_path])[0]

def decompress_file(file_path):
    success, _, _ = run_bash_script("./decompress.sh", [file_path])
    return success

def process_file(file_path):
    return run_bash_script("python3", ["main.py", file_path])[0]

def cleanup(download_path):
    run_bash_script("./cleanup.sh", [download_path])

def load_state():
    try:
        with open(STATE_FILE, "r") as file:
            return json.load(file)
    except FileNotFoundError:
        return {}

def save_state(state):
    with open(STATE_FILE, "w") as file:
        json.dump(state, file)

def main():
    state = load_state()
    for index in range(START_INDEX, END_INDEX + 1):
        if state.get(str(index)) == "completed":
            logging.info(f"Index {index} already processed. Skipping.")
            continue

        success, download_path = download_file(index)
        if not success:
            logging.error(f"Download failed for index {index}.")
            break

        if not check_integrity(download_path):
            logging.error(f"Integrity check failed for file {download_path}.")
            break

        if not decompress_file(download_path):
            logging.error(f"Decompression failed for file {download_path}.")
            break

        decompressed_file = download_path.rsplit('.zst', 1)[0]
        if not process_file(decompressed_file):
            logging.error(f"Processing failed for file {decompressed_file}.")
            break

        cleanup(download_path, decompressed_file)
        state[str(index)] = "completed"
        save_state(state)

if __name__ == "__main__":
    main()
