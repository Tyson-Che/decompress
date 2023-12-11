import zstandard
import pymongo
import json
import os
import sys
import logging.handlers

# Set up logging
log = logging.getLogger("bot")
log.setLevel(logging.DEBUG)
log.addHandler(logging.StreamHandler())

def read_and_decode(reader, chunk_size, max_window_size, previous_chunk=None, bytes_read=0):
	chunk = reader.read(chunk_size)
	bytes_read += chunk_size
	if previous_chunk is not None:
		chunk = previous_chunk + chunk
	try:
		return chunk.decode()
	except UnicodeDecodeError:
		if bytes_read > max_window_size:
			raise UnicodeError(f"Unable to decode frame after reading {bytes_read:,} bytes")
		log.info(f"Decoding error with {bytes_read:,} bytes, reading another chunk")
		return read_and_decode(reader, chunk_size, max_window_size, chunk, bytes_read)


def read_lines_zst(file_name):
	with open(file_name, 'rb') as file_handle:
		buffer = ''
		reader = zstandard.ZstdDecompressor(max_window_size=2**31).stream_reader(file_handle)
		while True:
			chunk = read_and_decode(reader, 2**27, (2**29) * 2)

			if not chunk:
				break
			lines = (buffer + chunk).split("\n")

			for line in lines[:-1]:
				yield line, file_handle.tell()

			buffer = lines[-1]

		reader.close()
def process_line(line):
    try:
        json_data = json.loads(line)
        subreddit = json_data.get('subreddit')
        if subreddit not in subreddits:
            return None
        json_data['_id'] = json_data.pop('id', None)
        return subreddit, json_data
    except json.JSONDecodeError as err:
        log.error(f"JSON decode error: {err}")
        return None

def insert_batch(batch_data, db):
    for subreddit, data in batch_data:
        if data:
            collection = db[subreddit]
            try:
                collection.insert_one(data)
            except Exception as e:
                log.error(f"Error inserting into database: {e}")

if __name__ == "__main__":
    file_path = sys.argv[1]
    file_size = os.stat(file_path).st_size
    file_lines = 0
    bad_lines = 0

    # Load subreddits into a set for faster lookups
    with open('subreddits.txt', 'r') as f:
        subreddits = set(line.strip() for line in f)

    client = pymongo.MongoClient('localhost', 27017)
    db = client['subreddits']

    try:
        batch_data = []
        for line, file_bytes_processed in read_lines_zst(file_path):
            processed = process_line(line)
            if processed:
                batch_data.append(processed)
                if len(batch_data) >= 1000:  # Adjust batch size as needed
                    insert_batch(batch_data, db)
                    batch_data = []
            
            file_lines += 1
            if file_lines % 100000 == 0:
                progress = (file_bytes_processed / file_size) * 100
                log.info(f"Processed {file_lines:,} lines. Bad lines: {bad_lines:,}. Progress: {progress:.2f}%")

        # Insert any remaining data
        if batch_data:
            insert_batch(batch_data, db)

    except Exception as e:
        log.error(f"An error occurred: {e}")
    finally:
        client.close()
        log.info(f"Complete: Processed {file_lines:,} lines with {bad_lines:,} bad lines.")
