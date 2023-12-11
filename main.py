import json
import sys
import pymongo


# Load subreddits into a global list
with open('subreddits.txt', 'r') as f:
    subreddits = [line.strip() for line in f]

#@profile
def process_line(line):
    try:
        json_data = json.loads(line)

        # Check if subreddit is in the list
        subreddit = json_data.get('subreddit')
        if subreddit not in subreddits:
            return None

        # Modify 'id' to '_id'
        json_data['_id'] = json_data.pop('id', None)
        return subreddit, json_data
    except json.JSONDecodeError:
        return None



#@profile
def insert_batch(batch_data):
    if batch_data:
        # Create a new client for each batch within the process
        client = pymongo.MongoClient('mongodb+srv://writer:200809@fuckrd.nswnbxy.mongodb.net/')
        db = client['aca_sub_rd']
        try:
            for subreddit, data in batch_data:
                if data:  # Check if data is not None
                    collection = db[subreddit]
                    collection.insert_one(data)
        finally:
            client.close()  # Ensure the client is closed after the batch is processed
        except Exception as e:
            print(f"Failed to insert data: {e}")


#@profile
def process_chunk(chunk):
    processed_data = [process_line(line) for line in chunk if process_line(line) is not None]
    insert_batch(processed_data)
#@profile
def chunkify(file, chunk_size=1000):  # chunk_size is the number of lines
    chunk = []
    for line in file:
        chunk.append(line)
        if len(chunk) >= chunk_size:
            yield chunk
            chunk = []
    if chunk:  # yield the last chunk if it has any lines
        yield chunk


# The main entry point of your program
if __name__ == "__main__":

    try:
        if len(sys.argv) < 2:
            print("Usage: python3 main.py <file_path>")
            sys.exit(1)

        file_path = sys.argv[1]
        # Open the file and process it in chunks
        with open(file_path, 'r') as file:
            # Loop through chunks and process them asynchronously
            for chunk in chunkify(file):
                process_chunk(chunk)

    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        # No need to close the client here as each process initializes its own
        pass
