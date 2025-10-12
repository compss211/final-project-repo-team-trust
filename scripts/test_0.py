import json
#change the path to your own path
#get the data from using the torrent
input_path = "C:/Users/16082/Desktop/class_project/PoliticalDiscussion_comments"
output_path = "C:/Users/16082/Desktop/class_project/PoliticalDiscussion_cleaned.jsonl"

# thing we want to keep
keep_fields = [
    "subreddit",
    "created_utc",
    "link_id",
    "score",
    "id",
    "downs",
    "controversiality",
    "ups",
    "body",
]

count_in, count_out = 0, 0

with open(input_path, "r", encoding="utf-8") as fin, open(output_path, "w", encoding="utf-8") as fout:
    for line in fin:
        count_in += 1
        try:
            data = json.loads(line)
            body = data.get("body", "")
            # skip deleted or removed comments, remove later
            if body in ("[deleted]", "[removed]", None, ""):
                continue
            # keep only the fields we want
            clean_data = {k: data.get(k, None) for k in keep_fields}

            fout.write(json.dumps(clean_data, ensure_ascii=False) + "\n")
            count_out += 1

        except json.JSONDecodeError:
            continue  # skip error lines

print(f"input {count_in:,} lines，output{count_out:,} lines")
print("Try it out!")
