# Data preprocessing for messanger

### How to use?

1. Install all dependencies from [requirements.txt](requirements.txt).
2. Run all commands listed in [additional_commands.txt](resources/additional_commands.txt).

All tasks are performed from [main.py](main.py) that accepts corresponding flags:

```bash
python main.py -h         
```

```
usage: main.py [-h] [--input_dir_path INPUT_DIR_PATH] [--output_dir_path OUTPUT_DIR_PATH] [--n_threds N_THREDS] [--prefix PREFIX] [--default_language DEFAULT_LANGUAGE] [--verbose VERBOSE]

Facebook data formatter. Drops photos, encodes all found users into unique ids, performs messages lemmatization and links and emoji encodings. Can Tokenize message contents.

options:
  -h, --help            show this help message and exit
  --input_dir_path INPUT_DIR_PATH
                        Directory holding folders containing facebook data in json format.
  --output_dir_path OUTPUT_DIR_PATH
                        Directory to which output should be written.
  --n_threds N_THREDS   Number of threads to be used for processing.
  --prefix PREFIX       Prefix added to each identifier.
  --default_language DEFAULT_LANGUAGE
                        If program will be unable to detect language it will use that.
  --verbose VERBOSE     Verbosity mode - 0 for None, 1 for logging without warnings, 2 for all.
```

Program produces following files in output directory:
- **conversation_prefix_title_id.jon** files - conversation files for each of conversations in messenger. Each file holds list of entries *(sender_id, words, timestamp)*, where words are already preprocessed yet not encoded.
- **prefix_users.json **- map how to get (user_id, gender) from user_name
- **prefix_titles.json** - map how to get title_id from title_name
- **prefix_users_reversed.json** - map how to get (user_name, gender) from user_id
- **prefix_titles_reversed.json** - map how to get title_name from title_id
- **prefix_conversations.json** - merged conversations into one file - a list of entries *(title_id, sender_id, words, timestamp*), where words are already preprocessed yet not encoded.
