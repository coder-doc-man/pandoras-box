# component_1_preprocessing.py (Revised)
import re
import json
from datetime import datetime

def preprocess_transcript(transcript_text):
    """
    Cleans and structures the raw transcript text. Handles WhatsApp format.

    Args:
        transcript_text (str): The raw multiline string from the WhatsApp transcript file.

    Returns:
        list: A list of dictionaries, where each dictionary represents a message
              with 'id', 'timestamp_str', 'timestamp', 'user', 'original_text',
              'cleaned_text'. Returns an empty list if input is invalid.
    """
    if not transcript_text or not isinstance(transcript_text, str):
        print("Warning: Invalid input transcript_text. Returning empty list.")
        return []

    # Adjusted Regex for DD/MM/YY format and potentially international numbers/names
    # Makes timestamp/user capture non-greedy and optional
    # Handles potential LTR/RTL marks (\u200E|\u200F)
    line_regex = re.compile(
        r"^(?:\[?(\d{1,2}/\d{1,2}/\d{2,4},? \d{1,2}:\d{2}(?::\d{2})?(?: [AP]M)?)]? - )?" # Optional Timestamp
        r"((?:\u200E|\u200F)*?.*?(?:: ))?"  # Optional User (non-greedy), ends with ': '
        r"(.+)"  # Message Text
    , re.MULTILINE)


    messages = []
    current_message = None
    message_id_counter = 0

    for line in transcript_text.splitlines():
        line = line.strip()
        if not line:
            continue # Skip empty lines

        match = line_regex.match(line)

        if match:
            timestamp_str, user_part, text = match.groups()

            # If timestamp is detected, it's potentially a new message start
            if timestamp_str:
                 # Store the previous message if it exists
                if current_message:
                    messages.append(current_message)

                message_id_counter += 1
                user = user_part.replace(':', '').strip() if user_part else "Unknown"
                user = re.sub(r"[\u200E\u200F]", "", user) # Clean LTR/RTL marks from user

                # Attempt to parse timestamp (handle potential variations)
                parsed_timestamp = None
                formats_to_try = [
                    "%d/%m/%y, %H:%M", # Adjusted for YY format
                    "%d/%m/%Y, %H:%M",
                    "%m/%d/%y, %I:%M %p",
                    "%d/%m/%Y, %H:%M:%S",
                    "%m/%d/%Y, %H:%M:%S",
                    # Add more formats if needed
                ]
                for fmt in formats_to_try:
                    try:
                        ts_str_cleaned = timestamp_str.replace('[','').replace(']','')
                        parsed_timestamp = datetime.strptime(ts_str_cleaned.strip(), fmt)
                        break
                    except ValueError:
                        continue

                current_message = {
                    "id": message_id_counter,
                    "timestamp_str": timestamp_str,
                    "timestamp": parsed_timestamp.isoformat() if parsed_timestamp else None,
                    "user": user,
                    "original_text": text.strip(),
                    "cleaned_text": text.strip().lower() # Basic cleaning
                }
            # If no timestamp, it's likely a continuation or lacks standard formatting
            elif text:
                # Append to the text of the current message if it exists
                if current_message:
                    current_message["original_text"] += "\n" + text
                    current_message["cleaned_text"] += "\n" + text.lower()
                else:
                    # Handle case where the very first line(s) might lack timestamp/user
                    message_id_counter += 1
                    current_message = {
                        "id": message_id_counter,
                        "timestamp_str": None,
                        "timestamp": None,
                        "user": "Unknown",
                        "original_text": text.strip(),
                        "cleaned_text": text.strip().lower()
                    }
            # If match is None, it could be a system message or non-standard line
            # We'll try appending it to the current message if one exists
        elif current_message:
             # Ignore common system messages explicitly
             system_msg_patterns = [r"left$", r"added$", r"created group", r"changed the subject", r"changed this group's icon", r"‎image omitted", r"‎video omitted", r"‎sticker omitted", r"‎document omitted", r"‎audio omitted"]
             is_system_message = any(re.search(p, line, re.IGNORECASE) for p in system_msg_patterns)

             if not is_system_message:
                current_message["original_text"] += "\n" + line
                current_message["cleaned_text"] += "\n" + line.lower()
             else:
                 # Optionally, create a specific message type for system messages if needed later
                 # For now, we just ignore them for MCQ extraction
                 pass
        # If it doesn't match and there's no current message, we likely skip it (could be initial system messages)

    # Add the last message accumulated
    if current_message:
        messages.append(current_message)

    return messages

# --- Main execution part (assuming the snippet is in 'sample_transcript.txt') ---
if __name__ == "__main__":
    transcript_file = "sample_transcript.txt"
    try:
        # IMPORTANT: Ensure your 'sample_transcript.txt' contains the exact snippet you provided.
        with open(transcript_file, 'r', encoding='utf-8') as f:
            raw_text = f.read()

        processed_messages = preprocess_transcript(raw_text)

        output_file = "processed_messages.json"
        with open(output_file, 'w', encoding='utf-8') as f:
             json.dump(processed_messages, f, indent=2, ensure_ascii=False) # ensure_ascii=False for phone numbers/names
        print(f"Processed messages saved to {output_file}")
        # Optional: Print to console for quick check
        # print(json.dumps(processed_messages, indent=2, ensure_ascii=False))


    except FileNotFoundError:
        print(f"Error: Transcript file '{transcript_file}' not found.")
    except Exception as e:
        print(f"An error occurred during preprocessing: {e}")