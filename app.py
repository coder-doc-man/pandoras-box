# app.py
import os
import io
from flask import Flask, render_template, jsonify, request, Response, send_file
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure
from bson import ObjectId
from dotenv import load_dotenv
import certifi
from datetime import datetime, time # Import time for combining date and time
import re
from processing import collate_questions_from_transcript
import openpyxl
from functools import wraps

# --- Load .env ---
load_dotenv()

# --- Configuration ---
MONGO_URI = os.getenv("MONGO_URI")
if not MONGO_URI: print("FATAL ERROR: MONGO_URI environment variable not set."); MONGO_URI = "mongodb://invalid_uri_placeholder"
DB_NAME = "mcq_data"
COLLECTION_NAME = "collated_questions"

# --- Configuration --- (Add near other config)
# Load Basic Auth credentials from environment variables
# Choose secure, unique username/password later in Render Env Vars
EXPECTED_USERNAME = os.getenv("BASIC_AUTH_USERNAME", "defaultuser")
EXPECTED_PASSWORD = os.getenv("BASIC_AUTH_PASSWORD", "defaultpass")

# --- Authentication Helper Functions ---
def check_auth(username, password):
    """This function is called to check if a username /
    password combination is valid."""
    return username == EXPECTED_USERNAME and password == EXPECTED_PASSWORD

def authenticate():
    """Sends a 401 response that enables basic auth"""
    return Response(
    'Could not verify your access level for that URL.\n'
    'You have to login with proper credentials', 401,
    {'WWW-Authenticate': 'Basic realm="Login Required"'})

def requires_auth(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        auth = request.authorization
        if not auth or not check_auth(auth.username, auth.password):
            # Only require auth if credentials are set in environment
            # Allows running locally without auth if vars are not set
            if EXPECTED_USERNAME != "defaultuser" or EXPECTED_PASSWORD != "defaultpass":
                 print(f"DEBUG: Auth failed for user '{auth.username if auth else 'None'}'") # Be careful logging usernames in real prod
                 return authenticate()
            # else:
                 # print("DEBUG: Skipping auth as default credentials are used (likely local dev)")
        # else:
            # print(f"DEBUG: Auth successful for user '{auth.username}'")
        return f(*args, **kwargs)
    return decorated

# --- Initialize Flask App ---
app = Flask(__name__)

# --- Database Connection ---
# ... (Database connection logic remains the same) ...
client = None; db = None; collection = None
try:
    if MONGO_URI and MONGO_URI != "mongodb://invalid_uri_placeholder":
        print("Attempting to connect to MongoDB Atlas with custom CA file...")
        ca = certifi.where(); print(f"DEBUG: Using CA file from certifi: {ca}")
        client = MongoClient(MONGO_URI, tlsCAFile=ca, serverSelectionTimeoutMS=5000)
        client.admin.command('ismaster'); db = client[DB_NAME]; collection = db[COLLECTION_NAME]
        print(f"Successfully connected to MongoDB. Database: '{DB_NAME}', Collection: '{COLLECTION_NAME}'")
    else: print("WARNING: MONGO_URI not set or invalid, skipping MongoDB connection.")
except ConnectionFailure as e: print(f"ERROR: Could not connect to MongoDB: {e}")
except Exception as e: print(f"An unexpected error occurred during MongoDB connection: {e}")


# --- Timestamp Parsing Logic (REUSABLE) ---
# Regex to find lines starting with potential WhatsApp timestamps
# Handles variations like [dd/mm/yy, HH:MM] - Name: Message or dd/mm/yy, HH:MM - Name: Message
# Or m/d/yy, H:MM AM/PM - Name...
# Increased robustness for formats found in exports
TIMESTAMP_LINE_REGEX = re.compile(
    # Optional brackets, date (d/m/y or m/d/y, 2 or 4 digit year), comma, space, time (H:M or H:M:S, optional AM/PM), optional brackets, space, dash, space
    r"^\s*(?:\[?(\d{1,2}/\d{1,2}/\d{2,4},?\s+\d{1,2}:\d{2}(?::\d{2})?(?:\s*[AP]M)?)]?\s*-\s*)",
    re.IGNORECASE # Ignore case for AM/PM
)

# List of formats to try for parsing dates robustly
DATE_FORMATS_TO_TRY = [
    # Common formats observed
    "%d/%m/%y, %H:%M",        # 01/01/24, 14:30
    "%d/%m/%Y, %H:%M",       # 01/01/2024, 14:30
    "%m/%d/%y, %I:%M %p",     # 1/1/24, 2:30 PM
    "%m/%d/%Y, %I:%M %p",    # 1/1/2024, 2:30 PM
    "%d/%m/%y, %H:%M:%S",     # 01/01/24, 14:30:55
    "%d/%m/%Y, %H:%M:%S",    # 01/01/2024, 14:30:55
    "%m/%d/%y, %I:%M:%S %p",  # 1/1/24, 2:30:55 PM
    "%m/%d/Y, %I:%M:%S %p", # 1/1/2024, 2:30:55 PM
    # Add more formats if needed based on real transcript examples
]

def parse_timestamp_from_string(ts_str):
    """Attempts to parse a timestamp string using multiple formats."""
    if not ts_str: return None
    # Basic cleaning, handle potential extra spaces or brackets if regex didn't capture cleanly
    ts_str_cleaned = ts_str.replace('[','').replace(']','').strip()
    for fmt in DATE_FORMATS_TO_TRY:
        try:
            # print(f"DEBUG: Trying format '{fmt}' on '{ts_str_cleaned}'") # Verbose debug
            return datetime.strptime(ts_str_cleaned, fmt)
        except ValueError:
            # print(f"DEBUG: Format '{fmt}' failed.") # Verbose debug
            continue
    # print(f"WARN: Could not parse timestamp string: '{ts_str}' (Cleaned: '{ts_str_cleaned}')") # Log if fails completely
    return None # Return None if no format matched

# --- Database Operations ---
# ... (save_questions_to_db, get_questions_from_db, get_questions_by_ids remain the same) ...
def save_questions_to_db(questions_list):
    """ Saves questions, deletes existing. """
    if collection is None: print("ERROR in save_questions_to_db: DB collection not initialized."); return False, "DB connection not established.", 0
    if not isinstance(questions_list, list): return False, "Invalid input: questions_list must be a list.", 0
    try:
        print(f"Clearing existing documents from collection '{COLLECTION_NAME}'..."); delete_result = collection.delete_many({})
        print(f"Deleted {delete_result.deleted_count} documents.")
        if not questions_list: print("Empty questions list, nothing to insert."); return True, "No questions found to save.", 0
        print(f"Inserting {len(questions_list)} new documents..."); timestamp_added = datetime.utcnow()
        # Add timestamp before inserting
        docs_to_insert = [{**q, 'processed_at': timestamp_added} for q in questions_list]
        insert_result = collection.insert_many(docs_to_insert)
        inserted_count = len(insert_result.inserted_ids)
        print(f"Successfully inserted {inserted_count} documents.")
        return True, f"Successfully saved {inserted_count} questions.", inserted_count
    except Exception as e: error_msg = f"Error saving questions to MongoDB: {e}"; print(error_msg); import traceback; traceback.print_exc(); return False, error_msg, 0

def get_questions_from_db(limit=500): # Increased limit slightly
    """ Retrieves questions from DB. """
    if collection is None: print("ERROR in get_questions_from_db: DB collection not initialized."); return []
    try:
        questions = list(collection.find().sort("processed_at", -1).limit(limit))
        for q in questions: q['_id'] = str(q['_id']) # Convert ObjectId for JSON
        print(f"Retrieved {len(questions)} questions from DB.")
        return questions
    except Exception as e: print(f"Error retrieving questions from MongoDB: {e}"); import traceback; traceback.print_exc(); return []

def get_questions_by_ids(ids_list):
    """ Retrieves specific questions by their _id list from DB. """
    if collection is None: print("ERROR in get_questions_by_ids: DB collection not initialized."); return []
    if not ids_list: return []
    try:
        # Convert string IDs back to ObjectId for MongoDB query
        object_ids = [ObjectId(id_str) for id_str in ids_list]
        # Fetch documents where _id is in the provided list
        questions = list(collection.find({"_id": {"$in": object_ids}}))
        # Convert _id back to string for consistency if needed later (though not for Excel)
        # for q in questions: q['_id'] = str(q['_id'])
        print(f"Retrieved {len(questions)} specific questions for export.")
        return questions
    except Exception as e: print(f"Error retrieving questions by ID list: {e}"); import traceback; traceback.print_exc(); return []


# --- API Routes ---

# --- NEW ANALYZE ROUTE ---
@app.route('/analyze', methods=['POST'])
@requires_auth
def analyze_transcript():
    """
    Analyzes the uploaded transcript (without saving) to find the
    earliest and latest timestamps present.
    Reads the file line by line for efficiency.
    """
    print("DEBUG: /analyze endpoint called.")
    if 'transcript' not in request.files:
        return jsonify({"success": False, "message": "No transcript file part."}), 400

    file = request.files['transcript']
    if file.filename == '':
        return jsonify({"success": False, "message": "No selected file."}), 400

    if file:
        min_date = None
        max_date = None
        line_count = 0
        timestamp_count = 0
        first_timestamp_line = -1
        last_timestamp_line = -1

        try:
            # Read line by line to avoid loading huge files into memory
            print(f"DEBUG: Analyzing file '{file.filename}' for date range...")
            # Use io.TextIOWrapper for robust decoding
            with io.TextIOWrapper(file, encoding='utf-8', errors='ignore') as text_file:
                for i, line in enumerate(text_file):
                    line_count += 1
                    if not line.strip(): continue # Skip empty lines faster

                    match = TIMESTAMP_LINE_REGEX.match(line) # Use the refactored regex
                    if match:
                        timestamp_str = match.group(1) # Get the captured timestamp part
                        parsed_dt = parse_timestamp_from_string(timestamp_str) # Use refactored parser
                        if parsed_dt:
                            timestamp_count += 1
                            if first_timestamp_line == -1: first_timestamp_line = line_count
                            last_timestamp_line = line_count

                            if min_date is None or parsed_dt < min_date:
                                min_date = parsed_dt
                            if max_date is None or parsed_dt > max_date:
                                max_date = parsed_dt
                        # else: # Optional: Log if a potential timestamp line failed parsing
                            # print(f"DEBUG: Line {line_count} matched regex but failed parse: {line[:100]}")


            # Ensure the file stream position is reset if it needs to be read again
            # Although in our flow, we re-upload, it's good practice if reusing the object was intended
            try:
                file.seek(0)
            except Exception as seek_err:
                print(f"WARN: Could not seek back on file stream: {seek_err}")


            print(f"DEBUG: Analysis complete. Lines: {line_count}, Timestamps found: {timestamp_count} (Lines {first_timestamp_line} to {last_timestamp_line}). MinDate: {min_date}, MaxDate: {max_date}")

            if min_date and max_date:
                # Return dates in ISO format (YYYY-MM-DD) for easy use with <input type="date">
                return jsonify({
                    "success": True,
                    "startDate": min_date.strftime('%Y-%m-%d'),
                    "endDate": max_date.strftime('%Y-%m-%d'),
                    "message": f"Found {timestamp_count} timestamps spanning from {min_date.strftime('%Y-%m-%d')} to {max_date.strftime('%Y-%m-%d')}.",
                    "timestampCount": timestamp_count
                }), 200
            elif timestamp_count > 0: # Handle cases where only one timestamp was found
                 single_date_dt = min_date or max_date
                 single_date_str = single_date_dt.strftime('%Y-%m-%d')
                 return jsonify({
                    "success": True,
                    "startDate": single_date_str,
                    "endDate": single_date_str,
                    "message": f"Found {timestamp_count} timestamp(s) only on {single_date_str}.",
                    "timestampCount": timestamp_count
                 }), 200
            else:
                # No valid timestamps found anywhere in the file
                return jsonify({"success": False, "message": "No valid timestamps found in the transcript. Cannot determine date range."}), 400

        except UnicodeDecodeError as ude:
             # This might catch error if the *first* chunk read fails decode
             print(f"ERROR: Failed initial decode of '{file.filename}'. Ensure UTF-8. Error: {ude}")
             return jsonify({"success": False, "message": "Error decoding file. Please ensure it's a UTF-8 encoded text file."}), 400
        except Exception as e:
            error_msg = f"An unexpected error occurred during analysis: {e}"
            print(f"ERROR: /analyze - {error_msg}"); import traceback; traceback.print_exc()
            return jsonify({"success": False, "message": error_msg}), 500
    else:
         # This case should ideally not be reached due to prior checks
         return jsonify({"success": False, "message": "File object not valid after initial checks."}), 400

@app.route('/upload', methods=['POST'])
@requires_auth
def upload_transcript():
    """ Handles transcript file upload, processing, and saving to DB.
        MODIFIED to accept optional date range for filtering. """
    if 'transcript' not in request.files:
        return jsonify({"success": False, "message": "No transcript file part."}), 400

    file = request.files['transcript']
    if file.filename == '':
        return jsonify({"success": False, "message": "No selected file."}), 400

    # --- Get optional date range from form data ---
    start_date_str = request.form.get('startDate') # Expected format 'YYYY-MM-DD'
    end_date_str = request.form.get('endDate')     # Expected format 'YYYY-MM-DD'
    print(f"DEBUG: /upload received startDate: {start_date_str}, endDate: {end_date_str}")

    start_dt = None
    end_dt = None
    filtering_active = False

    # --- Parse provided date strings ---
    if start_date_str and end_date_str:
        try:
            # Combine date with start/end of day for inclusive range
            start_dt = datetime.combine(datetime.strptime(start_date_str, '%Y-%m-%d').date(), time.min)
            end_dt = datetime.combine(datetime.strptime(end_date_str, '%Y-%m-%d').date(), time.max)
            if start_dt <= end_dt:
                filtering_active = True
                print(f"DEBUG: Filtering transcript between {start_dt} and {end_dt}")
            else:
                print("WARN: Invalid date range (start > end), ignoring filter.")
        except ValueError:
            print("WARN: Could not parse provided start/end dates, ignoring filter.")
            filtering_active = False # Ensure it's off if parsing fails

    if file:
        try:
            filtered_lines = []
            lines_processed = 0
            lines_included = 0
            # --- Read file line-by-line for filtering (memory efficient) ---
            # Use io.TextIOWrapper for robust decoding
            with io.TextIOWrapper(file, encoding='utf-8', errors='ignore') as text_file:
                for line in text_file:
                    lines_processed += 1
                    include_line = False
                    if not filtering_active:
                        include_line = True # Include all lines if not filtering
                    else:
                        match = TIMESTAMP_LINE_REGEX.match(line)
                        if match:
                            timestamp_str = match.group(1)
                            parsed_dt = parse_timestamp_from_string(timestamp_str)
                            if parsed_dt and start_dt <= parsed_dt <= end_dt:
                                include_line = True # Timestamp is within range
                            # Else: Line has timestamp but it's outside the range, exclude
                        else:
                            # Line does *not* start with a recognizable timestamp.
                            # Decision: Include these lines IF filtering is active.
                            # Rationale: These are likely continuation lines of messages
                            # within the desired date range, or system messages we might
                            # want Gemini to see for context (though Gemini should ignore).
                            # If we excluded them, we might break up relevant conversations.
                            include_line = True

                    if include_line:
                        filtered_lines.append(line)
                        lines_included += 1

            filtered_transcript_text = "".join(filtered_lines)
            total_chars = len(filtered_transcript_text)

            if filtering_active:
                print(f"DEBUG: Filtering complete. Processed {lines_processed} lines, included {lines_included} lines ({total_chars} chars) based on date range.")
            else:
                print(f"DEBUG: No date filtering applied. Processed {lines_included} lines ({total_chars} chars).")

            if not filtered_transcript_text.strip():
                 print("WARN: Transcript empty after filtering (or was originally empty).")
                 # Return success but indicate no questions saved
                 return jsonify({"success": True, "message": "Transcript is empty after applying date filter. No questions to process.", "questions_saved": 0}), 200

            # --- Call Gemini with the (potentially filtered) transcript ---
            print(f"DEBUG: Calling collate_questions_from_transcript with {total_chars} chars...")
            collated_questions = collate_questions_from_transcript(filtered_transcript_text)
            print(f"DEBUG: Collation returned {len(collated_questions)} questions.")

            # --- Save results to DB ---
            success, message, count = save_questions_to_db(collated_questions)
            if success:
                return jsonify({"success": True, "message": message, "questions_saved": count}), 200
            else:
                return jsonify({"success": False, "message": message}), 500

        except UnicodeDecodeError:
             # This might occur if the initial read/decode fails fundamentally
             print(f"ERROR: Failed decode '{file.filename}'. Ensure UTF-8.")
             return jsonify({"success": False, "message": "Error decoding file. Please ensure it's a UTF-8 encoded text file."}), 400
        except Exception as e:
            error_msg = f"Unexpected error during upload/processing: {e}"
            print(f"ERROR: /upload - {error_msg}"); import traceback; traceback.print_exc()
            return jsonify({"success": False, "message": error_msg}), 500
    else:
         return jsonify({"success": False, "message": "File object invalid."}), 400


@app.route('/questions', methods=['GET'])
@requires_auth
# ... (get_collated_questions remains the same) ...
def get_collated_questions():
    """ Retrieves collated questions from the database (No fuzzy matching). """
    print("DEBUG: /questions endpoint called.")
    final_data = {"success": False, "message": "An unknown error occurred", "questions": []}
    status_code = 500
    try:
        questions = get_questions_from_db() # Fetch directly
        if questions is None: raise Exception("Failed to retrieve questions from database.")
        # No grouping logic needed here anymore
        final_data = {"success": True, "questions": questions}
        status_code = 200
    except Exception as e:
        error_msg = f"An unexpected error occurred retrieving questions: {e}"
        print(f"ERROR: /questions - {error_msg}"); import traceback; traceback.print_exc()
        final_data = {"success": False, "message": error_msg, "questions": []}; status_code = 500
    return jsonify(final_data), status_code


@app.route('/export', methods=['POST'])
@requires_auth
# ... (export_questions remains the same) ...
def export_questions():
    """ Exports selected questions to an Excel (.xlsx) file. """
    print("DEBUG: /export endpoint called.")
    try:
        data = request.get_json()
        if not data or 'question_ids' not in data or not isinstance(data['question_ids'], list):
            return jsonify({"success": False, "message": "Invalid request. 'question_ids' list is required."}), 400

        question_ids = data['question_ids']
        if not question_ids:
            return jsonify({"success": False, "message": "No question IDs provided for export."}), 400

        print(f"DEBUG: Export requested for {len(question_ids)} IDs: {question_ids}")

        # Fetch the specific questions from DB
        questions_to_export = get_questions_by_ids(question_ids)

        if not questions_to_export:
            return jsonify({"success": False, "message": "Could not find questions for the provided IDs."}), 404

        # Create Excel workbook in memory
        workbook = openpyxl.Workbook()
        sheet = workbook.active
        sheet.title = "Collated MCQs"

        # Write header row
        sheet['A1'] = "Stem Text"
        sheet['B1'] = "Option 1"
        sheet['C1'] = "Option 2"
        sheet['D1'] = "Option 3"
        sheet['E1'] = "Option 4"
        # Add more option columns if needed, or handle dynamically
        # You could also add the DB _id in a column if useful

        # Write data rows
        row_num = 2
        for q in questions_to_export:
            sheet[f'A{row_num}'] = q.get('stem_text', '')
            options = q.get('options', [])
            if isinstance(options, list):
                for i, option in enumerate(options):
                    if i < 4: # Limit to 4 options for fixed columns B-E
                       col_letter = chr(ord('B') + i) # B, C, D, E
                       sheet[f'{col_letter}{row_num}'] = option
                    # Else: handle more than 4 options if needed (e.g., add more columns)
            row_num += 1

        # Save workbook to a byte stream
        excel_stream = io.BytesIO()
        workbook.save(excel_stream)
        excel_stream.seek(0) # Rewind the stream to the beginning

        print("DEBUG: Excel file generated in memory.")

        # Return the stream as a file download
        return send_file(
            excel_stream,
            as_attachment=True,
            download_name='collated_mcqs.xlsx',
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        )

    except Exception as e:
        error_msg = f"An unexpected error occurred during export: {e}"
        print(f"ERROR: /export - {error_msg}"); import traceback; traceback.print_exc()
        return jsonify({"success": False, "message": error_msg}), 500

# --- Basic Routes ---
@app.route('/')
@requires_auth
def index(): return render_template('index.html')
@app.route('/ping')
@requires_auth
def ping(): return jsonify({"message": "pong!"})
@app.route('/db_status')
@requires_auth
# ... (db_status remains the same) ...
def db_status():
    status_data = {}; status_code = 500
    try:
        if client is not None and db is not None and collection is not None:
            try: db.command('ping'); status_data = {"status": "connected", "db": DB_NAME, "collection": COLLECTION_NAME}; status_code = 200
            except Exception as e: error_msg = f"DB runtime check failed: {e}"; print(f"ERROR: /db_status - {error_msg}"); status_data = {"status": "error", "message": error_msg}
        else: error_msg = "Initial MongoDB connection failed."; print(f"DEBUG: /db_status - {error_msg}"); status_data = {"status": "disconnected", "message": error_msg}
    except Exception as e: critical_error_msg = f"Unexpected error in /db_status route: {e}"; print(f"CRITICAL ERROR: /db_status - {critical_error_msg}"); import traceback; traceback.print_exc(); status_data = {"status": "critical_error", "message": critical_error_msg}
    return jsonify(status_data), status_code

# --- Main execution block ---
#if __name__ == '__main__':
#    port = int(os.environ.get('PORT', 5001))
    # Ensure debug is False in production, but True is fine for local dev
#    app.run(host='0.0.0.0', port=port, debug=os.environ.get('FLASK_DEBUG', 'True').lower() == 'true')
