# processing.py
import os
import re
import json
from datetime import datetime
import google.generativeai as genai
from google.generativeai.types import HarmCategory, HarmBlockThreshold
from dotenv import load_dotenv
from pathlib import Path
import math # For ceiling division

# --- Load .env file ---
dotenv_path = Path('.') / '.env'
print(f"DEBUG (processing.py): Checking for .env at {dotenv_path.resolve()}: {dotenv_path.is_file()}")
loaded = load_dotenv(dotenv_path=dotenv_path)
print(f"DEBUG (processing.py): load_dotenv executed: {loaded}")
# -------------------------------------------------------------

# --- Constants ---
TARGET_CHUNK_CHAR_SIZE = 25000 # Adjust as needed
CHUNK_OVERLAP_LINES = 10

# --- Configure Gemini Client ---
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
print(f"DEBUG (processing.py): Value read for GEMINI_API_KEY: {'Set' if GEMINI_API_KEY else 'Not Set'}")
if not GEMINI_API_KEY:
    print("WARNING: GEMINI_API_KEY not found in environment. Processing will fail.")
else:
    try:
        genai.configure(api_key=GEMINI_API_KEY)
        print("DEBUG: Gemini API configured.")
    except Exception as e:
        print(f"ERROR: Failed to configure Gemini API: {e}")

# --- Gemini Model Configuration ---
MODEL_NAME = "gemini-2.0-flash-exp-image-generation" # Or your preferred model
safety_settings = {
    HarmCategory.HARM_CATEGORY_HARASSMENT: HarmBlockThreshold.BLOCK_NONE,
    HarmCategory.HARM_CATEGORY_HATE_SPEECH: HarmBlockThreshold.BLOCK_NONE,
    HarmCategory.HARM_CATEGORY_SEXUALLY_EXPLICIT: HarmBlockThreshold.BLOCK_NONE,
    HarmCategory.HARM_CATEGORY_DANGEROUS_CONTENT: HarmBlockThreshold.BLOCK_NONE,
}
generation_config = {
  "temperature": 0.3,
  "top_p": 1,
  "top_k": 1,
  "max_output_tokens": 1000000, # Keep high for potentially large JSON responses per chunk
  "response_mime_type": "application/json",
}

# --- Helper Function for Chunking ---
def create_transcript_chunks(transcript_text, target_size=TARGET_CHUNK_CHAR_SIZE, overlap_lines=CHUNK_OVERLAP_LINES):
    """Splits transcript into chunks with overlap."""
    lines = transcript_text.splitlines()
    if not lines: return []
    chunks = []
    current_chunk_lines = []
    current_char_count = 0
    start_index = 0
    for i, line in enumerate(lines):
        line_len = len(line) + 1
        # If adding line exceeds target OR it's the last line, finalize chunk
        if (current_char_count + line_len > target_size and current_chunk_lines) or i == len(lines) - 1:
            if i == len(lines) - 1: current_chunk_lines.append(line) # Add last line
            chunk_text = "\n".join(current_chunk_lines)
            chunks.append(chunk_text)
            # Prepare overlap for next chunk
            if i < len(lines) - 1:
                overlap_start_index = max(start_index, i - overlap_lines + 1) # Ensure overlap has content
                start_index = overlap_start_index
                current_chunk_lines = lines[start_index : i + 1]
                current_char_count = sum(len(l) + 1 for l in current_chunk_lines)
            else:
                current_chunk_lines = [] # Last line was added
        else:
            current_chunk_lines.append(line)
            current_char_count += line_len
            if len(current_chunk_lines) == 1: start_index = i # Mark start of potential new chunk
    if current_chunk_lines: chunks.append("\n".join(current_chunk_lines)) # Add any remaining chunk
    return [chunk for chunk in chunks if chunk.strip()]


# --- Main Orchestration Function ---
def collate_questions_from_transcript(transcript_text):
    """
    Processes a raw transcript string using the Gemini API (with chunking)
    and returns a list of collated questions.
    """
    print("Starting transcript processing via Gemini API (Chunking Enabled)...")
    if not GEMINI_API_KEY: print("ERROR: Gemini API Key not configured."); return []
    if not transcript_text: print("WARN: Empty transcript text provided."); return []

    transcript_chunks = create_transcript_chunks(transcript_text)
    print(f"Split transcript into {len(transcript_chunks)} chunks.")
    if not transcript_chunks: return []

    all_collated_questions = []
    processed_chunk_count = 0

    for i, chunk in enumerate(transcript_chunks):
        print(f"\nProcessing Chunk {i+1}/{len(transcript_chunks)} (Length: {len(chunk)} chars)...")
        # --- Prepare the Prompt for the chunk---
        # This prompt should reflect the version that successfully extracted stem-only questions
        prompt = f"""
        Analyze the following WhatsApp conversation transcript chunk where medical students are recalling multiple-choice questions (MCQs) from a recent exam.
        This is chunk {i+1} of {len(transcript_chunks)}. Be aware context might be incomplete at chunk boundaries, but try your best.
        Your task is to extract ONLY the exam questions (stems) and their corresponding options found *within this specific chunk*.
        Ignore conversational filler, greetings, off-topic chat, system messages (like 'left', 'added', 'image omitted'), acknowledgments, and incomplete fragments that aren't clearly part of a question or its options IN THIS CHUNK.
        Associate options mentioned nearby or in reference to a specific question stem within this chunk.

        Output the result as a valid JSON array where each object represents a single distinct MCQ found in this chunk and has the following structure:
        {{
          "stem_text": "The full text of the question stem.",
          "options": ["Option A text", "Option B text", "Option C text", "Option D text"]
        }}
        If a question stem is clearly identified but no options are found for it in the conversation chunk, STILL include it in the JSON output, setting the "options" key to an empty array ([]). If no valid MCQs are found in this chunk, return an empty JSON array ([]).

        Transcript Chunk:
        ---
        {chunk}
        ---

        JSON Output:
        """ # <-- End of Prompt String

        try:
            print(f"DEBUG: Sending chunk {i+1} request to Gemini model '{MODEL_NAME}'...")
            model = genai.GenerativeModel(model_name=MODEL_NAME,
                                          generation_config=generation_config,
                                          safety_settings=safety_settings)
            response = model.generate_content(prompt)
            print(f"DEBUG: Received response for chunk {i+1}.")

            if response.parts:
                response_text = response.text
                try:
                    chunk_questions = json.loads(response_text)
                    if isinstance(chunk_questions, list):
                        valid_chunk_questions = []
                        item_counter = 0
                        for item in chunk_questions:
                            item_counter += 1
                            # Validation allows empty options list
                            if isinstance(item, dict) and "stem_text" in item and item.get("stem_text"): # Ensure stem not empty
                                options_list = item.get("options", []) # Default to [] if key missing
                                if options_list is None: options_list = [] # Treat null as empty
                                if isinstance(options_list, list):
                                    valid_chunk_questions.append({
                                        "stem_text": item["stem_text"],
                                        "options": options_list,
                                        "source_chunk": i+1
                                    })
                                else: print(f"WARN CHUNK {i+1} ITEM {item_counter}: Skipping item with invalid 'options' type --> {item}")
                            else: print(f"WARN CHUNK {i+1} ITEM {item_counter}: Skipping invalid item structure or empty stem --> {item}")
                        print(f"Chunk {i+1}: Extracted {len(valid_chunk_questions)} valid questions.")
                        all_collated_questions.extend(valid_chunk_questions)
                        processed_chunk_count += 1
                    else: print(f"ERROR: Chunk {i+1} response was not a JSON list as expected.")
                except json.JSONDecodeError as json_err: print(f"ERROR: Failed to decode JSON for chunk {i+1}: {json_err}")
                except Exception as parse_err: print(f"ERROR: Error parsing structure for chunk {i+1}: {parse_err}")
            else: print(f"WARN: Response for chunk {i+1} empty/blocked. Feedback: {response.prompt_feedback}")
        except Exception as e: print(f"ERROR: API call failed for chunk {i+1}: {e}"); import traceback; traceback.print_exc()

    # --- Combine and De-duplicate Results (Basic) ---
    final_questions = []
    seen_stems = set()
    print(f"\nCombining results from {processed_chunk_count} successfully processed chunks...")
    for q in all_collated_questions:
        norm_stem = q['stem_text'].lower().strip()
        if norm_stem not in seen_stems:
             final_questions.append({"stem_text": q['stem_text'], "options": q['options']})
             seen_stems.add(norm_stem)
        else: print(f"DEBUG: Duplicate stem found and skipped: {q['stem_text'][:50]}...")
    print(f"\nTotal processing done: {len(final_questions)} unique questions collated.")
    return final_questions

# --- Standalone Test Block ---
if __name__ == '__main__':
    # ... (Keep test block as is for verifying chunking/Gemini call) ...
    pass