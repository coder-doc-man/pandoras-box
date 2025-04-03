# component_4_refined_grouping.py
import json
import re
from datetime import datetime, timedelta
from collections import defaultdict
from thefuzz import fuzz # Using thefuzz library

# --- Constants for Matching Logic ---
# How many messages back to look for a potential stem for an orphan option block.
MESSAGE_SEARCH_WINDOW = 15
# How far back in time (minutes) to look. Use if timestamps are reliable.
TIME_SEARCH_WINDOW_MINUTES = 20
# Similarity score threshold (0-100) for fuzzy matching options to stems. Adjust based on testing.
SIMILARITY_THRESHOLD = 65 # Start relatively high to avoid wrong matches initially
# Max lines in a message to be considered an "option block" if no markers are present
MAX_LINES_FOR_HEURISTIC_OPTION_BLOCK = 6
# Max length of text for heuristic option block
MAX_LENGTH_FOR_HEURISTIC_OPTION_BLOCK = 200


# --- Option Identification Logic (similar to Component 3 but reusable) ---
option_marker_regex = re.compile(r"^\s*([a-d][\.\):]|[1-4][\.\):]|[-•*])\s+", re.IGNORECASE)
only_marker_regex = re.compile(r"^\s*([a-d][\.\):]?|[1-4][\.\):]?|[-•*])\s*$", re.IGNORECASE)
explicit_option_keywords = re.compile(r"^\s*(options were|options?|optns?)\b.*[:\-]?\s*", re.IGNORECASE)

def extract_options_from_text(text):
    """Extracts lines that look like options from a given text block."""
    options = []
    lines = text.splitlines()
    in_option_section = False # Flag if triggered by keywords like "options:"

    for line in lines:
        line_stripped = line.strip()
        if not line_stripped or only_marker_regex.match(line_stripped):
            continue

        # Check for explicit start keywords
        if explicit_option_keywords.match(line_stripped):
            in_option_section = True
             # Extract potential option text after the keyword phrase if on the same line
            option_text_on_keyword_line = explicit_option_keywords.sub('', line_stripped).strip()
            if option_text_on_keyword_line:
                options.append(option_text_on_keyword_line)
            continue # Move to next line after keyword line

        # If we are in an options section, or the line starts with a marker
        if in_option_section or option_marker_regex.match(line_stripped):
            options.append(line_stripped)
        # Heuristic: Add short lines immediately following a marked option
        elif options and (option_marker_regex.match(options[-1]) or len(options[-1]) < 80) and len(line_stripped) < 80:
             options.append(line_stripped)
        # Reset if we hit a clearly non-option line after options started by keyword
        elif in_option_section and len(line_stripped) > 100 and not option_marker_regex.match(line_stripped):
             in_option_section = False # Assume the options list ended


    # If no options found via markers/keywords, check simple structure
    # (e.g., multiple short lines) - less reliable
    if not options and len(lines) <= MAX_LINES_FOR_HEURISTIC_OPTION_BLOCK and len(text) < MAX_LENGTH_FOR_HEURISTIC_OPTION_BLOCK:
         non_empty_lines = [ln.strip() for ln in lines if ln.strip()]
         # Consider it options if most lines are shortish? Risky heuristic.
         short_line_count = sum(1 for ln in non_empty_lines if len(ln) < 80)
         if len(non_empty_lines) > 0 and short_line_count >= len(non_empty_lines) * 0.6:
             options.extend(non_empty_lines)

    return options


def refine_grouping_with_similarity(messages):
    """
    Groups stems and options using adjacency, timestamps, and text similarity.

    Args:
        messages (list): List of message dictionaries from preprocessing,
                         including 'id', 'timestamp', 'user', 'original_text',
                         'cleaned_text', 'is_potential_stem'.

    Returns:
        list: A list of dictionaries, where each represents a collated question
              with 'stem_message' and a consolidated list of 'options_text'.
    """
    if not messages or not isinstance(messages, list):
        return []

    # Store potential questions, keyed by the stem message ID for easy access
    potential_questions = {}
    # Keep track of which messages provided options that have been associated
    associated_option_message_ids = set()

    # --- Pass 1: Identify stems and options within the stem message ---
    for i, msg in enumerate(messages):
        if msg.get("is_potential_stem"):
            stem_id = msg['id']
            original_text = msg.get("original_text", "")
            lines = original_text.splitlines()
            stem_lines = []
            internal_options = []

            current_section = 'stem' # Can be 'stem' or 'option'

            for line in lines:
                line_stripped = line.strip()
                if not line_stripped or only_marker_regex.match(line_stripped):
                    continue

                is_option_marker = option_marker_regex.match(line_stripped)

                # If we see an option marker, switch to option mode
                if is_option_marker:
                    current_section = 'option'
                    internal_options.append(line_stripped)
                # If it's a short line following the main stem text (heuristic)
                elif current_section == 'stem' and len(stem_lines) > 0 and len(line_stripped) < 80 and not is_option_marker:
                     # Check context - if previous line was long, maybe this is first option?
                     if len(stem_lines[-1]) > 50 : # Heuristic threshold
                         current_section = 'option'
                         internal_options.append(line_stripped)
                     else: # likely continuation of stem
                         stem_lines.append(line_stripped)

                # If in option mode, add the line
                elif current_section == 'option':
                    internal_options.append(line_stripped)
                # Otherwise, it's part of the stem
                else:
                    stem_lines.append(line_stripped)


            stem_text_only = "\n".join(stem_lines).strip()
            # Add stem to our potential questions dictionary
            potential_questions[stem_id] = {
                "stem_message": msg,
                "stem_text_only": stem_text_only if stem_text_only else original_text.strip(), # Fallback if only options found
                "options": [] # Initialize options list
            }
            if internal_options:
                 potential_questions[stem_id]["options"].append({
                     "source_message_id": stem_id,
                     "option_texts": internal_options,
                     "method": "internal"
                 })
                 associated_option_message_ids.add(stem_id) # Mark stem msg as providing options

    # --- Pass 2: Identify external option blocks and associate them ---
    for i, msg in enumerate(messages):
        msg_id = msg['id']
        original_text = msg.get("original_text", "")

        # Skip if it's a stem or already processed as internal options
        if msg.get("is_potential_stem") or msg_id in associated_option_message_ids:
            continue

        # Check if this message looks like an option block
        options_in_msg = extract_options_from_text(original_text)

        if options_in_msg:
            associated = False
            # 1. Check immediate predecessor
            if i > 0:
                prev_msg_id = messages[i-1]['id']
                if prev_msg_id in potential_questions:
                    # Simple adjacency association
                    potential_questions[prev_msg_id]["options"].append({
                         "source_message_id": msg_id,
                         "option_texts": options_in_msg,
                         "method": "adjacent"
                    })
                    associated_option_message_ids.add(msg_id)
                    associated = True
                    # print(f"Associated options from msg {msg_id} to adjacent stem {prev_msg_id}")


            # 2. If not adjacent, try similarity and proximity matching (for orphan options)
            if not associated:
                best_match_stem_id = None
                highest_score = 0

                # Define search window (time and message count)
                min_message_id = max(0, msg_id - MESSAGE_SEARCH_WINDOW - 1)
                min_timestamp = None
                if msg.get("timestamp"):
                     try:
                         current_dt = datetime.fromisoformat(msg["timestamp"])
                         min_timestamp = current_dt - timedelta(minutes=TIME_SEARCH_WINDOW_MINUTES)
                     except (ValueError, TypeError):
                         pass # Ignore if timestamp is invalid


                # Candidate stems are those within the window *before* the current message
                candidate_stem_ids = [
                    stem_id for stem_id in potential_questions
                    if stem_id < msg_id and stem_id >= min_message_id
                ]

                # Filter further by timestamp if available
                if min_timestamp:
                    candidate_stem_ids = [
                        stem_id for stem_id in candidate_stem_ids
                        if potential_questions[stem_id]["stem_message"].get("timestamp")
                           and datetime.fromisoformat(potential_questions[stem_id]["stem_message"]["timestamp"]) >= min_timestamp
                    ]

                if candidate_stem_ids:
                    # Use the first few option lines for similarity matching text
                    options_preview = " ".join(options_in_msg[:3])[:200] # Limit length

                    for stem_id in candidate_stem_ids:
                        stem_text = potential_questions[stem_id]["stem_text_only"]
                        # Use token_set_ratio: ignores word order and duplicates, good for keywords
                        score = fuzz.token_set_ratio(options_preview, stem_text)

                        # Simple temporal decay (optional): reduce score slightly for older messages
                        time_diff_factor = 1.0 # No decay for now, can add later

                        final_score = score * time_diff_factor

                        if final_score > highest_score:
                             highest_score = final_score
                             best_match_stem_id = stem_id

                    # Associate if score exceeds threshold
                    if best_match_stem_id and highest_score >= SIMILARITY_THRESHOLD:
                         potential_questions[best_match_stem_id]["options"].append({
                             "source_message_id": msg_id,
                             "option_texts": options_in_msg,
                             "method": f"similarity ({highest_score:.0f})"
                         })
                         associated_option_message_ids.add(msg_id)
                         associated = True
                         # print(f"Associated options from msg {msg_id} to stem {best_match_stem_id} via similarity (score: {highest_score:.0f})")


    # --- Pass 3: Consolidate and Format Output ---
    final_collated_questions = []
    processed_stem_ids = set() # Track stems already added to avoid duplicates if structure changes

    # Sort potential_questions by stem ID to maintain original order
    sorted_stem_ids = sorted(potential_questions.keys())

    for stem_id in sorted_stem_ids:
        if stem_id in processed_stem_ids:
            continue

        q_data = potential_questions[stem_id]
        stem_message = q_data["stem_message"]
        stem_text = q_data["stem_text_only"]
        all_option_texts = []

        # Consolidate all options found for this stem
        for option_block in q_data["options"]:
            all_option_texts.extend(option_block["option_texts"])

        # Basic cleaning of option text: remove markers, strip whitespace
        cleaned_options = []
        for opt in all_option_texts:
             # Remove common markers like A., 1), -, *
             cleaned = option_marker_regex.sub('', opt).strip()
             # Remove just the marker if it was alone? (Handled earlier mostly)
             # cleaned = only_marker_regex.sub('', cleaned).strip() # Careful not to remove single letter options
             if cleaned: # Only add non-empty options
                cleaned_options.append(cleaned)

        # Simple de-duplication
        unique_options = []
        seen_options = set()
        for opt in cleaned_options:
             # Use lower case for case-insensitive check
             opt_lower = opt.lower()
             if opt_lower not in seen_options:
                 unique_options.append(opt)
                 seen_options.add(opt_lower)


        # Only include questions that have a stem AND at least one potential option found
        if stem_text and unique_options:
             final_collated_questions.append({
                 "stem_id": stem_id, # Keep ID for reference if needed
                 "stem_text": stem_text,
                 "options": unique_options,
                 # Optional: Add metadata about where options came from if useful for debugging
                 # "option_sources": q_data["options"]
             })
             processed_stem_ids.add(stem_id)


    return final_collated_questions


# --- Main execution part ---
if __name__ == "__main__":
    input_file = "stems_identified.json" # Input from Component 2
    output_file = "collated_questions_refined.json"

    try:
        with open(input_file, 'r', encoding='utf-8') as f:
            messages_with_stems = json.load(f)

        collated_questions = refine_grouping_with_similarity(messages_with_stems)

        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(collated_questions, f, indent=2, ensure_ascii=False)

        print(f"Refined and collated questions saved to {output_file}")
        print(f"Found {len(collated_questions)} questions with stems and options.")

        # Optional: Print some output for review
        # print("\n--- Sample Collated Questions ---")
        # for i, q in enumerate(collated_questions[:5]): # Print first 5
        #     print(f"\nQuestion {i+1} (Stem ID: {q['stem_id']})")
        #     print(f"  Stem: {q['stem_text']}")
        #     print(f"  Options ({len(q['options'])}):")
        #     for opt in q['options']:
        #          print(f"    - {opt}")
        # print("--------------------------------")


    except FileNotFoundError:
        print(f"Error: Input file '{input_file}' not found. Ensure it exists.")
    except ImportError:
        print("Error: 'thefuzz' library not found. Please install it using: pip install thefuzz python-Levenshtein")
    except json.JSONDecodeError:
        print(f"Error: Could not decode JSON from '{input_file}'. Ensure it's valid.")
    except Exception as e:
        print(f"An error occurred during refined grouping: {e}")
        import traceback
        traceback.print_exc() # Print detailed traceback for debugging