# component_2_stem_identification.py
import json
import re

def identify_potential_stems(messages):
    """
    Identifies potential question stems in a list of processed messages.

    Args:
        messages (list): A list of message dictionaries from preprocessing.

    Returns:
        list: The same list of message dictionaries, with an added
              'is_potential_stem' boolean key for each message.
    """
    if not messages or not isinstance(messages, list):
        print("Warning: Invalid input messages list. Returning empty list.")
        return []

    # Keywords/phrases suggesting a question or topic stem
    stem_keywords = [
        'who', 'what', 'when', 'where', 'why', 'how', # Question words
        'which of the following', 'choose the', 'select the',
        'next best step', 'next best line', 'approach', 'management',
        'treatment', 'indication', 'contraindication', 'c/i',
        'risk factor', 'cause', 'causes', 'except', 'not a', 'incorrect', 'correct statement',
        'definition', 'diagnosis', 'diagnose', 'differentiate',
        'complication', 'complicating', 'predictor', 'based on',
        'investigation of choice', 'most likely', 'picture', 'sign',
        'technique', 'trial', 'referencing', 'parameter', 'value',
        'count', 'grading', 'placement', 'entry', 'criteria',
        # Add more domain-specific keywords as observed
    ]

    # Precompile regex for efficiency if needed, simple check first
    # stem_keyword_regex = re.compile(r'\b(' + '|'.join(map(re.escape, stem_keywords)) + r')\b', re.IGNORECASE)
    # Using simple string contains for now, might need regex for word boundaries later

    for message in messages:
        text = message.get("cleaned_text", "")
        original_text = message.get("original_text", "")
        message["is_potential_stem"] = False # Default to false

        if not text:
            continue

        # --- Heuristics for Stem Identification ---

        # 1. Starts with "Q)" or similar convention
        # Allow for optional space after Q)
        if re.match(r'^\s*q\)\s*', text, re.IGNORECASE):
            message["is_potential_stem"] = True
            continue # If it matches Q), assume it's a stem

        # 2. Ends with a question mark
        if text.endswith('?'):
            message["is_potential_stem"] = True
            continue

        # 3. Contains stem keywords (check in lowercase 'cleaned_text')
        # Be careful with very common words, prioritize more specific ones
        # Maybe require a certain length or structure if only keywords are found?
        found_keyword = False
        for keyword in stem_keywords:
             # Use \b for word boundaries if using regex to avoid partial matches (e.g., 'indication' in 'vindication')
             # Simple check for now:
             if keyword in text:
                 found_keyword = True
                 break
        if found_keyword:
            # Add refinement: avoid flagging *very* short messages that just contain a keyword
            # For example, a message just saying "treatment" might be a reply, not a stem.
            # Let's set a minimum length or word count if only a keyword is found.
            word_count = len(text.split())
            if word_count > 3: # Arbitrary threshold, adjust as needed
                 message["is_potential_stem"] = True
                 continue
            # Also consider if it's just a topic mentioned briefly
            elif word_count <= 3 and (text.endswith(':') or ':' in text): # e.g. "Treatment:"
                message["is_potential_stem"] = True
                continue


        # 4. Looks like a clinical scenario description (often longer)
        # Heuristic: longer text, contains numbers (age, weeks), medical terms (can be added)
        # This is harder and overlaps with keywords. For now, keyword check covers most cases.
        # if len(text) > 50 and ('yr old' in text or 'weeks' in text or 'case' in text):
        #    message["is_potential_stem"] = True
        #    continue

        # 5. Specific patterns observed (e.g., "<Topic> - <detail>" structure)
        # Example: "Immature teratoma grading based on :"
        if ':' in text and len(text.split(':')[-1].strip()) < 15: # Ends with colon, short text after colon
             message["is_potential_stem"] = True
             continue


    return messages

# --- Main execution part ---
if __name__ == "__main__":
    input_file = "processed_messages.json"
    output_file = "stems_identified.json"

    try:
        with open(input_file, 'r', encoding='utf-8') as f:
            processed_messages = json.load(f)

        messages_with_stems = identify_potential_stems(processed_messages)

        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(messages_with_stems, f, indent=2, ensure_ascii=False)

        print(f"Potential stems identified and saved to {output_file}")

        # Optional: Print stems identified for quick review
        # print("\n--- Identified Potential Stems ---")
        # for msg in messages_with_stems:
        #     if msg.get("is_potential_stem"):
        #         print(f"ID: {msg['id']}, User: {msg['user']}, Text: {msg['original_text'][:100]}...") # Print first 100 chars
        # print("------------------------------")


    except FileNotFoundError:
        print(f"Error: Input file '{input_file}' not found. Run component 1 first.")
    except json.JSONDecodeError:
        print(f"Error: Could not decode JSON from '{input_file}'. Ensure it's valid.")
    except Exception as e:
        print(f"An error occurred during stem identification: {e}")