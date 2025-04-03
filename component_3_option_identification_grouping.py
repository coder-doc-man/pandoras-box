# component_3_option_identification_grouping.py
import json
import re

def identify_options_and_group(messages):
    """
    Identifies potential options and performs initial grouping with preceding stems.

    Args:
        messages (list): List of message dictionaries with 'is_potential_stem' flag.

    Returns:
        list: A list of dictionaries, where each dictionary represents a potential
              question group containing a 'stem' message object and a list of
              'option_messages' object list.
    """
    if not messages or not isinstance(messages, list):
        print("Warning: Invalid input messages list. Returning empty list.")
        return []

    potential_questions = []
    current_stem_message = None
    current_options_list = []

    # Option Pattern Ideas:
    # - Lines starting with A/B/C/D, 1/2/3/4 (with ., ), :)
    # - Lines starting with bullets (*, -, •)
    # - Lines within the *same message* as a stem, separated by newlines.
    # - Short lines following a stem message (heuristic).
    # - Lines mentioned explicitly ("Options:", "options were.:") - handle later?

    # Regex for common option markers (optional leading whitespace)
    # Handles A., A), A:, 1., 1), 1: etc. Also bullet points.
    option_marker_regex = re.compile(r"^\s*([a-d][\.\):]|[1-4][\.\):]|[-•*])\s+", re.IGNORECASE)
    # Regex to check if a line is likely just an option marker (e.g. "A.", "-")
    only_marker_regex = re.compile(r"^\s*([a-d][\.\):]?|[1-4][\.\):]?|[-•*])\s*$", re.IGNORECASE)

    for i, message in enumerate(messages):
        original_text = message.get("original_text", "")
        lines = original_text.splitlines()

        # Check if this message is identified as a potential stem
        if message.get("is_potential_stem"):
            # If we were accumulating options for a previous stem, store that question group.
            if current_stem_message and current_options_list:
                potential_questions.append({
                    "stem_message": current_stem_message,
                    "option_messages": current_options_list
                })
                # Reset for the new stem
                current_options_list = []

            # Start a new potential question group
            current_stem_message = message
            stem_lines = []
            potential_option_lines_in_stem = []

            # Separate stem content from potential options within the same message
            for line in lines:
                line_stripped = line.strip()
                if not line_stripped or only_marker_regex.match(line_stripped): # Ignore empty lines or lines with only a marker
                    continue

                # Check if the line *looks* like an option even if inside the stem message
                if option_marker_regex.match(line_stripped) or len(line_stripped) < 60 : # Heuristic: shorter lines might be options
                    # Check if it's *just* the option text without a marker but follows the stem text
                    if not option_marker_regex.match(line_stripped) and len(stem_lines) > 0:
                         # Check if it follows reasonably short previous option-like lines
                         if potential_option_lines_in_stem and len(potential_option_lines_in_stem[-1]) < 80:
                            potential_option_lines_in_stem.append(line_stripped)
                         # Or if it follows the main stem text directly and is short
                         elif len(stem_lines) == 1 and len(line_stripped) < 80:
                            potential_option_lines_in_stem.append(line_stripped)
                         else: # Likely part of the stem
                            stem_lines.append(line_stripped)

                    elif option_marker_regex.match(line_stripped):
                        potential_option_lines_in_stem.append(line_stripped)
                    else:
                         # Doesn't start like an option, likely part of the stem
                         stem_lines.append(line_stripped)
                else:
                    # Doesn't look like an option, likely part of the stem
                    stem_lines.append(line_stripped)

            # Update the stem message text to exclude the options found within it
            # Keep original_text, but maybe add a 'stem_text_only' field
            current_stem_message["stem_text_only"] = "\n".join(stem_lines).strip()

            # Add these internal options to the list for this stem
            if potential_option_lines_in_stem:
                 # Create a pseudo-message object for these internal options
                 current_options_list.append({
                     "id": message["id"], # Same ID as stem message
                     "user": message["user"], # Same user
                     "timestamp": message["timestamp"], # Same timestamp
                     "is_option_block": True, # Flag indicating these are options
                     "options_text": potential_option_lines_in_stem, # Store as list of strings
                     "source": "internal_to_stem_message"
                 })


        # Check if this message is *not* a stem, but might contain options for the *previous* stem
        elif current_stem_message:
            potential_option_lines_in_message = []
            is_likely_options = False

            # Heuristic: Does this message seem like it's listing options?
            # Check if multiple lines match option patterns or are short.
            option_like_line_count = 0
            non_empty_lines = [ln.strip() for ln in lines if ln.strip()]

            if not non_empty_lines: # Skip empty messages
                continue

            # Explicit markers like "options were:", "Options -"
            if re.match(r"^\s*(options were|options\s*-)", message["cleaned_text"]):
                 is_likely_options = True
                 # Add all non-empty lines following the marker
                 start_adding = False
                 for line in lines:
                     line_stripped = line.strip()
                     if re.match(r"^\s*(options were|options\s*-)", line_stripped.lower()):
                         start_adding = True
                         continue
                     if start_adding and line_stripped:
                          potential_option_lines_in_message.append(line_stripped)

            # Check lines individually if no explicit marker
            if not is_likely_options:
                for line in non_empty_lines:
                    if option_marker_regex.match(line) or len(line) < 80 : # Allow short lines too
                        option_like_line_count += 1
                # If most lines look like options, or the message is short and follows a stem
                if len(non_empty_lines) > 0 and \
                   (option_like_line_count >= len(non_empty_lines) * 0.5 or \
                   (len(non_empty_lines) <= 5 and len(message["cleaned_text"]) < 150)): # Arbitrary thresholds
                    is_likely_options = True
                    potential_option_lines_in_message = non_empty_lines # Add all lines if message seems option-focused


            # If it seems like options and follows a stem relatively closely
            # Basic check: is it the next message? (More complex time/user checks later)
            if is_likely_options and message["id"] == current_stem_message["id"] + 1:
                 current_options_list.append({
                     "id": message["id"],
                     "user": message["user"],
                     "timestamp": message["timestamp"],
                     "is_option_block": True,
                     "options_text": potential_option_lines_in_message,
                     "source": "separate_message"
                 })
            else:
                 # This message is not a stem and not identified as options for the current stem.
                 # Finalize the previous stem group if it exists and had options.
                 if current_stem_message and current_options_list:
                     potential_questions.append({
                         "stem_message": current_stem_message,
                         "option_messages": current_options_list
                     })
                 # Reset, as this message breaks the sequence.
                 current_stem_message = None
                 current_options_list = []

        # If message is not a stem and there's no active stem, just ignore it for grouping.
        else:
             # Finalize the previous stem group if it exists and had options.
             if current_stem_message and current_options_list:
                 potential_questions.append({
                     "stem_message": current_stem_message,
                     "option_messages": current_options_list
                 })
             # Reset
             current_stem_message = None
             current_options_list = []


    # Add the last accumulated question group if any
    if current_stem_message and current_options_list:
        potential_questions.append({
            "stem_message": current_stem_message,
            "option_messages": current_options_list
        })

    # Further cleaning of option text could happen here (e.g., stripping markers)

    return potential_questions

# --- Main execution part ---
if __name__ == "__main__":
    input_file = "stems_identified.json"
    output_file = "grouped_questions_basic.json"

    try:
        with open(input_file, 'r', encoding='utf-8') as f:
            messages_with_stems = json.load(f)

        potential_question_groups = identify_options_and_group(messages_with_stems)

        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(potential_question_groups, f, indent=2, ensure_ascii=False)

        print(f"Potential question groups (basic) saved to {output_file}")

        # Optional: Print summary for review
        # print(f"\n--- Found {len(potential_question_groups)} Potential Question Groups ---")
        # for i, group in enumerate(potential_question_groups):
        #     stem_text = group['stem_message'].get('stem_text_only', group['stem_message']['original_text'])
        #     print(f"\nGroup {i+1}:")
        #     print(f"  Stem (ID {group['stem_message']['id']}): {stem_text[:100]}...")
        #     print(f"  Options Found: {len(group['option_messages'])} block(s)")
        #     for opt_block in group['option_messages']:
        #          print(f"    - From Msg ID {opt_block['id']} ({opt_block['source']}): {len(opt_block['options_text'])} lines")
        #          # Print first few options text
        #          for opt_text in opt_block['options_text'][:2]:
        #               print(f"        * {opt_text[:80]}")
        # print("-------------------------------------")


    except FileNotFoundError:
        print(f"Error: Input file '{input_file}' not found. Run component 2 first.")
    except json.JSONDecodeError:
        print(f"Error: Could not decode JSON from '{input_file}'. Ensure it's valid.")
    except Exception as e:
        print(f"An error occurred during option identification/grouping: {e}")