import json
import re
import logging

logger = logging.getLogger(__name__)

def clean_and_parse_json(response_text: str) -> dict:
    """Clean Gemini API response and parse it into a proper JSON format."""
    logger.info("Starting JSON cleanup and parsing")
    
    def remove_markdown_blocks(text):
        """Remove markdown code block syntax"""
        return re.sub(r'```json\s*|\s*```', '', text).strip()

    def fix_quotes(text):
        """Fix inconsistent quote usage"""
        # First, replace all escaped quotes with a temporary marker
        text = text.replace('\\"', '!!QUOTE!!')
        
        # Replace all remaining double quotes with proper JSON double quotes
        text = text.replace('"', '"')
        
        # Restore escaped quotes
        text = text.replace('!!QUOTE!!', '\\"')
        
        return text

    try:
        # Step 1: Try parsing the raw response
        return json.loads(response_text)
    except json.JSONDecodeError:
        try:
            # Step 2: Remove markdown and fix quotes
            cleaned_text = remove_markdown_blocks(response_text)
            cleaned_text = fix_quotes(cleaned_text)
            
            logger.debug(f"Cleaned text: {cleaned_text}")
            return json.loads(cleaned_text)
            
        except json.JSONDecodeError as e:
            # Return a failure object instead of raising an error
            return {
                "status": "FAILED",
                "error": str(e),
                "raw_text": response_text
            }

def clean_numeric_value(value):
    """Convert currency string to float."""
    if isinstance(value, str):
        # Remove '$' and ',' from currency strings
        cleaned = value.replace('$', '').replace(',', '')
        try:
            return float(cleaned)
        except ValueError:
            return 0.0
    return float(value) if value else 0.0
