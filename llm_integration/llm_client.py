from config import get_config


class LLMClient:
    """LLM client that uses Anthropic or OpenAI if configured, else falls back."""

    def __init__(self):
        self.provider = None
        self.client = None

        cfg = get_config()
        anthropic_key = cfg.anthropic_api_key
        openai_key = cfg.openai_api_key

        if anthropic_key:
            try:
                import anthropic

                self.provider = "anthropic"
                self.client = anthropic.Anthropic(api_key=anthropic_key)
            except Exception:
                self.provider = None
                self.client = None
        elif openai_key:
            try:
                from openai import OpenAI

                self.provider = "openai"
                self.client = OpenAI(api_key=openai_key)
            except Exception:
                self.provider = None
                self.client = None

    def generate_response(self, prompt: str, results: dict = None) -> str:
        # Try configured provider; on any exception (e.g., 401 invalid x-api-key), fall back to demo response
        if self.provider == "anthropic" and self.client:
            try:
                msg = self.client.messages.create(
                    model="claude-3-5-sonnet-latest",
                    max_tokens=400,
                    messages=[{"role": "user", "content": prompt}],
                )
                return msg.content[0].text if getattr(msg, "content", None) else str(msg)
            except Exception:
                pass

        if self.provider == "openai" and self.client:
            try:
                chat = self.client.chat.completions.create(
                    model="gpt-4o-mini",
                    messages=[{"role": "user", "content": prompt}],
                    temperature=0.2,
                    max_tokens=400,
                )
                return chat.choices[0].message.content
            except Exception:
                pass

        # Generate a basic answer from structured data if available
        # This provides a response even without LLM API key
        return self._generate_basic_answer(prompt, results)
    
    def _generate_basic_answer(self, prompt: str, results: dict = None) -> str:
        """Generate a basic answer without LLM by parsing structured data."""
        # Use results directly if provided, otherwise try to parse from prompt
        if results:
            return self._format_answer_from_results(results)
        
        # Fallback: try to extract from prompt text
        import json
        import re
        try:
            if "Structured results" in prompt:
                json_match = re.search(r'Structured results.*?:\s*(\{.*\})', prompt, re.DOTALL)
                if json_match:
                    results_str = json_match.group(1)
                    results_str = results_str.strip()
                    try:
                        results = json.loads(results_str)
                        return self._format_answer_from_results(results)
                    except json.JSONDecodeError:
                        pass
        except Exception:
            pass
        
        # Fallback: check for error messages in the prompt
        if "District crop production dataset" in prompt or "district_crop_production_resource_id" in prompt:
            return "⚠️ **Dataset Error**: The district crop production dataset is not available. The resource ID may be incorrect or the dataset may not be accessible. Please verify your `DISTRICT_CROP_PRODUCTION_RESOURCE_ID` configuration in your `.env` file."
        
        if "schema unexpected" in prompt.lower() or "schema does not match" in prompt.lower():
            return "⚠️ **Schema Error**: The dataset does not contain the expected crop production columns. The resource ID may be pointing to the wrong dataset type."
        
        return "✓ Query processed. Please review the structured results and visualizations above for detailed information."
    
    def _format_answer_from_results(self, results: dict) -> str:
        """Format a readable answer from structured results."""
        answer_parts = []
        
        # Check for errors first
        answer_data = results.get('answer_data', {}) or {}
        
        # Check for district crop queries
        if 'district_highest_crop_year' in answer_data:
            dhc = answer_data['district_highest_crop_year']
            if 'error' in dhc:
                error_msg = dhc.get('error', 'Unknown error')
                if 'total_production' in dhc and dhc.get('warning'):
                    # State-level fallback response
                    state = dhc.get('state', '').title()
                    crop = dhc.get('crop', '').title()
                    year = dhc.get('year', '')
                    total_prod = dhc.get('total_production', 0)
                    return f"⚠️ **Note**: District-level data is not available. However, {state} had a total {crop} production of {total_prod:,.0f} tonnes in {year}. For district-specific information, please ensure a valid DISTRICT_CROP_PRODUCTION_RESOURCE_ID is configured."
                if 'schema unexpected' in error_msg.lower():
                    return "⚠️ **Error**: The district crop production dataset has an incorrect schema. The resource ID `DISTRICT_CROP_PRODUCTION_RESOURCE_ID` appears to be pointing to a different dataset type (possibly air pollution data). Please update your `.env` file with the correct resource ID for district crop production data from data.gov.in."
                return f"⚠️ **Error**: {error_msg}"
            elif 'district' in dhc and 'production' in dhc:
                state = dhc.get('state', '').title()
                district = dhc.get('district', '')
                crop = dhc.get('crop', '').title()
                year = dhc.get('year', '')
                production = dhc.get('production', 0)
                return f"**Answer**: {district} district in {state} had the highest {crop} production in {year}, with {production:,.0f} tonnes."
        
        # Check for other query types
        if 'district_crop_comparison' in answer_data:
            dcc = answer_data['district_crop_comparison']
            if 'error' in dcc:
                return f"⚠️ **Error**: {dcc.get('error', 'Unknown error')}"
            # Could format district comparison results here
        
        if 'rainfall_compare' in answer_data:
            rc = answer_data['rainfall_compare']
            if not isinstance(rc, dict):
                return "✓ Rainfall comparison data is available. See the visualization above for details."
        
        if 'top_crops_state' in answer_data:
            tcs = answer_data['top_crops_state']
            if 'error' not in tcs and 'crops' in tcs:
                state = tcs.get('state', '').title()
                top_n = tcs.get('top_n', 0)
                return f"✓ Top {top_n} crops for {state} are shown in the table above."
        
        # Generic success message
        return "✓ Query executed successfully. See the detailed results and visualizations above."


