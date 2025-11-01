import re
from typing import Dict, Optional


class QueryParser:
    """Very small parser that returns a structured dict."""

    def __init__(self, llm_client):
        self.llm_client = llm_client

    def parse_query(self, question: str, available: Dict) -> Dict:
        def normalize_state_name(name: str) -> str:
            n = name.strip()
            # minimal, targeted normalizations for common typos
            fixes = {
                "karnatak": "karnataka",
                "odisha": "odisha",  # placeholder for future custom logic
            }
            lowered = n.lower()
            if lowered in fixes:
                return fixes[lowered]
            # heuristic: if it ends with 'karnatak', append 'a'
            if lowered.endswith("karnatak"):
                return "karnataka"
            return n
        # Intent 1: Compare average annual rainfall in State_X and State_Y for last N years; list top M crops of Crop_Type
        pattern = re.compile(
            r"compare\s+the\s+average\s+annual\s+rainfall\s+in\s+(?P<s1>[A-Za-z\s_]+?)\s+and\s+(?P<s2>[A-Za-z\s_]+?)\s+for\s+the\s+last\s+(?P<n>\d+)\s+years.*?top\s+(?P<m>\d+)\s+most\s+produced\s+crops\s+of\s+(?P<crop_type>[^,\.]+?)(?:\s+in\s+each|$)",
            re.IGNORECASE,
        )
        match = pattern.search(question)
        if match:
            return {
                "intent": "rainfall_vs_top_crops",
                "state1": match.group("s1").strip(),
                "state2": match.group("s2").strip(),
                "years": int(match.group("n")),
                "top_m": int(match.group("m")),
                "crop_type": match.group("crop_type").strip(),
            }

        # Fallback simple rainfall compare without crops
        pattern2 = re.compile(
            r"average\s+annual\s+rainfall.*\b(?P<s1>[A-Za-z\s_]+)\b.*\b(?P<s2>[A-Za-z\s_]+)\b.*last\s+(?P<n>\d+)\s+years",
            re.IGNORECASE,
        )
        m2 = pattern2.search(question)
        if m2:
            return {
                "intent": "rainfall_compare",
                "state1": m2.group("s1").strip(),
                "state2": m2.group("s2").strip(),
                "years": int(m2.group("n")),
            }

        # Specific: "compare rainfall in X and Y for the last N years"
        pattern3a = re.compile(
            r"compare\s+rainfall\s+in\s+(?P<s1>[A-Za-z\s_]+?)\s+and\s+(?P<s2>[A-Za-z\s_]+?)\s+for\s+the\s+last\s+(?P<n>\d+)\s+years",
            re.IGNORECASE,
        )
        m3a = pattern3a.search(question)
        if m3a:
            return {
                "intent": "rainfall_compare",
                "state1": normalize_state_name(m3a.group("s1")),
                "state2": normalize_state_name(m3a.group("s2")),
                "years": int(m3a.group("n")),
            }

        # Specific: "compare rainfall in X and Y for N years" (without "the last")
        pattern3b = re.compile(
            r"compare\s+rainfall\s+in\s+(?P<s1>[A-Za-z\s_]+?)\s+and\s+(?P<s2>[A-Za-z\s_]+?)\s+for\s+(?P<n>\d+)\s+years",
            re.IGNORECASE,
        )
        m3b = pattern3b.search(question)
        if m3b:
            return {
                "intent": "rainfall_compare",
                "state1": normalize_state_name(m3b.group("s1")),
                "state2": normalize_state_name(m3b.group("s2")),
                "years": int(m3b.group("n")),
            }

        # Minimal phrasing without explicit years; default to last 5 years
        pattern3 = re.compile(
            r"compare\s+rainfall\s+in\s+(?P<s1>[A-Za-z\s_]+?)\s+and\s+(?P<s2>[A-Za-z\s_]+)\b",
            re.IGNORECASE,
        )
        m3 = pattern3.search(question)
        if m3:
            return {
                "intent": "rainfall_compare",
                "state1": normalize_state_name(m3.group("s1")),
                "state2": normalize_state_name(m3.group("s2")),
                "years": 5,
            }

        # Specific: "compare rainfall between X and Y for the last N years"
        pattern4a = re.compile(
            r"compare\s+rainfall\s+between\s+(?P<s1>[A-Za-z\s_]+?)\s+and\s+(?P<s2>[A-Za-z\s_]+?)\s+for\s+the\s+last\s+(?P<n>\d+)\s+years",
            re.IGNORECASE,
        )
        m4a = pattern4a.search(question)
        if m4a:
            return {
                "intent": "rainfall_compare",
                "state1": normalize_state_name(m4a.group("s1")),
                "state2": normalize_state_name(m4a.group("s2")),
                "years": int(m4a.group("n")),
            }

        # Specific: "compare rainfall between X and Y for N years" (without "the last")
        pattern4b = re.compile(
            r"compare\s+rainfall\s+between\s+(?P<s1>[A-Za-z\s_]+?)\s+and\s+(?P<s2>[A-Za-z\s_]+?)\s+for\s+(?P<n>\d+)\s+years",
            re.IGNORECASE,
        )
        m4b = pattern4b.search(question)
        if m4b:
            return {
                "intent": "rainfall_compare",
                "state1": normalize_state_name(m4b.group("s1")),
                "state2": normalize_state_name(m4b.group("s2")),
                "years": int(m4b.group("n")),
            }

        # Alternate phrasing: "compare rainfall between X and Y"; default to last 5 years
        pattern4 = re.compile(
            r"compare\s+rainfall\s+between\s+(?P<s1>[A-Za-z\s_]+?)\s+and\s+(?P<s2>[A-Za-z\s_]+)\b",
            re.IGNORECASE,
        )
        m4 = pattern4.search(question)
        if m4:
            return {
                "intent": "rainfall_compare",
                "state1": normalize_state_name(m4.group("s1")),
                "state2": normalize_state_name(m4.group("s2")),
                "years": 5,
            }

        # District comparison: "Identify the district in <StateA> with the highest production of <CropA> ... compare with ... lowest production of <CropB> in <StateB>"
        pattern5 = re.compile(
            r"identify\s+the\s+district\s+in\s+(?P<smax>[A-Za-z\s_]+?)\s+with\s+the\s+highest\s+production\s+of\s+(?P<cmax>[A-Za-z\s_]+?)\s+.*?compare\s+that\s+with\s+the\s+district\s+with\s+the\s+lowest\s+production\s+of\s+(?P<cmin>[A-Za-z\s_]+?)\s+in\s+(?P<smin>[A-Za-z\s_]+)\??",
            re.IGNORECASE,
        )
        m5 = pattern5.search(question)
        if m5:
            return {
                "intent": "district_crop_extrema_compare",
                "state_max": normalize_state_name(m5.group("smax")),
                "crop_max": m5.group("cmax").strip(),
                "state_min": normalize_state_name(m5.group("smin")),
                "crop_min": m5.group("cmin").strip(),
            }

        # Top N crops in state: "List the top N crops produced in <State> during the last M years"
        pattern6 = re.compile(
            r"list\s+the\s+top\s+(?P<n>\d+)\s+crops?\s+produced\s+in\s+(?P<state>[A-Za-z\s_]+?)\s+during\s+the\s+last\s+(?P<years>\d+)\s+years",
            re.IGNORECASE,
        )
        m6 = pattern6.search(question)
        if m6:
            return {
                "intent": "top_crops_state",
                "state": normalize_state_name(m6.group("state")),
                "top_n": int(m6.group("n")),
                "years": int(m6.group("years")),
            }

        # District highest production: "Which district in [State] had the highest [Crop] production in [Year]?"
        pattern7 = re.compile(
            r"which\s+district\s+(?:in\s+)?(?P<state>[A-Za-z\s_]+?)\s+(?:had\s+)?(?:the\s+)?highest\s+(?P<crop>[A-Za-z\s_]+?)(?:\s+production)?\s+(?:in\s+)?(?P<year>\d{4})\??",
            re.IGNORECASE,
        )
        m7 = pattern7.search(question)
        if m7:
            return {
                "intent": "district_highest_crop_year",
                "state": normalize_state_name(m7.group("state")),
                "crop": m7.group("crop").strip(),
                "year": int(m7.group("year")),
            }
        
        # Alternative: "Which district had highest [Crop] in [State] in [Year]?"
        # Need to be careful - crop comes first, then state, then year
        pattern7b = re.compile(
            r"which\s+district\s+(?:had\s+)?(?:the\s+)?highest\s+(?P<crop>[A-Za-z]+)\s+in\s+(?P<state>[A-Za-z\s_]+?)\s+in\s+(?P<year>\d{4})\??",
            re.IGNORECASE,
        )
        m7b = pattern7b.search(question)
        if m7b:
            crop = m7b.group("crop").strip()
            state = m7b.group("state").strip()
            # Make sure crop doesn't include "in" or state name
            crop = crop.split()[0] if crop.split() else crop  # Take first word only
            return {
                "intent": "district_highest_crop_year",
                "state": normalize_state_name(state),
                "crop": crop,
                "year": int(m7b.group("year")),
            }

        # Compare crop production across districts: "Compare [Crop] production across all districts in [State] for the last N years"
        pattern8 = re.compile(
            r"compare\s+(?P<crop>[A-Za-z\s_]+?)\s+production\s+across\s+all\s+districts\s+in\s+(?P<state>[A-Za-z\s_]+?)\s+for\s+the\s+last\s+(?P<years>\d+)\s+years\.?",
            re.IGNORECASE,
        )
        m8 = pattern8.search(question)
        if m8:
            return {
                "intent": "district_crop_comparison",
                "state": normalize_state_name(m8.group("state")),
                "crop": m8.group("crop").strip(),
                "years": int(m8.group("years")),
            }

        # Try flexible keyword-based parsing as fallback for unmatched queries
        try:
            return self._parse_with_keywords(question, available)
        except Exception:
            # If keyword parsing fails, return unknown with helpful message
            return {
                "intent": "unknown", 
                "question": question, 
                "available_datasets": available,
                "error": "Query not recognized. Please use one of the supported query formats.",
                "suggestions": [
                    "Compare rainfall in [State1] and [State2] for the last N years",
                    "List the top N crops produced in [State] during the last M years",
                    "Which district in [State] had the highest [Crop] production in [Year]",
                    "Compare [Crop] production across all districts in [State] for the last N years"
                ]
            }
    
    def _parse_with_keywords(self, question: str, available: Dict) -> Dict:
        """Use keyword matching to parse queries that don't match regex patterns."""
        
        # Try to extract key information using simple keyword matching
        question_lower = question.lower()
        
        # Check for rainfall queries (flexible)
        rainfall_keywords = ['rainfall', 'rain', 'precipitation']
        if any(kw in question_lower for kw in rainfall_keywords):
            # Try to extract states and years
            import re
            states = []
            years = []
            
            # Common state names
            common_states = ['karnataka', 'tamil nadu', 'maharashtra', 'punjab', 'gujarat', 
                           'uttar pradesh', 'bihar', 'west bengal', 'odisha', 'rajasthan']
            for state in common_states:
                if state in question_lower:
                    states.append(state)
            
            # Extract year count (e.g., "5 years")
            year_match = re.search(r'(\d+)\s*years?', question_lower)
            if year_match:
                years.append(int(year_match.group(1)))
            
            if len(states) >= 2:
                return {
                    "intent": "rainfall_compare",
                    "state1": states[0],
                    "state2": states[1],
                    "years": years[0] if years else 5
                }
        
        # Check for top crops queries
        if 'top' in question_lower and 'crop' in question_lower:
            import re
            top_match = re.search(r'top\s+(\d+)', question_lower)
            top_n = int(top_match.group(1)) if top_match else 10
            
            year_match = re.search(r'(\d+)\s*years?', question_lower)
            years = int(year_match.group(1)) if year_match else 5
            
            common_states = ['karnataka', 'tamil nadu', 'maharashtra', 'punjab', 'gujarat']
            state = None
            for s in common_states:
                if s in question_lower:
                    state = s
                    break
            
            if state:
                return {
                    "intent": "top_crops_state",
                    "state": state,
                    "top_n": top_n,
                    "years": years
                }
        
        # Check for district queries (more flexible pattern matching)
        if 'district' in question_lower and ('highest' in question_lower or 'had' in question_lower):
            import re
            common_states = ['karnataka', 'tamil nadu', 'maharashtra', 'punjab', 'gujarat', 
                           'uttar pradesh', 'bihar', 'west bengal', 'odisha', 'rajasthan',
                           'andhra pradesh', 'telangana', 'kerala', 'haryana', 'himachal pradesh',
                           'madhya pradesh', 'assam', 'jharkhand', 'chhattisgarh']
            
            # Extract 4-digit year first (like 2020, 2019, etc.)
            year_match = re.search(r'\b(20\d{2}|19\d{2})\b', question)
            year = int(year_match.group(1)) if year_match else None
            
            # Extract crop name (common crops) - check more carefully, prioritize before state
            crops = ['rice', 'wheat', 'sugarcane', 'cotton', 'maize', 'jowar', 'bajra', 
                    'millet', 'pulses', 'oilseed', 'groundnut', 'soybean', 'barley', 'mustard',
                    'ragi', 'tur', 'gram', 'moong', 'urad', 'arhar']
            crop = None
            crop_positions = []
            for c in crops:
                # Use word boundaries to match whole words only
                match = re.search(r'\b' + re.escape(c) + r'\b', question_lower)
                if match:
                    crop_positions.append((match.start(), c))
            
            # Get the crop that appears earliest (before state)
            if crop_positions:
                crop_positions.sort()
                crop = crop_positions[0][1]
            
            # Find state - try to find it after the crop
            state = None
            state_positions = []
            for s in common_states:
                match = re.search(r'\b' + re.escape(s) + r'\b', question_lower)
                if match:
                    state_positions.append((match.start(), s))
            
            # Get the state that appears after the crop
            if state_positions and crop_positions:
                crop_start = crop_positions[0][0]
                # Find state that appears after crop
                for pos, s in state_positions:
                    if pos > crop_start:
                        state = s
                        break
                # If no state after crop, take first state
                if not state and state_positions:
                    state_positions.sort()
                    state = state_positions[0][1]
            elif state_positions:
                state_positions.sort()
                state = state_positions[0][1]
            
            if state and crop and year:
                return {
                    "intent": "district_highest_crop_year",
                    "state": normalize_state_name(state),
                    "crop": crop,
                    "year": year
                }
            
            if state and crop:
                year_match = re.search(r'(\d+)\s*years?', question_lower)
                years = int(year_match.group(1)) if year_match else 5
                return {
                    "intent": "district_crop_comparison",
                    "state": normalize_state_name(state),
                    "crop": crop,
                    "years": years
                }
        
        raise Exception("Could not parse query")


