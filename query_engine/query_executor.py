from typing import Any, Dict, List, Optional
import re

import pandas as pd
from config import get_config
from data_processing.data_fetcher import DataFetcher


class QueryExecutor:
    def __init__(self, data_store):
        self.data_store = data_store
        self.fetcher = DataFetcher()

    def execute(self, plan: Dict[str, Any]) -> Dict[str, Any]:
        results: Dict[str, Any] = {}
        for step in plan.get("steps", []):
            if isinstance(step, str):
                continue
            if step.get("type") == "compute_rainfall_compare":
                results["rainfall_compare"] = self._compute_rainfall_compare(
                    step["state1"], step["state2"], step["years"]
                )
            if step.get("type") == "compute_top_crops":
                results["top_crops"] = self._compute_top_crops(
                    step["state1"], step["state2"], step["top_m"], step["crop_type"]
                )
            if step.get("type") == "compute_district_crop_extrema":
                results["district_crop_extrema"] = self._compute_district_crop_extrema(
                    step["state_max"], step["crop_max"], step["state_min"], step["crop_min"]
                )
            if step.get("type") == "compute_top_crops_state":
                results["top_crops_state"] = self._compute_top_crops_state(
                    step["state"], step["top_n"], step["years"]
                )
            if step.get("type") == "compute_district_highest_crop_year":
                results["district_highest_crop_year"] = self._compute_district_highest_crop_year(
                    step["state"], step["crop"], step["year"]
                )
            if step.get("type") == "compute_district_crop_comparison":
                results["district_crop_comparison"] = self._compute_district_crop_comparison(
                    step["state"], step["crop"], step["years"]
                )
        return {"plan": plan, "answer_data": results}

    def _load_crop_production(self) -> pd.DataFrame:
        cfg = get_config()
        rid = cfg.crop_production_resource_id
        return self.fetcher.fetch_dataset(rid)

    def _validate_crop_production_schema(self, df: pd.DataFrame) -> bool:
        """Validate that a dataframe has crop production schema."""
        if df is None or df.empty:
            return False
        
        def find_col(candidates):
            for c in candidates:
                if c in df.columns:
                    return c
            lower_map = {c.lower(): c for c in df.columns}
            for c in candidates:
                if c.lower() in lower_map:
                    return lower_map[c.lower()]
            return None
        
        # Check for required columns (state/district/crop/production)
        state_col = find_col(["State_Name", "STATE", "State", "STATE/UT", "STATE_UT_NAME", "State Name", "state"])
        district_col = find_col(["District_Name", "District", "DISTRICT", "District Name", "district"])
        crop_col = find_col(["Crop", "CROP", "Crop_Name", "Commodity", "CROP_NAME", "crop"])
        prod_col = find_col(["Production", "PRODUCTION", "Prodn", "PROD", "production_tonnes", "Prod", "production"])
        
        # For district crop production, we need at least state, district, crop, and production
        return bool(state_col and district_col and crop_col and prod_col)

    def _load_district_crop_production(self) -> pd.DataFrame:
        """Load district crop production dataset with schema validation and fallback."""
        # Try data store first
        df = self.data_store.get_dataset('crop_production_district_season')
        
        # Validate schema if dataset exists
        if df is not None and not df.empty and self._validate_crop_production_schema(df):
            return df
        
        # Fallback: fetch directly from API (try CKAN first, then data.gov.in)
        cfg = get_config()
        resource_ids_to_try = []
        
        # First try the configured resource ID
        if cfg.district_crop_production_resource_id:
            resource_ids_to_try.append(cfg.district_crop_production_resource_id)
        
        # Try auto-discovery as fallback
        try:
            from data_discovery.ckan_client import DataGovInClient
            client = DataGovInClient()
            discovered_id = client.discover_district_crop_production_resource_id()
            if discovered_id and discovered_id not in resource_ids_to_try:
                resource_ids_to_try.append(discovered_id)
        except Exception:
            pass
        
        # Try each resource ID with both CKAN and data.gov.in endpoints
        for resource_id in resource_ids_to_try:
            # First try CKAN datastore_search endpoint
            try:
                df_fetched = self.fetcher.fetch_dataset(resource_id, use_ckan=True)
                # Validate the fetched dataset
                if self._validate_crop_production_schema(df_fetched):
                    # Cache it in the data store for future use
                    self.data_store.add_dataset(
                        'agriculture',
                        'crop_production_district_season',
                        df_fetched,
                        {'id': resource_id, 'name': 'District-wise Season-wise Crop Production'}
                    )
                    return df_fetched
            except Exception:
                pass
            
            # Fallback to data.gov.in API
            try:
                df_fetched = self.fetcher.fetch_dataset(resource_id, use_ckan=False)
                # Validate the fetched dataset
                if self._validate_crop_production_schema(df_fetched):
                    # Cache it in the data store for future use
                    self.data_store.add_dataset(
                        'agriculture',
                        'crop_production_district_season',
                        df_fetched,
                        {'id': resource_id, 'name': 'District-wise Season-wise Crop Production'}
                    )
                    return df_fetched
            except Exception:
                # Try next resource ID
                continue
        
        return pd.DataFrame()

    def _load_rainfall(self) -> pd.DataFrame:
        cfg = get_config()
        rid = cfg.rainfall_resource_id
        if not rid:
            return pd.DataFrame()
        return self.fetcher.fetch_dataset(rid)

    def _compute_rainfall_compare(self, state1: str, state2: str, years: int) -> Dict[str, Any]:
        df = self._load_rainfall()
        if df.empty:
            return {"note": "No rainfall dataset configured"}

        # Try common column names (case-insensitive)
        state_col_candidates: List[str] = [
            "STATE",
            "STATE_UT_NAME",
            "SUBDIVISION",
            "State",
            "STATE/UT",
            "subdivision",
        ]
        year_col_candidates: List[str] = ["YEAR", "Year", "year", "Year", "YEAR"]
        annual_col_candidates: List[str] = ["ANNUAL", "Annual", "ANN", "Rainfall", "annual"]

        def pick(cols: List[str]) -> str | None:
            # exact match first
            for c in cols:
                if c in df.columns:
                    return c
            # case-insensitive fallback
            lower_map = {c.lower(): c for c in df.columns}
            for c in cols:
                lc = c.lower()
                if lc in lower_map:
                    return lower_map[lc]
            return None

        sc = pick(state_col_candidates)
        yc = pick(year_col_candidates)
        ac = pick(annual_col_candidates)
        if not (sc and yc and ac):
            return {"error": "Rainfall dataset has unexpected schema", "columns": list(df.columns)}

        df[yc] = pd.to_numeric(df[yc], errors="coerce")
        df[ac] = pd.to_numeric(df[ac], errors="coerce")
        recent_years = sorted(df[yc].dropna().unique())[-years:]
        sub = df[df[yc].isin(recent_years)]

        out = {}
        for s in [state1, state2]:
            s_sub = sub[sub[sc].str.contains(s, case=False, na=False)]
            out[s] = {
                "years": recent_years,
                "avg_annual_mm": float(s_sub[ac].mean()) if not s_sub.empty else None,
            }
        return out

    def _compute_top_crops(self, state1: str, state2: str, top_m: int, crop_type: str) -> Dict[str, Any]:
        df = self._load_crop_production()
        if df.empty:
            return {"note": "Crop production dataset empty"}

        # Normalize likely columns across variants
        def find_col(candidates):
            for c in candidates:
                if c in df.columns:
                    return c
            # case-insensitive match
            lower_map = {c.lower(): c for c in df.columns}
            for c in candidates:
                if c.lower() in lower_map:
                    return lower_map[c.lower()]
            return None

        state_col = find_col(["State_Name", "STATE", "State", "STATE/UT", "STATE_UT_NAME", "State Name"])
        crop_col = find_col(["Crop", "CROP", "Crop_Name", "Commodity", "CROP_NAME"])
        prod_col = find_col(["Production", "PRODUCTION", "Prodn", "PROD", "production_tonnes"]) or "Production"

        for col in [prod_col, "Area"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        if not state_col or not crop_col or prod_col not in df.columns:
            return {"error": "Crop dataset schema unexpected", "columns": list(df.columns)}

        out = {}
        for s in [state1, state2]:
            s_df = df[df[state_col].astype(str).str.contains(s, case=False, na=False)]
            if crop_type:
                s_df = s_df[s_df[crop_col].astype(str).str.contains(crop_type, case=False, na=False)]
            top = (
                s_df.groupby(crop_col, as_index=False)[prod_col]
                .sum()
                .sort_values(prod_col, ascending=False)
                .head(top_m)
            )
            out[s] = top.to_dict(orient="records")
        return out

    def _compute_district_crop_extrema(self, state_max: str, crop_max: str, state_min: str, crop_min: str) -> Dict[str, Any]:
        df = self._load_district_crop_production()
        if df is None or df.empty:
            cfg = get_config()
            resource_id = cfg.district_crop_production_resource_id or "not configured"
            return {
                "error": f"District crop production dataset not available. Resource ID: {resource_id}",
                "hint": "The district crop production resource ID may be incorrect or the dataset may not be accessible."
            }

        # Flexible column detection
        def find_col(candidates):
            for c in candidates:
                if c in df.columns:
                    return c
            lower_map = {c.lower(): c for c in df.columns}
            for c in candidates:
                if c.lower() in lower_map:
                    return lower_map[c.lower()]
            return None

        state_col = find_col(["State_Name", "STATE", "State", "STATE/UT", "STATE_UT_NAME", "State Name"])
        district_col = find_col(["District_Name", "District", "DISTRICT", "District Name"])
        crop_col = find_col(["Crop", "CROP", "Crop_Name", "Commodity", "CROP_NAME"])
        prod_col = find_col(["Production", "PRODUCTION", "Prodn", "PROD", "production_tonnes", "Prod"])
        year_col = find_col(["Year", "YEAR", "year"])

        if not all([state_col, district_col, crop_col, prod_col, year_col]):
            return {"error": "District dataset schema unexpected", "columns": list(df.columns)}

        # Normalize numeric
        df[prod_col] = pd.to_numeric(df[prod_col], errors="coerce")
        df[year_col] = pd.to_numeric(df[year_col], errors="coerce")

        def extrema_for(state: str, crop: str, mode: str):
            sub = df[
                df[state_col].astype(str).str.contains(state, case=False, na=False)
                & df[crop_col].astype(str).str.contains(crop, case=False, na=False)
            ]
            if sub.empty:
                return None
            # most recent year for this subset
            years = sorted(sub[year_col].dropna().unique())
            if not years:
                return None
            recent = years[-1]
            sub_recent = sub[sub[year_col] == recent]
            grouped = (
                sub_recent.groupby(district_col, as_index=False)[prod_col].sum().sort_values(prod_col, ascending=(mode=="min"))
            )
            row = grouped.iloc[0]
            return {"state": state, "crop": crop, "district": str(row[district_col]), "year": int(recent), "production": float(row[prod_col])}

        return {
            "max": extrema_for(state_max, crop_max, mode="max"),
            "min": extrema_for(state_min, crop_min, mode="min"),
        }

    def _compute_top_crops_state(self, state: str, top_n: int, years: int) -> Dict[str, Any]:
        # Try data store datasets (loaded during initialization) - try district first (has state data), then major crops
        df = self.data_store.get_dataset('crop_production_district_season')
        if df is None or df.empty:
            df = self.data_store.get_dataset('crop_production_major_crops')
        if df is None or df.empty:
            # Fallback to direct API fetch
            df = self._load_crop_production()
        if df.empty:
            return {"error": "Crop production dataset empty", "state": state}

        def find_col(candidates):
            for c in candidates:
                if c in df.columns:
                    return c
            lower_map = {c.lower(): c for c in df.columns}
            for c in candidates:
                if c.lower() in lower_map:
                    return lower_map[c.lower()]
            return None

        state_col = find_col(["State_Name", "STATE", "State", "STATE/UT", "STATE_UT_NAME", "State Name"])
        crop_col = find_col(["Crop", "CROP", "Crop_Name", "Commodity", "CROP_NAME"])
        prod_col = find_col(["Production", "PRODUCTION", "Prodn", "PROD", "production_tonnes"]) or "Production"
        year_col = find_col(["Year", "YEAR", "year", "_year"])

        # Check if this is a "long format" dataset (State, Crop, Year, Production columns)
        if state_col and crop_col and year_col and prod_col:
            # Normalize numeric columns
            df[prod_col] = pd.to_numeric(df[prod_col], errors="coerce")
            df[year_col] = pd.to_numeric(df[year_col], errors="coerce")
            
            # Filter by state
            state_filter = df[state_col].astype(str).str.contains(state, case=False, na=False)
            filtered = df[state_filter].copy()
            
            if filtered.empty:
                return {"error": f"No data found for {state}", "state": state, "debug": {"total_rows": len(df), "sample_state_values": df[state_col].astype(str).unique()[:5].tolist() if state_col in df.columns else []}}
            
            # Get recent years
            all_years = sorted(filtered[year_col].dropna().unique())
            recent_years = all_years[-years:] if len(all_years) >= years else all_years
            
            # Filter by recent years
            filtered = filtered[filtered[year_col].isin(recent_years)]
            
            if filtered.empty:
                return {"error": f"No data found for {state} in recent {years} years", "state": state, "debug": {"available_years": all_years, "requested_years": recent_years}}
            
            # Drop rows with null/missing crop or production values
            filtered = filtered.dropna(subset=[crop_col, prod_col])
            
            # Aggregate by crop across years (sum production for each crop)
            # Make sure crop_col values are strings for grouping
            filtered[crop_col] = filtered[crop_col].astype(str).str.strip()
            filtered = filtered[filtered[crop_col] != '']  # Remove empty crop names
            
            if filtered.empty:
                return {"error": f"No valid crop data found for {state} in recent {years} years", "state": state}
            
            # Group by crop and sum production
            top = (
                filtered.groupby(crop_col, as_index=False)[prod_col]
                .sum()
                .sort_values(prod_col, ascending=False)
                .head(top_n)
            )
            
            # Debug: If we got fewer crops than expected, include debug info
            unique_crops_count = filtered[crop_col].nunique() if crop_col in filtered.columns else 0
            
            result = {
                "state": state,
                "top_n": top_n,
                "years": int(years),
                "crops": top.to_dict(orient="records"),
                "year_range": [int(y) for y in recent_years],
            }
            
            # Add debug info if we got fewer crops than requested
            if len(top) < top_n:
                result["debug_info"] = {
                    "total_rows_after_filter": len(filtered),
                    "unique_crops_found": unique_crops_count,
                    "crops_returned": len(top),
                    "sample_crops": filtered[crop_col].unique()[:20].tolist() if crop_col in filtered.columns else [],
                    "sample_data": filtered[[crop_col, prod_col, year_col]].head(20).to_dict(orient="records") if all(c in filtered.columns for c in [crop_col, prod_col, year_col]) else []
                }
            
            return result
        
        # If we got here and have the required columns but didn't process, there's an issue
        if state_col and crop_col and prod_col:
            return {
                "error": f"Could not process dataset for {state}. Dataset format may be unexpected.",
                "state": state,
                "available_columns": list(df.columns),
                "found_columns": {
                    "state": state_col,
                    "crop": crop_col,
                    "production": prod_col,
                    "year": year_col
                }
            }

        # Check if this is a "wide format" dataset (year rows, crop columns)
        # Try to filter by state if state column exists, even in wide format
        if state_col:
            # Wide format but has state column - filter by state first
            state_filter = df[state_col].astype(str).str.contains(state, case=False, na=False)
            df = df[state_filter].copy()
            if df.empty:
                return {"error": f"No data found for {state} in wide format dataset", "state": state}
        
        if "_year" in df.columns or (year_col and year_col.startswith("_")):
            # Wide format: each row is a year, columns are crop names
            year_col = "_year" if "_year" in df.columns else find_col(["_year", "Year", "YEAR", "year"])
            if year_col and year_col in df.columns:
                # Get all years (may be strings like "2014-15" or numeric)
                years_raw = df[year_col].dropna().unique()
                # Try to extract numeric year from strings like "2014-15" -> 2014
                years_numeric = []
                for y in years_raw:
                    try:
                        # If it's already numeric
                        ynum = int(float(y))
                        years_numeric.append(ynum)
                    except (ValueError, TypeError):
                        # Try to extract first 4 digits if string like "2014-15"
                        match = re.search(r'(\d{4})', str(y))
                        if match:
                            years_numeric.append(int(match.group(1)))
                
                if not years_numeric:
                    return {"error": "Could not parse years from dataset", "year_column": year_col, "sample_values": list(years_raw[:5])}
                
                # Get recent years
                recent_years = sorted(years_numeric)[-years:]
                # Filter rows - match by extracting year from the year column
                df_recent = df.copy()
                def extract_year(val):
                    try:
                        if pd.api.types.is_numeric_dtype(type(val)):
                            return int(float(val))
                        match = re.search(r'(\d{4})', str(val))
                        return int(match.group(1)) if match else None
                    except (ValueError, TypeError, AttributeError):
                        return None
                
                df_recent['_year_numeric'] = df_recent[year_col].apply(extract_year)
                df_recent = df_recent[df_recent['_year_numeric'].isin(recent_years)].copy()
                
                # Melt to long format: year -> crop -> production
                id_cols = [year_col]
                if state_col and state_col in df_recent.columns:
                    id_cols.append(state_col)
                # All other numeric columns are crops (exclude the helper column)
                crop_cols = [c for c in df_recent.columns if c not in [year_col, '_year_numeric'] + ([state_col] if state_col else []) and pd.api.types.is_numeric_dtype(df_recent[c])]
                
                if not crop_cols:
                    return {"error": "No crop production columns found in wide format dataset", "columns": list(df.columns)}
                
                # Melt the dataframe
                df_long = df_recent.melt(id_vars=id_cols, value_vars=crop_cols, var_name="Crop", value_name="Production")
                df_long["Production"] = pd.to_numeric(df_long["Production"], errors="coerce")
                
                # Aggregate by crop across years
                top = (
                    df_long.groupby("Crop", as_index=False)["Production"]
                    .sum()
                    .sort_values("Production", ascending=False)
                    .head(top_n)
                )
                
                return {
                    "state": state,
                    "top_n": top_n,
                    "years": int(years),
                    "crops": top.to_dict(orient="records"),
                    "year_range": sorted(recent_years),
                }

    def _try_state_level_fallback(self, state: str, crop: str, year: Optional[int] = None) -> Optional[Dict[str, Any]]:
        """Try to provide state-level crop data as fallback when district data is unavailable."""
        try:
            # Try to get major crops dataset
            df = self.data_store.get_dataset('crop_production_major_crops')
            if df is None or df.empty:
                df = self._load_crop_production()
            
            if df.empty:
                return None
            
            def find_col(candidates):
                for c in candidates:
                    if c in df.columns:
                        return c
                lower_map = {c.lower(): c for c in df.columns}
                for c in candidates:
                    if c.lower() in lower_map:
                        return lower_map[c.lower()]
                return None
            
            state_col = find_col(["State_Name", "STATE", "State", "STATE/UT", "STATE_UT_NAME", "State Name"])
            crop_col = find_col(["Crop", "CROP", "Crop_Name", "Commodity", "CROP_NAME"])
            prod_col = find_col(["Production", "PRODUCTION", "Prodn", "PROD", "production_tonnes"])
            year_col = find_col(["Year", "YEAR", "year"])
            
            if not all([state_col, crop_col, prod_col]):
                return None
            
            # Filter by state and crop
            state_filter = df[state_col].astype(str).str.contains(state, case=False, na=False)
            crop_filter = df[crop_col].astype(str).str.contains(crop, case=False, na=False)
            filtered = df[state_filter & crop_filter]
            
            if year_col and year:
                # Filter by year if available
                df[year_col] = pd.to_numeric(df[year_col], errors="coerce")
                filtered = filtered[filtered[year_col] == year]
            
            if filtered.empty:
                return None
            
            # Get production data
            df[prod_col] = pd.to_numeric(df[prod_col], errors="coerce")
            total_production = filtered[prod_col].sum()
            
            return {
                "state": state,
                "crop": crop,
                "year": year,
                "total_production": float(total_production),
                "note": "State-level data (district-level not available)"
            }
        except Exception:
            return None

    def _compute_district_highest_crop_year(self, state: str, crop: str, year: int) -> Dict[str, Any]:
        df = self._load_district_crop_production()
        if df is None or df.empty:
            # Try state-level fallback
            fallback_result = self._try_state_level_fallback(state, crop, year)
            if fallback_result:
                return {
                    **fallback_result,
                    "warning": "District-level data not available. Showing state-level summary instead.",
                    "note": "For specific district information, please ensure DISTRICT_CROP_PRODUCTION_RESOURCE_ID is correctly configured.",
                    "suggestion": f"To get district-level data, try configuring a valid resource ID for district crop production."
                }
            
            cfg = get_config()
            resource_id = cfg.district_crop_production_resource_id or "not configured"
            return {
                "error": f"District crop production dataset not available. Resource ID: {resource_id}",
                "hint": "The district crop production resource ID may be incorrect or the dataset may not be accessible.",
                "required_columns": ["State", "District", "Crop", "Production", "Year"],
                "suggestion": f"Try a state-level query instead, such as 'List the top crops produced in {state.title()}' or check your DISTRICT_CROP_PRODUCTION_RESOURCE_ID configuration.",
                "alternative_queries": [
                    f"List the top 10 crops produced in {state.title()} during the last 5 years",
                    f"Compare {crop} production across all districts in {state.title()} for the last 5 years"
                ]
            }

        def find_col(candidates):
            for c in candidates:
                if c in df.columns:
                    return c
            lower_map = {c.lower(): c for c in df.columns}
            for c in candidates:
                if c.lower() in lower_map:
                    return lower_map[c.lower()]
            return None

        state_col = find_col(["State_Name", "STATE", "State", "STATE/UT", "STATE_UT_NAME", "State Name"])
        district_col = find_col(["District_Name", "District", "DISTRICT", "District Name"])
        crop_col = find_col(["Crop", "CROP", "Crop_Name", "Commodity", "CROP_NAME"])
        prod_col = find_col(["Production", "PRODUCTION", "Prodn", "PROD", "production_tonnes", "Prod"])
        year_col = find_col(["Year", "YEAR", "year"])

        if not all([state_col, district_col, crop_col, prod_col, year_col]):
            return {
                "error": "District dataset schema unexpected - dataset does not contain required crop production columns",
                "columns": list(df.columns),
                "required_columns": {
                    "state": "State_Name/STATE/State",
                    "district": "District_Name/District/DISTRICT", 
                    "crop": "Crop/CROP/Crop_Name",
                    "production": "Production/PRODUCTION/Prodn",
                    "year": "Year/YEAR/year"
                },
                "found_columns": {
                    "state": state_col,
                    "district": district_col,
                    "crop": crop_col,
                    "production": prod_col,
                    "year": year_col
                }
            }

        # Normalize numeric
        df[prod_col] = pd.to_numeric(df[prod_col], errors="coerce")
        df[year_col] = pd.to_numeric(df[year_col], errors="coerce")

        # Filter by state, crop, and year
        filtered = df[
            df[state_col].astype(str).str.contains(state, case=False, na=False)
            & df[crop_col].astype(str).str.contains(crop, case=False, na=False)
            & (df[year_col] == year)
        ]

        if filtered.empty:
            return {
                "error": f"No data found for {state}, {crop}, year {year}",
                "state": state,
                "crop": crop,
                "year": year,
            }

        # Group by district and sum production (in case of multiple records per district)
        grouped = filtered.groupby(district_col, as_index=False)[prod_col].sum()
        # Get district with highest production
        max_row = grouped.loc[grouped[prod_col].idxmax()]

        return {
            "state": state,
            "crop": crop,
            "year": int(year),
            "district": str(max_row[district_col]),
            "production": float(max_row[prod_col]),
        }

    def _compute_district_crop_comparison(self, state: str, crop: str, years: int) -> Dict[str, Any]:
        df = self._load_district_crop_production()
        if df is None or df.empty:
            cfg = get_config()
            resource_id = cfg.district_crop_production_resource_id or "not configured"
            return {
                "error": f"District crop production dataset not available. Resource ID: {resource_id}",
                "hint": "The district crop production resource ID may be incorrect or the dataset may not be accessible."
            }

        def find_col(candidates):
            for c in candidates:
                if c in df.columns:
                    return c
            lower_map = {c.lower(): c for c in df.columns}
            for c in candidates:
                if c.lower() in lower_map:
                    return lower_map[c.lower()]
            return None

        state_col = find_col(["State_Name", "STATE", "State", "STATE/UT", "STATE_UT_NAME", "State Name"])
        district_col = find_col(["District_Name", "District", "DISTRICT", "District Name"])
        crop_col = find_col(["Crop", "CROP", "Crop_Name", "Commodity", "CROP_NAME"])
        prod_col = find_col(["Production", "PRODUCTION", "Prodn", "PROD", "production_tonnes", "Prod"])
        year_col = find_col(["Year", "YEAR", "year"])

        if not all([state_col, district_col, crop_col, prod_col, year_col]):
            return {"error": "District dataset schema unexpected", "columns": list(df.columns)}

        # Normalize numeric
        df[prod_col] = pd.to_numeric(df[prod_col], errors="coerce")
        df[year_col] = pd.to_numeric(df[year_col], errors="coerce")

        # Get recent years
        all_years = sorted(df[year_col].dropna().unique())
        recent_years = all_years[-years:] if len(all_years) >= years else all_years

        # Filter by state and crop
        filtered = df[
            df[state_col].astype(str).str.contains(state, case=False, na=False)
            & df[crop_col].astype(str).str.contains(crop, case=False, na=False)
            & df[year_col].isin(recent_years)
        ]

        if filtered.empty:
            return {
                "error": f"No data found for {state}, {crop}",
                "state": state,
                "crop": crop,
                "years": years,
            }

        # Aggregate by district across years (sum production for each district)
        district_totals = (
            filtered.groupby([district_col], as_index=False)[prod_col]
            .sum()
            .sort_values(prod_col, ascending=False)
        )

        # Also get year-by-year breakdown for each district
        district_by_year = (
            filtered.groupby([district_col, year_col], as_index=False)[prod_col]
            .sum()
            .sort_values([district_col, year_col])
        )

        return {
            "state": state,
            "crop": crop,
            "years": int(years),
            "year_range": [int(y) for y in recent_years],
            "districts": district_totals.to_dict(orient="records"),
            "district_by_year": district_by_year.to_dict(orient="records"),
        }


