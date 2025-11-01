from typing import Dict


class QueryPlanner:
    def __init__(self, executor):
        self.executor = executor

    def plan_and_execute(self, parsed: Dict):
        intent = parsed.get("intent")
        if intent == "rainfall_vs_top_crops":
            plan = {
                "intent": intent,
                "steps": [
                    {
                        "type": "compute_rainfall_compare",
                        "state1": parsed["state1"],
                        "state2": parsed["state2"],
                        "years": parsed["years"],
                    },
                    {
                        "type": "compute_top_crops",
                        "state1": parsed["state1"],
                        "state2": parsed["state2"],
                        "top_m": parsed["top_m"],
                        "crop_type": parsed["crop_type"],
                    },
                ],
            }
            return self.executor.execute(plan)

        if intent == "rainfall_compare":
            plan = {
                "intent": intent,
                "steps": [
                    {
                        "type": "compute_rainfall_compare",
                        "state1": parsed["state1"],
                        "state2": parsed["state2"],
                        "years": parsed["years"],
                    }
                ],
            }
            return self.executor.execute(plan)

        if intent == "district_crop_extrema_compare":
            plan = {
                "intent": intent,
                "steps": [
                    {
                        "type": "compute_district_crop_extrema",
                        "state_max": parsed["state_max"],
                        "crop_max": parsed["crop_max"],
                        "state_min": parsed["state_min"],
                        "crop_min": parsed["crop_min"],
                    }
                ],
            }
            return self.executor.execute(plan)

        if intent == "top_crops_state":
            plan = {
                "intent": intent,
                "steps": [
                    {
                        "type": "compute_top_crops_state",
                        "state": parsed["state"],
                        "top_n": parsed["top_n"],
                        "years": parsed["years"],
                    }
                ],
            }
            return self.executor.execute(plan)

        if intent == "district_highest_crop_year":
            plan = {
                "intent": intent,
                "steps": [
                    {
                        "type": "compute_district_highest_crop_year",
                        "state": parsed["state"],
                        "crop": parsed["crop"],
                        "year": parsed["year"],
                    }
                ],
            }
            return self.executor.execute(plan)

        if intent == "district_crop_comparison":
            plan = {
                "intent": intent,
                "steps": [
                    {
                        "type": "compute_district_crop_comparison",
                        "state": parsed["state"],
                        "crop": parsed["crop"],
                        "years": parsed["years"],
                    }
                ],
            }
            return self.executor.execute(plan)

        return self.executor.execute({"intent": intent, "steps": ["demo-step"]})


