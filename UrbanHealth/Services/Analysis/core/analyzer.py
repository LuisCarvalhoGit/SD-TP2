import logging

class Analyzer:
    @staticmethod
    def evaluate_risk(sensor_id: str, data_type: str, grpc_readings) -> dict:
        try:
            # Extract raw numeric float values from the gRPC repeated message stream
            values = [reading.value for reading in grpc_readings]
            count = len(values)

            # Handle empty historical query windows
            if count == 0:
                return {
                    "success": True,
                    "sample_count": 0,
                    "mean_value": 0.0,
                    "max_value": 0.0,
                    "min_value": 0.0,
                    "risk_pattern": "No Data",
                    "message": f"No telemetry historical logs found for sensor {sensor_id}."
                }

            # Direct mathematical operations over memory arrays
            mean_val = round(sum(values) / count, 2)
            max_val = round(max(values), 2)
            min_val = round(min(values), 2)

            # Algorithmic business evaluation rules
            risk_pattern = "Stable"
            if data_type in ["PM2", "CO2"] and mean_val > 40.0:
                risk_pattern = "Moderate Risk - Degraded air quality"
            
            if data_type == "TEMP" and max_val > 40.0:
                risk_pattern = "Public Health Risk - Heatwave"

            return {
                "success": True,
                "sample_count": count,
                "mean_value": mean_val,
                "max_value": max_val,
                "min_value": min_val,
                "risk_pattern": risk_pattern,
                "message": f"Stateless analysis engine completed computation for sensor {sensor_id}."
            }

        except Exception as e:
            logging.error(f"[ENGINE ERROR] Failed execution: {e}")
            return {
                "success": False,
                "sample_count": 0,
                "mean_value": 0.0,
                "max_value": 0.0,
                "min_value": 0.0,
                "risk_pattern": "Critical Failure",
                "message": f"Execution processing error: {str(e)}"
            }