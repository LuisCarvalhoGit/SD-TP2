import logging
import time
from typing import Dict, Any

logger = logging.getLogger(__name__)

class Analyzer:
    @staticmethod
    def evaluate_risk(sensor_id: str, data_type: str, grpc_readings) -> Dict[str, Any]:
        try:
            logger.info(f"[ANALYSIS START] Sensor={sensor_id}, Type={data_type}")

            if sensor_id is None or not isinstance(sensor_id, str):
                logger.warning(f"[VALIDATION] Invalid sensor_id: {sensor_id}")
                return {
                    "success": False,
                    "sample_count": 0,
                    "mean_value": 0.0,
                    "max_value": 0.0,
                    "min_value": 0.0,
                    "risk_pattern": "Invalid Input",
                    "message": "Invalid sensor ID provided."
                }

            if data_type is None or not isinstance(data_type, str):
                logger.warning(f"[VALIDATION] Invalid data_type: {data_type}")
                return {
                    "success": False,
                    "sample_count": 0,
                    "mean_value": 0.0,
                    "max_value": 0.0,
                    "min_value": 0.0,
                    "risk_pattern": "Invalid Input",
                    "message": "Invalid data type provided."
                }

            values = [float(reading.value) for reading in grpc_readings if reading.value is not None]
            count = len(values)

            if count == 0:
                logger.info(f"[NO DATA] No readings found for {sensor_id}/{data_type}")
                return {
                    "success": True,
                    "sample_count": 0,
                    "mean_value": 0.0,
                    "max_value": 0.0,
                    "min_value": 0.0,
                    "risk_pattern": "No Data Available",
                    "message": f"No historical telemetry found for {sensor_id}/{data_type}."
                }

            mean_val = round(sum(values) / count, 2)
            max_val = round(max(values), 2)
            min_val = round(min(values), 2)

            risk_pattern = "Normal"
            if data_type in ["PM2", "CO2"]:
                if mean_val > 60:
                    risk_pattern = "High Risk - Poor air quality detected"
                elif mean_val > 40:
                    risk_pattern = "Moderate Risk - Air quality degrading"
                elif mean_val > 20:
                    risk_pattern = "Caution - Monitor air quality"

            elif data_type == "TEMP":
                if max_val > 40:
                    risk_pattern = "Critical - Heatwave detected"
                elif max_val > 35:
                    risk_pattern = "High Risk - Elevated temperatures"
                elif max_val > 28:
                    risk_pattern = "Moderate - Warm conditions"

            elif data_type == "HUM":
                if mean_val > 80 or mean_val < 30:
                    risk_pattern = "Moderate Risk - Humidity out of comfort zone"

            elif data_type == "NOISE":
                if max_val > 80:
                    risk_pattern = "High Risk - Noise pollution detected"

            elif data_type == "UV":
                if max_val > 8:
                    risk_pattern = "High Risk - Strong UV exposure"

            logger.info(f"[ANALYSIS DONE] {sensor_id}: pattern={risk_pattern}, samples={count}, mean={mean_val}")

            return {
                "success": True,
                "sample_count": count,
                "mean_value": mean_val,
                "max_value": max_val,
                "min_value": min_val,
                "risk_pattern": risk_pattern,
                "message": f"Analysis completed for {sensor_id} with {count} samples."
            }

        except ValueError as e:
            logger.error(f"[VALUE ERROR] Failed to parse readings: {e}")
            return {
                "success": False,
                "sample_count": 0,
                "mean_value": 0.0,
                "max_value": 0.0,
                "min_value": 0.0,
                "risk_pattern": "Processing Error",
                "message": f"Invalid data format: {str(e)}"
            }

        except Exception as e:
            logger.error(f"[ENGINE ERROR] Unexpected failure: {e}")
            return {
                "success": False,
                "sample_count": 0,
                "mean_value": 0.0,
                "max_value": 0.0,
                "min_value": 0.0,
                "risk_pattern": "Critical Failure",
                "message": f"Execution error: {str(e)}"
            }
