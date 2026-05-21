import logging
from typing import Tuple

logger = logging.getLogger(__name__)

class DataCleaner:
    @staticmethod
    def normalize(sensor_id: str, data_type: str, raw_value: float) -> Tuple[bool, float, str]:
        try:
            if sensor_id is None or not isinstance(sensor_id, str) or not sensor_id.strip():
                logger.warning(f"[VALIDATION] Invalid sensor_id: {sensor_id}")
                return False, 0.0, "Invalid sensor ID."

            if data_type is None or not isinstance(data_type, str):
                logger.warning(f"[VALIDATION] Invalid data_type: {data_type}")
                return False, 0.0, "Invalid data type."

            if raw_value is None:
                logger.warning(f"[VALIDATION] Null value for {sensor_id}/{data_type}")
                return False, 0.0, "Null value received."

            if not isinstance(raw_value, (int, float)):
                try:
                    raw_value = float(raw_value)
                except (TypeError, ValueError):
                    logger.warning(f"[VALIDATION] Cannot parse value: {raw_value}")
                    return False, 0.0, "Value is not numeric."

            if data_type == "TEMP":
                if raw_value < -20 or raw_value > 60:
                    logger.info(f"[REJECT] TEMP={raw_value} out of bounds for {sensor_id}")
                    return False, 0.0, f"Temperature {raw_value}°C exceeds valid range."
                return True, round(raw_value, 2), "OK"

            elif data_type == "HUM":
                if raw_value < 0 or raw_value > 100:
                    logger.info(f"[REJECT] HUM={raw_value} out of bounds for {sensor_id}")
                    return False, 0.0, "Humidity must be 0-100%."
                return True, round(raw_value, 1), "OK"

            elif data_type in ["PM2", "CO2", "NOISE", "UV"]:
                if raw_value < 0:
                    logger.info(f"[REJECT] {data_type}={raw_value} negative for {sensor_id}")
                    return False, 0.0, f"{data_type} value cannot be negative."
                return True, round(raw_value, 2), "OK"

            logger.debug(f"[PASS-THROUGH] Unknown type {data_type} for {sensor_id}")
            return True, raw_value, "Type not validated by strict rules."

        except Exception as e:
            logger.error(f"[CLEANER ERROR] Unexpected error: {e}")
            return False, 0.0, f"Processing error: {str(e)}"
