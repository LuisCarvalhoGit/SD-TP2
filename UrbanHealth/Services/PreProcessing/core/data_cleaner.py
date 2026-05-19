class DataCleaner:
    @staticmethod
    def normalize(sensor_id: str, data_type: str, raw_value: float) -> tuple[bool, float, str]:
        """
        Valida e normaliza os dados do sensor.
        Retorna: (success, processed_value, message)
        """
        # Regras de negócio para as métricas da One Health
        if data_type == "TEMP":
            if raw_value < -20 or raw_value > 60:
                return False, 0.0, f"Temperatura {raw_value} fora dos limites."
            return True, round(raw_value, 2), "OK"
            
        elif data_type == "HUM":
            if raw_value < 0 or raw_value > 100:
                return False, 0.0, "Humidade deve estar entre 0% e 100%."
            return True, round(raw_value, 1), "OK"
            
        elif data_type in ["PM2", "CO2", "NOISE", "UV"]:
            if raw_value < 0:
                return False, 0.0, "O valor não pode ser negativo."
            return True, round(raw_value, 2), "OK"

        # Se for um tipo desconhecido, deixa passar mas avisa
        return True, raw_value, "Tipo de dado não reconhecido pelas regras estritas."