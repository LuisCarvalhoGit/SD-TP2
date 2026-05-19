import grpc
from concurrent import futures
import logging

# Ficheiros gerados a partir do analysis.proto
import analysis_pb2
import analysis_pb2_grpc

from core.analyzer import Analyzer

class AnalysisServicer(analysis_pb2_grpc.AnalysisServiceServicer):
    def AnalyzeData(self, request, context):
        logging.info(f"[RPC] Compute analytics requested for Sensor={request.sensor_id}, Type={request.data_type}")
        
        # Invoke stateless processing loop passing the collection directly
        result = Analyzer.evaluate_risk(
            request.sensor_id, 
            request.data_type, 
            request.readings
        )
        
        return analysis_pb2.AnalysisResponse(
            success=result["success"],
            sample_count=result["sample_count"],
            mean_value=result["mean_value"],
            max_value=result["max_value"],
            min_value=result["min_value"],
            risk_pattern=result["risk_pattern"],
            message=result["message"]
        )

def serve():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    analysis_pb2_grpc.add_AnalysisServiceServicer_to_server(AnalysisServicer(), server)
    
    # Usar a porta 50052 para o microserviço de análise
    server.add_insecure_port('[::]:50052')
    logging.info("Serviço de Análise e Previsão (RPC) iniciado na porta 50052...")
    
    server.start()
    server.wait_for_termination()

if __name__ == '__main__':
    serve()