import grpc
from concurrent import futures
import logging

# Ficheiros que vão ser gerados pelo compilador
import preprocess_pb2
import preprocess_pb2_grpc

from core.data_cleaner import DataCleaner

# Nota que a classe agora implementa o Servicer gerado
class PreProcessingServicer(preprocess_pb2_grpc.PreProcessingServiceServicer):
    
    # O nome do método tem de ser exatamente igual ao do RPC no .proto
    def ProcessData(self, request, context):
        logging.info(f"Recebido pedido: Sensor={request.sensor_id}, Tipo={request.data_type}, Valor={request.raw_value}")
        
        # Passar os dados para a lógica de limpeza
        success, proc_value, msg = DataCleaner.normalize(
            request.sensor_id, 
            request.data_type, 
            request.raw_value
        )
        
        # Devolver a estrutura DataResponse exata do .proto
        return preprocess_pb2.DataResponse(
            success=success,
            processed_value=proc_value,
            message=msg
        )

def serve():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    preprocess_pb2_grpc.add_PreProcessingServiceServicer_to_server(PreProcessingServicer(), server)
    
    server.add_insecure_port('[::]:50051')
    logging.info("Serviço de Pré-processamento (RPC) iniciado na porta 50051...")
    
    server.start()
    server.wait_for_termination()

if __name__ == '__main__':
    serve()