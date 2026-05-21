import grpc
from concurrent import futures
import logging
import signal
import sys
import time
from typing import Tuple

sys.path.insert(0, '/app')

import preprocess_pb2
import preprocess_pb2_grpc
from core.data_cleaner import DataCleaner
from shared.health import HealthMetrics
from shared.logging_config import setup_logging

logger = setup_logging("PreProcessing Service")
metrics = HealthMetrics()
server_instance = None
shutdown_event = False

class PreProcessingServicer(preprocess_pb2_grpc.PreProcessingServiceServicer):
    def ProcessData(self, request, context):
        global metrics
        start_time = time.time()
        metrics.increment_request()

        try:
            logger.info(f"[RPC] Processing Sensor={request.sensor_id}, Type={request.data_type}, Value={request.raw_value}")

            success, proc_value, msg = DataCleaner.normalize(
                request.sensor_id,
                request.data_type,
                request.raw_value
            )

            if not success:
                metrics.increment_error()
                logger.warning(f"[RPC REJECT] {request.sensor_id}/{request.data_type}: {msg}")

            elapsed = round((time.time() - start_time) * 1000, 2)
            logger.info(f"[RPC DONE] {request.sensor_id} processed in {elapsed}ms, success={success}")

            return preprocess_pb2.DataResponse(
                success=success,
                processed_value=proc_value,
                message=msg
            )

        except Exception as e:
            metrics.increment_error()
            logger.error(f"[RPC FAULT] {request.sensor_id}: {str(e)}")
            return preprocess_pb2.DataResponse(
                success=False,
                processed_value=0.0,
                message=f"Processing error: {str(e)}"
            )

class HealthServicer(preprocess_pb2_grpc.HealthServiceServicer):
    def Check(self, request, context):
        status = metrics.get_status()
        logger.debug(f"[HEALTH CHECK] Status: {status}")
        return preprocess_pb2.HealthCheckResponse(
            status=status["status"],
            uptime_seconds=status["uptime_seconds"],
            requests_processed=status["requests_processed"]
        )

def handle_shutdown(signum, frame):
    global server_instance, shutdown_event
    logger.info("[SHUTDOWN] Graceful shutdown initiated (SIGTERM/SIGINT)")
    shutdown_event = True
    if server_instance:
        server_instance.stop(grace=5)
        logger.info("[SHUTDOWN] gRPC server stopped")
    sys.exit(0)

def serve():
    global server_instance
    signal.signal(signal.SIGTERM, handle_shutdown)
    signal.signal(signal.SIGINT, handle_shutdown)

    server_instance = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    preprocess_pb2_grpc.add_PreProcessingServiceServicer_to_server(PreProcessingServicer(), server_instance)
    preprocess_pb2_grpc.add_HealthServiceServicer_to_server(HealthServicer(), server_instance)

    server_instance.add_insecure_port('[::]:50051')
    logger.info("[STARTUP] PreProcessing Service started on port 50051")

    server_instance.start()
    server_instance.wait_for_termination()

if __name__ == '__main__':
    serve()
