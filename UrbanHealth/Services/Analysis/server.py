import grpc
from concurrent import futures
import logging
import signal
import sys
import time

sys.path.insert(0, '/app')

import analysis_pb2
import analysis_pb2_grpc
from core.analyzer import Analyzer
from shared.health import HealthMetrics
from shared.logging_config import setup_logging

logger = setup_logging("Analysis Service")
metrics = HealthMetrics()
server_instance = None

class AnalysisServicer(analysis_pb2_grpc.AnalysisServiceServicer):
    def AnalyzeData(self, request, context):
        global metrics
        start_time = time.time()
        metrics.increment_request()

        try:
            logger.info(f"[RPC] Analysis requested: Sensor={request.sensor_id}, Type={request.data_type}, Samples={len(request.readings)}")

            result = Analyzer.evaluate_risk(
                request.sensor_id,
                request.data_type,
                request.readings
            )

            if not result["success"]:
                metrics.increment_error()
                logger.warning(f"[RPC FAILED] {request.sensor_id}: {result['risk_pattern']}")
            else:
                logger.info(f"[RPC SUCCESS] {request.sensor_id}: {result['risk_pattern']}")

            elapsed = round((time.time() - start_time) * 1000, 2)
            logger.debug(f"[RPC TIMING] Analysis took {elapsed}ms")

            return analysis_pb2.AnalysisResponse(
                success=result["success"],
                sample_count=result["sample_count"],
                mean_value=result["mean_value"],
                max_value=result["max_value"],
                min_value=result["min_value"],
                risk_pattern=result["risk_pattern"],
                message=result["message"]
            )

        except Exception as e:
            metrics.increment_error()
            logger.error(f"[RPC FAULT] {request.sensor_id}: {str(e)}")
            return analysis_pb2.AnalysisResponse(
                success=False,
                sample_count=0,
                mean_value=0.0,
                max_value=0.0,
                min_value=0.0,
                risk_pattern="Critical Error",
                message=f"Service error: {str(e)}"
            )

class HealthServicer(analysis_pb2_grpc.HealthServiceServicer):
    def Check(self, request, context):
        status = metrics.get_status()
        logger.debug(f"[HEALTH CHECK] Status: {status}")
        return analysis_pb2.HealthCheckResponse(
            status=status["status"],
            uptime_seconds=status["uptime_seconds"],
            requests_processed=status["requests_processed"]
        )

def handle_shutdown(signum, frame):
    global server_instance
    logger.info("[SHUTDOWN] Graceful shutdown initiated (SIGTERM/SIGINT)")
    if server_instance:
        server_instance.stop(grace=5)
        logger.info("[SHUTDOWN] gRPC server stopped")
    sys.exit(0)

def serve():
    global server_instance
    signal.signal(signal.SIGTERM, handle_shutdown)
    signal.signal(signal.SIGINT, handle_shutdown)

    server_instance = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    analysis_pb2_grpc.add_AnalysisServiceServicer_to_server(AnalysisServicer(), server_instance)
    analysis_pb2_grpc.add_HealthServiceServicer_to_server(HealthServicer(), server_instance)

    server_instance.add_insecure_port('[::]:50052')
    logger.info("[STARTUP] Analysis Service started on port 50052")

    server_instance.start()
    server_instance.wait_for_termination()

if __name__ == '__main__':
    serve()
