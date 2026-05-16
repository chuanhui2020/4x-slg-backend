package io.nexus.actor.grpc;

import com.google.protobuf.ByteString;
import io.grpc.stub.StreamObserver;
import io.nexus.actor.grpc.proto.CommandRequest;
import io.nexus.actor.grpc.proto.CommandResponse;
import io.nexus.actor.grpc.proto.MasterCommandServiceGrpc;
import io.nexus.metrics.GameMetrics;

public class MasterCommandServiceImpl extends MasterCommandServiceGrpc.MasterCommandServiceImplBase {

    private final CommandDispatcher dispatcher;
    private final GameMetrics metrics;

    public MasterCommandServiceImpl(CommandDispatcher dispatcher, GameMetrics metrics) {
        this.dispatcher = dispatcher;
        this.metrics = metrics;
    }

    @Override
    public void execute(CommandRequest request, StreamObserver<CommandResponse> responseObserver) {
        if (metrics != null) metrics.grpcConcurrentRequests().incrementAndGet();
        var sample = (metrics != null) ? io.micrometer.core.instrument.Timer.start() : null;

        dispatcher.dispatch(
                request.getCommandType(),
                request.getPlayerId(),
                request.getRequestId(),
                request.getPayload().toByteArray(),
                request.getSourcePodId(),
                request.getDeadlineMs()
        ).whenComplete((result, ex) -> {
            if (metrics != null) {
                metrics.grpcConcurrentRequests().decrementAndGet();
                if (sample != null) sample.stop(metrics.grpcRequestTimer());
            }

            CommandResponse.Builder builder = CommandResponse.newBuilder()
                    .setRequestId(request.getRequestId());

            if (ex != null) {
                if (metrics != null) metrics.grpcRequestErrorCounter().increment();
                builder.setSuccess(false)
                        .setErrorCode("INTERNAL_ERROR")
                        .setErrorMessage(ex.getMessage());
            } else if (result.success()) {
                builder.setSuccess(true);
                if (result.payload() != null) {
                    builder.setPayload(ByteString.copyFrom(result.payload()));
                }
            } else {
                if (metrics != null) metrics.grpcRequestErrorCounter().increment();
                builder.setSuccess(false)
                        .setErrorCode(result.errorCode())
                        .setErrorMessage(result.errorMessage());
            }

            responseObserver.onNext(builder.build());
            responseObserver.onCompleted();
        });
    }
}
