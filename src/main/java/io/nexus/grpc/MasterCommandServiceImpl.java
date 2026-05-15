package io.nexus.actor.grpc;

import com.google.protobuf.ByteString;
import io.grpc.stub.StreamObserver;
import io.nexus.actor.grpc.proto.CommandRequest;
import io.nexus.actor.grpc.proto.CommandResponse;
import io.nexus.actor.grpc.proto.MasterCommandServiceGrpc;

public class MasterCommandServiceImpl extends MasterCommandServiceGrpc.MasterCommandServiceImplBase {

    private final CommandDispatcher dispatcher;

    public MasterCommandServiceImpl(CommandDispatcher dispatcher) {
        this.dispatcher = dispatcher;
    }

    @Override
    public void execute(CommandRequest request, StreamObserver<CommandResponse> responseObserver) {
        dispatcher.dispatch(
                request.getCommandType(),
                request.getPlayerId(),
                request.getRequestId(),
                request.getPayload().toByteArray(),
                request.getSourcePodId(),
                request.getDeadlineMs()
        ).whenComplete((result, ex) -> {
            CommandResponse.Builder builder = CommandResponse.newBuilder()
                    .setRequestId(request.getRequestId());

            if (ex != null) {
                builder.setSuccess(false)
                        .setErrorCode("INTERNAL_ERROR")
                        .setErrorMessage(ex.getMessage());
            } else if (result.success()) {
                builder.setSuccess(true);
                if (result.payload() != null) {
                    builder.setPayload(ByteString.copyFrom(result.payload()));
                }
            } else {
                builder.setSuccess(false)
                        .setErrorCode(result.errorCode())
                        .setErrorMessage(result.errorMessage());
            }

            responseObserver.onNext(builder.build());
            responseObserver.onCompleted();
        });
    }
}
