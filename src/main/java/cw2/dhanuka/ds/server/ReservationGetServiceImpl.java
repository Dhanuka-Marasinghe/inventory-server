package cw2.dhanuka.ds.server;

import cw2.dhanuka.ds.communication.grpc.generated.ReservationGetServiceGrpc;
import cw2.dhanuka.ds.communication.grpc.generated.ReservationSearchResponse;
import cw2.dhanuka.ds.communication.grpc.generated.UserRequest;
import io.grpc.stub.StreamObserver;

public class ReservationGetServiceImpl extends ReservationGetServiceGrpc.ReservationGetServiceImplBase {

    private final DataProviderImpl dataProvider;
    public ReservationGetServiceImpl(DataProviderImpl dataProvider) {
        this.dataProvider = dataProvider;
    }

    @Override
    public void getReservations(UserRequest request, StreamObserver<ReservationSearchResponse> responseObserver) {
        System.out.println("getReservations request received..");
        String buyerId = request.getUserName();
        ReservationSearchResponse response = ReservationSearchResponse
                .newBuilder()
                .addAllReservations(dataProvider.getReservationsByUserName(buyerId))
                .build();
        System.out.println("Responds for all reservations");
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }
}
