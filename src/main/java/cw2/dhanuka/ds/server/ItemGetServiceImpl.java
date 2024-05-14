package cw2.dhanuka.ds.server;

import cw2.dhanuka.ds.communication.grpc.generated.ItemGetAllResponse;
import cw2.dhanuka.ds.communication.grpc.generated.ItemGetServiceGrpc;
import cw2.dhanuka.ds.communication.grpc.generated.ItemSellerResponse;
import cw2.dhanuka.ds.communication.grpc.generated.UserRequest;
import io.grpc.stub.StreamObserver;

public class ItemGetServiceImpl extends ItemGetServiceGrpc.ItemGetServiceImplBase {

    private final DataProviderImpl dataProvider;
    public ItemGetServiceImpl(DataProviderImpl dataProvider) {
        this.dataProvider = dataProvider;
    }

    @Override
    public void getSellerItems(UserRequest request, StreamObserver<ItemSellerResponse> responseObserver) {
        System.out.println("getSellerItems request received..");
        String sellerId = request.getUserName();
        ItemSellerResponse responseBuilder = ItemSellerResponse
                .newBuilder()
                .addAllItems(dataProvider.getItemsBySellerName(sellerId))
                .build();
        System.out.println("Response for items for seller " + sellerId);
        responseObserver.onNext(responseBuilder);
        responseObserver.onCompleted();
    }

    @Override
    public void getAllItems(UserRequest request, StreamObserver<ItemGetAllResponse> responseObserver) {
        System.out.println("getAllItems request received..");
        String sellerId = request.getUserName();
        ItemGetAllResponse responseBuilder = ItemGetAllResponse
                .newBuilder()
                .addAllItems(dataProvider.getAllItems(sellerId))
                .build();
        System.out.println("Response for all items");
        responseObserver.onNext(responseBuilder);
        responseObserver.onCompleted();
    }
}
