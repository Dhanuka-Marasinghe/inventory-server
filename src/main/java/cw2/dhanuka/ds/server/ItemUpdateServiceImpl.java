package cw2.dhanuka.ds.server;

import cw2.dhanuka.ds.synchronization.DistributedTxCoordinator;
import cw2.dhanuka.ds.synchronization.DistributedTxListener;
import cw2.dhanuka.ds.synchronization.DistributedTxParticipant;
import cw2.dhanuka.ds.communication.grpc.generated.*;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import org.apache.zookeeper.KeeperException;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.List;
import java.util.UUID;

public class ItemUpdateServiceImpl extends ItemUpdateServiceGrpc.ItemUpdateServiceImplBase implements DistributedTxListener {

    ItemUpdateServiceGrpc.ItemUpdateServiceBlockingStub clientStub = null;
    private ManagedChannel channel = null;
    private final ReservationServer server;
    private final DataProviderImpl dataProvider;
    boolean status = false;
    private String statusMessage = "";
    private AbstractMap.SimpleEntry<String, ItemUpdateRequest> tempDataHolder;
    public ItemUpdateServiceImpl(ReservationServer reservationServer, DataProviderImpl dataProvider) {
        this.server = reservationServer;
        this.dataProvider = dataProvider;
    }

    private ItemUpdateResponse callServer(ItemUpdateRequest itemUpdateRequest, boolean isSentByPrimary,
                                      String IPAddress, int port) {
        System.out.println("Call Server " + IPAddress + ":" + port);
        channel = ManagedChannelBuilder.forAddress(IPAddress, port)
                .usePlaintext()
                .build();
        clientStub = ItemUpdateServiceGrpc.newBlockingStub(channel);
        ItemUpdateRequest request = itemUpdateRequest.toBuilder()
                .setIsSentByPrimary(isSentByPrimary)
                .build();

        return clientStub.updateItem(request);
    }

    private ItemUpdateResponse callPrimary(ItemUpdateRequest itemUpdateRequest) {
        System.out.println("Calling Primary server");
        String[] currentLeaderData = server.getCurrentLeaderData();
        String IPAddress = currentLeaderData[0];
        int port = Integer.parseInt(currentLeaderData[1]);
        return callServer(itemUpdateRequest, false, IPAddress, port);
    }
    private void updateSecondaryServers(ItemUpdateRequest itemUpdateRequest) throws KeeperException, InterruptedException {
        System.out.println("Updating secondary servers");
        List<String[]> othersData = server.getOthersData();
        for (String[] data : othersData) {
            String IPAddress = data[0];
            int port = Integer.parseInt(data[1]);
            callServer(itemUpdateRequest, true, IPAddress, port);
        }
    }

    private void startDistributedTx(String itemId, ItemUpdateRequest itemUpdateRequest) {
        try {
            server.getTransactionItemUpdate().start("update"+itemId, String.valueOf(UUID.randomUUID()));
            tempDataHolder = new AbstractMap.SimpleEntry<>(itemId, itemUpdateRequest);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void onGlobalCommit() {
        updateItem();
    }

    @Override
    public void onGlobalAbort() {
        tempDataHolder = null;
        status = false;
        System.out.println("Transaction Aborted by the Coordinator");
    }

    @Override
    public synchronized void updateItem(ItemUpdateRequest request, StreamObserver<ItemUpdateResponse> responseObserver) {
        if (server.isLeader()) {
            try {
                System.out.println("Updating Item as Primary");
                startDistributedTx(request.getItemId(), request);
                updateSecondaryServers(request);
                System.out.println("going to perform");
                if (checkEligibility(request)){
                    ((DistributedTxCoordinator) server.getTransactionItemUpdate()).perform();
                } else {
                    ((DistributedTxCoordinator) server.getTransactionItemUpdate()).sendGlobalAbort();
                }
            } catch (Exception e) {
                System.out.println("Error while updating a new item" + e.getMessage());
                e.printStackTrace();
            }
        } else {
            if (request.getIsSentByPrimary()) {
                System.out.println("Updating a new item on secondary, on Primary's command");
                startDistributedTx(request.getItemId(), request);
                if (checkEligibility(request)) {
                    ((DistributedTxParticipant) server.getTransactionItemUpdate()).voteCommit();
                } else {
                    ((DistributedTxParticipant) server.getTransactionItemUpdate()).voteAbort();
                }
            } else {
                ItemUpdateResponse response = callPrimary(request);
                if (response.getStatus()) {
                    status = true;
                }
            }
        }
        ItemUpdateResponse response = ItemUpdateResponse
                .newBuilder()
                .setStatus(status)
                .setMessage(statusMessage)
                .build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    private boolean checkEligibility(ItemUpdateRequest request) {
        if (!dataProvider.isUserExist(request.getSellerName())) {
            statusMessage = "The specified seller does not exist";
            status = false;
            return false;
        }
        if (!dataProvider.isItemExist(request.getItemId())) {
            statusMessage = "The specified item does not exist";
            status = false;
            return false;
        }
        return true;
    }

    private void updateItem() {
        if (tempDataHolder != null) {
            ItemUpdateRequest request = tempDataHolder.getValue();
            dataProvider.updateItem(request);
            System.out.println("Item " + request.getItemId() + " Updated & committed");
            status = true;
            statusMessage = "Item has been successfully updated.";
            tempDataHolder = null;
        }
    }
}
