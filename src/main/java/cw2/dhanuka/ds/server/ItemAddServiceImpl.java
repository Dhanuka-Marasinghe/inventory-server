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

public class ItemAddServiceImpl extends ItemAddServiceGrpc.ItemAddServiceImplBase implements DistributedTxListener {
    ItemAddServiceGrpc.ItemAddServiceBlockingStub clientStub = null;
    private ManagedChannel channel = null;
    private final ReservationServer server;
    private final DataProviderImpl dataProvider;
    private boolean status = false;
    private String statusMessage = "";
    private AbstractMap.SimpleEntry<String, ItemAddRequest> tempDataHolder;
    public ItemAddServiceImpl(ReservationServer reservationServer, DataProviderImpl dataProvider) {
        this.server = reservationServer;
        this.dataProvider = dataProvider;
    }

    private ItemAddResponse callServer(ItemAddRequest itemAddRequest, boolean isSentByPrimary,
                                      String IPAddress, int port) {
        System.out.println("Call Server " + IPAddress + ":" + port);
        channel = ManagedChannelBuilder.forAddress(IPAddress, port)
                .usePlaintext()
                .build();
        clientStub = ItemAddServiceGrpc.newBlockingStub(channel);
        ItemAddRequest request = itemAddRequest.toBuilder()
                .setIsSentByPrimary(isSentByPrimary)
                .build();
        return clientStub.addItem(request);
    }

    private ItemAddResponse callPrimary(ItemAddRequest itemAddRequest) {
        System.out.println("Calling Primary server");
        String[] currentLeaderData = server.getCurrentLeaderData();
        String IPAddress = currentLeaderData[0];
        int port = Integer.parseInt(currentLeaderData[1]);
        return callServer(itemAddRequest, false, IPAddress, port);
    }
    private void updateSecondaryServers(ItemAddRequest itemAddRequest) throws KeeperException, InterruptedException {
        System.out.println("Updating secondary servers");
        List<String[]> othersData = server.getOthersData();
        for (String[] data : othersData) {
            String IPAddress = data[0];
            int port = Integer.parseInt(data[1]);
            callServer(itemAddRequest, true, IPAddress, port);
        }
    }

    private void startDistributedTx(String itemId, ItemAddRequest itemAddRequest) {
        try {
            server.getTransactionItemAdd().start(itemId, String.valueOf(UUID.randomUUID()));
            tempDataHolder = new AbstractMap.SimpleEntry<>(itemId, itemAddRequest);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void onGlobalCommit() {
        persistItem();
    }

    @Override
    public void onGlobalAbort() {
        tempDataHolder = null;
        status = false;
        System.out.println("Transaction Aborted by the Coordinator");
    }

    @Override
    public synchronized void addItem(ItemAddRequest request, StreamObserver<ItemAddResponse> responseObserver) {
        if (server.isLeader()) {
            try {
                System.out.println("Adding Item as Primary");
                startDistributedTx(request.getItemId(), request);
                updateSecondaryServers(request);
                System.out.println("going to perform");
                if (checkEligibility(request)){
                    ((DistributedTxCoordinator) server.getTransactionItemAdd()).perform();
                } else {
                    ((DistributedTxCoordinator) server.getTransactionItemAdd()).sendGlobalAbort();
                }
            } catch (Exception e) {
                System.out.println("Error while adding a new item" + e.getMessage());
                e.printStackTrace();
            }
        } else {
            if (request.getIsSentByPrimary()) {
                System.out.println("Adding new item on secondary, on Primary's command");
                startDistributedTx(request.getItemId(), request);
                if (checkEligibility(request)) {
                    ((DistributedTxParticipant) server.getTransactionItemAdd()).voteCommit();
                } else {
                    ((DistributedTxParticipant) server.getTransactionItemAdd()).voteAbort();
                }
            } else {
                ItemAddResponse response = callPrimary(request);
                if (response.getStatus()) {
                    status = true;
                }
            }
        }
        ItemAddResponse response = ItemAddResponse
                .newBuilder()
                .setStatus(status)
                .setMessage(statusMessage)
                .build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    private boolean checkEligibility(ItemAddRequest request) {
        if (!dataProvider.isUserExist(request.getSellerName())) {
            statusMessage = "The specified seller does not exist";
            status = false;
            return false;
        }
        User seller = dataProvider.getUser(request.getSellerName());
        if ((seller.getRole() == RoleType.INVENTORY_SYSTEM_CLERK && request.getType() != Type.NEW_ARRIVAL)
        || (seller.getRole() != RoleType.INVENTORY_SYSTEM_CLERK && request.getType() == Type.NEW_ARRIVAL)) {
            statusMessage = "User's role conflicts with the requested action";
            status = false;
            return false;
        }
        return true;
    }

    private void persistItem() {
        if (tempDataHolder != null) {
            ItemAddRequest request = tempDataHolder.getValue();
            dataProvider.addItem(request);
            System.out.println("Item " + request.getItemName() + " of type " + request.getType() + " Added & committed");
            status = false;
            statusMessage = "Item has been successfully added";
            tempDataHolder = null;
        }
    }
}
