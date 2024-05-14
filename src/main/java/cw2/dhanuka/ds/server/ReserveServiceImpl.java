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

public class ReserveServiceImpl extends ReserveServiceGrpc.ReserveServiceImplBase implements DistributedTxListener {

    ReserveServiceGrpc.ReserveServiceBlockingStub clientStub = null;
    private ManagedChannel channel = null;
    private final ReservationServer server;
    private final DataProviderImpl dataProvider;
    private boolean status = false;
    private String statusMessage = "";
    private AbstractMap.SimpleEntry<String, ReserveRequest> tempDataHolder;
    public ReserveServiceImpl(ReservationServer reservationServer, DataProviderImpl dataProvider) {
        this.server = reservationServer;
        this.dataProvider = dataProvider;
    }

    private ReserveResponse callServer(ReserveRequest reserveRequest, boolean isSentByPrimary,
                                      String IPAddress, int port) {
        System.out.println("Call Server " + IPAddress + ":" + port);
        channel = ManagedChannelBuilder.forAddress(IPAddress, port)
                .usePlaintext()
                .build();
        clientStub = ReserveServiceGrpc.newBlockingStub(channel);
        ReserveRequest request = reserveRequest.toBuilder()
                .setIsSentByPrimary(isSentByPrimary)
                .build();
        return clientStub.reserveItem(request);
    }

    private ReserveResponse callPrimary(ReserveRequest reserveRequest) {
        System.out.println("Calling Primary server");
        String[] currentLeaderData = server.getCurrentLeaderData();
        String IPAddress = currentLeaderData[0];
        int port = Integer.parseInt(currentLeaderData[1]);
        return callServer(reserveRequest, false, IPAddress, port);
    }
    private void updateSecondaryServers(ReserveRequest reserveRequest) throws KeeperException, InterruptedException {
        System.out.println("Updating secondary servers");
        List<String[]> othersData = server.getOthersData();
        for (String[] data : othersData) {
            String IPAddress = data[0];
            int port = Integer.parseInt(data[1]);
            callServer(reserveRequest, true, IPAddress, port);
        }
    }

    private void startDistributedTx(String reservationId, ReserveRequest reserveRequest) {
        try {
            server.getTransactionReserve().start(reservationId, String.valueOf(UUID.randomUUID()));
            tempDataHolder = new AbstractMap.SimpleEntry<>(reservationId, reserveRequest);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void onGlobalCommit() {
        createReservation();
    }

    @Override
    public void onGlobalAbort() {
        tempDataHolder = null;
        status = false;
        System.out.println("Transaction Aborted by the Coordinator");
    }

    @Override
    public synchronized void reserveItem(ReserveRequest request, StreamObserver<ReserveResponse> responseObserver) {
        if (server.isLeader()) {
            // Act as primary
            try {
                System.out.println("Creating a Reservation as Primary");
                startDistributedTx(request.getReservationId(), request);
                updateSecondaryServers(request);
                System.out.println("going to perform");
                if (checkEligibility(request)){
                    ((DistributedTxCoordinator) server.getTransactionReserve()).perform();
                } else {
                    ((DistributedTxCoordinator) server.getTransactionReserve()).sendGlobalAbort();
                }
            } catch (Exception e) {
                System.out.println("Error while creating a reservation" + e.getMessage());
                e.printStackTrace();
            }
        } else {
            // Act As Secondary
            if (request.getIsSentByPrimary()) {
                System.out.println("Creating a new reservation on secondary, on Primary's command");
                startDistributedTx(request.getReservationId(), request);
                if (checkEligibility(request)) {
                    ((DistributedTxParticipant) server.getTransactionReserve()).voteCommit();
                } else {
                    ((DistributedTxParticipant) server.getTransactionReserve()).voteAbort();
                }
            } else {
                ReserveResponse response = callPrimary(request);
                if (response.getStatus()) {
                    status = true;
                }
            }
        }
        ReserveResponse response = ReserveResponse
                .newBuilder()
                .setStatus(status)
                .setMessage(statusMessage)
                .build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    private boolean checkEligibility(ReserveRequest request) {
        if (!dataProvider.isUserExist(request.getBuyerName())) {
            statusMessage = "Specified buyer does not exist";
            status = false;
            return false;
        }
        User buyer = dataProvider.getUser(request.getBuyerName());
        if (buyer.getRole() != RoleType.BUYER) {
            statusMessage = "User's role conflicts with the requested action.";
            status = false;
            return false;
        }
        if (!dataProvider.isItemExist(request.getItemId())) {
            statusMessage = "Specified item does not exist";
            status = false;
            return false;
        }
        if (dataProvider.getItem(request.getItemId()).getAvailableQuantity() < request.getQuantity()) {
            statusMessage = "Insufficient quantity available.";
            status = false;
            return false;
        }
        return true;
    }

    private void createReservation() {
        if (tempDataHolder != null) {
            ReserveRequest request = tempDataHolder.getValue();
            dataProvider.updateItemQuantities(request.getItemId(), request.getQuantity());
            dataProvider.addReservation(request);
            System.out.println("Reservation for " + request.getItemId() + " created & committed");
            status = true;
            statusMessage = "Reservation has been successfully created";
            tempDataHolder = null;
        }
    }
}
