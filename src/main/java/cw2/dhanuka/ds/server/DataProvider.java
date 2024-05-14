package cw2.dhanuka.ds.server;

import cw2.dhanuka.ds.communication.grpc.generated.*;

import java.util.List;

public interface DataProvider {
    void addItem(ItemAddRequest itemAddRequest);
    void updateItem(ItemUpdateRequest itemUpdateRequest);
    void deleteItem(String itemId);
    Item getItem(String itemId);

    List<Item> getItemsBySellerName(String name);
    List<ItemDTO> getAllItems(String name);
    boolean isItemExist(String itemId);

    void addReservation(ReserveRequest reserveRequest);
    List<Reservation> getReservationsByUserName(String username);

    void addUser(UserAddRequest userAddRequest);
    boolean isUserExist(String username);
    User getUser(String username);
}
