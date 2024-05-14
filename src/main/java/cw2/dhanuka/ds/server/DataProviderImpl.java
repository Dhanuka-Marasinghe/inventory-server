package cw2.dhanuka.ds.server;

import cw2.dhanuka.ds.communication.grpc.generated.*;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class DataProviderImpl implements DataProvider {

    private final ConcurrentHashMap<String, Item> items = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, User> users = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Reservation> reservations = new ConcurrentHashMap<>();

    @Override
    public void addItem(ItemAddRequest itemAddRequest) {
        Item item = Item.newBuilder()
                .setItemId(itemAddRequest.getItemId())
                .setSellerName(itemAddRequest.getSellerName())
                .setItemName(itemAddRequest.getItemName())
                .setType(itemAddRequest.getType())
                .setPrice(itemAddRequest.getPrice())
                .setAvailableQuantity(itemAddRequest.getAvailableQuantity())
                .setReservedQuantity(0)
                .build();
        items.put(item.getItemId(), item);
    }

    @Override
    public void updateItem(ItemUpdateRequest itemUpdateRequest) {
        Item item = items.get(itemUpdateRequest.getItemId());
        item = item.toBuilder()
                .setPrice(itemUpdateRequest.getPrice())
                .setAvailableQuantity(itemUpdateRequest.getAvailableQuantity())
                .build();
        items.put(item.getItemId(), item);
    }

    @Override
    public void deleteItem(String itemId) {
        items.remove(itemId);
    }

    @Override
    public Item getItem(String itemId) {
        return items.get(itemId);
    }
    @Override
    public List<Item> getItemsBySellerName(String name) {
        return items.values().stream().filter(item -> name.equals(item.getSellerName())).collect(Collectors.toList());
    }

    @Override
    public List<ItemDTO> getAllItems(String name) {
        if (!isUserExist(name)) {
            return new ArrayList<>();
        }
        return items.values().stream().map(item -> ItemDTO.newBuilder()
                .setItemId(item.getItemId())
                .setSellerName(item.getSellerName())
                .setItemName(item.getItemName())
                .setType(item.getType())
                .setPrice(item.getPrice())
                .setAvailableQuantity(item.getAvailableQuantity())
                .build()).collect(Collectors.toList());
    }

    @Override
    public boolean isItemExist(String itemId) {
        return items.containsKey(itemId);
    }

    @Override
    public void addReservation(ReserveRequest reserveRequest) {
        Item item = items.get(reserveRequest.getItemId());
        Reservation reservation = Reservation.newBuilder()
                .setReservationId(reserveRequest.getReservationId())
                .setItemId(reserveRequest.getItemId())
                .setItemName(item.getItemName())
                .setBuyerName(reserveRequest.getBuyerName())
                .setQuantity(reserveRequest.getQuantity())
                .setSellerName(item.getSellerName())
                .setPaymentAmount(item.getPrice() * reserveRequest.getQuantity())
                .setReservationDate(reserveRequest.getReservationDate())
                .build();
        reservations.put(reservation.getReservationId(), reservation);
    }

    @Override
    public List<Reservation> getReservationsByUserName(String username) {
        return reservations.values().stream().filter(reservation -> username.equals(reservation.getBuyerName())).collect(Collectors.toList());
    }

    @Override
    public void addUser(UserAddRequest userAddRequest) {
        User user = User.newBuilder()
                .setUserName(userAddRequest.getUserName())
                .setFirstName(userAddRequest.getFirstName())
                .setLastName(userAddRequest.getLastName())
                .setEmail(userAddRequest.getEmail())
                .setRole(userAddRequest.getRole())
                .setAddress(userAddRequest.getAddress())
                .build();
        users.put(user.getUserName(), user);
    }

    @Override
    public boolean isUserExist(String username) {
        return users.containsKey(username);
    }

    @Override
    public User getUser(String username) {
        return users.get(username);
    }

    public void updateItemQuantities(String itemId, int quantity) {
        Item item = items.get(itemId);
        item = item.toBuilder()
                .setAvailableQuantity(item.getAvailableQuantity() - quantity)
                .setReservedQuantity(item.getReservedQuantity() + quantity)
                .build();
        items.put(itemId, item);
    }
}
