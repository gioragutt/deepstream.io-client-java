package io.deepstream.testapp;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;

import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class Subscriber {
    public static void main(String[] args) throws InvalidDeepstreamConfig {
        new SubscriberApplication();
    }

    static class SubscriberApplication {

        SubscriberApplication() throws InvalidDeepstreamConfig {
            try {
                Map config = new HashMap<String, Object>();
                config.put(ConfigOptions.SUBSCRIPTION_TIMEOUT.toString(), 500);
                config.put(ConfigOptions.RECORD_READ_ACK_TIMEOUT.toString(), 500);
                config.put(ConfigOptions.RECORD_READ_TIMEOUT.toString(), 500);

                DeepstreamClient client = new DeepstreamClient("localhost:6020", config);
                subscribeConnectionChanges(client);
                subscribeRuntimeErrors(client);

                LoginResult loginResult = client.login();
                if (!loginResult.loggedIn()) {
                    System.err.println("Failed to login " + loginResult.getErrorEvent());
                } else {
                    System.out.println("Login Success");
                    subscribeEvent(client);
                    makeSnapshot(client, "record/snapshot");
                    hasRecord(client);
                    subscribeRecord(client, "record/b");
                    subscribeAnonymousRecord(client);
                    subscribeList(client);
                    makeRpc(client);
                    subscribePresence(client);
                    queryClients(client);
                }

            } catch (Exception e) {
                e.printStackTrace();
            }

        }

        private void hasRecord(final DeepstreamClient client) {
            HasResult hasResult = client.record.has("record/has");
            if (hasResult.hasError()) {
                System.err.println(String.format("Has did not work because: %s", hasResult.getError().getMessage()));
            } else {
                System.out.println(String.format("Has result: %s", hasResult.getResult()));
            }
        }

        private void makeSnapshot(final DeepstreamClient client, final String recordName) {
            SnapshotResult snapshotResult = client.record.snapshot(recordName);
            if (snapshotResult.hasError()) {
                System.err.println(String.format("Snapshot did not work because: %s", snapshotResult.getError().getMessage()));
            } else {
                System.out.println(String.format("Snapshot result: %s", snapshotResult.getData()));
            }
        }

        private void makeRpc(final DeepstreamClient client) {
            ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();
            executorService.scheduleAtFixedRate(new Runnable() {
                @Override
                public void run() {
                    RpcResult rpcResponse = client.rpc.make("add-numbers", new Double[]{Math.floor(Math.random() * 10), Math.floor(Math.random() * 10)});
                    if (rpcResponse.success() == false) {
                        System.out.println(String.format("RPC failed with data: %s", rpcResponse.getData()));
                    } else {
                        System.out.println(String.format("RPC success with data: %s", rpcResponse.getData()));
                    }
                }
            }, 1, 5, TimeUnit.SECONDS);
        }

        private void subscribeRuntimeErrors(DeepstreamClient client) {
            client.setRuntimeErrorHandler(new DeepstreamRuntimeErrorHandler() {
                @Override
                public void onException(Topic topic, Event event, String errorMessage) {
                    System.out.println(String.format("Error occured %s %s %s", topic, event, errorMessage));
                }
            });
        }

        private void subscribeConnectionChanges(DeepstreamClient client) {
            client.addConnectionChangeListener(new ConnectionStateListener() {
                @Override
                public void connectionStateChanged(ConnectionState connectionState) {
                    System.out.println("Connection state changed " + connectionState);
                }
            });
        }

        private void subscribeAnonymousRecord(DeepstreamClient client) {
            AnonymousRecord anonymousRecord = client.record.getAnonymousRecord();
        }

        private void subscribeList(final DeepstreamClient client) {
            List list = client.record.getList("list/a");
            list.subscribe(new ListChangedListener() {
                @Override
                public void onListChanged(String listName, String[] entries) {
                    System.out.println(String.format("List %s entries changed to %s", listName, Arrays.toString(entries)));
                }
            });
            list.subscribe(new ListEntryChangedListener() {
                @Override
                public void onEntryAdded(String listName, String entry, int position) {
                    System.out.println(String.format("List %s entry %s added", listName, entry));
                }

                @Override
                public void onEntryRemoved(String listName, String entry, int position) {
                    System.out.println(String.format("List %s entry %s removed", listName, entry));
                }

                @Override
                public void onEntryMoved(String listName, String entry, int position) {
                    System.out.println(String.format("List %s entry %s moved", listName, entry));
                }
            });
            System.out.println(String.format("List '%s' initial state: %s", list.name(), Arrays.toString(list.getEntries())));
        }

        private void subscribeRecord(final DeepstreamClient client, final String recordName) {
            Record record = client.record.getRecord(recordName);
            record.subscribe(new RecordChangedCallback() {
                @Override
                public void onRecordChanged(String recordName, JsonElement data) {
                    System.out.println(String.format("Record '%s' changed, data is now: %s", recordName, data));
                }
            });
            System.out.println(String.format("Record '%s' initial state: ", record.name(), record.get()));
        }

        private void subscribeEvent(DeepstreamClient client) {
            client.event.subscribe("event/a", new EventListener() {
                @Override
                public void onEvent(String eventName, Object args) {
                    JsonArray parameters = (JsonArray) args;
                    System.out.println(String.format("Event '%s' occurred with: %s at %s", eventName, parameters.get(0).getAsString(), parameters.get(1).getAsLong()));
                }
            });
        }

        private void subscribePresence(DeepstreamClient client) {
            client.presence.subscribe(new PresenceEventListener() {
                @Override
                public void onClientLogin(String username) {
                    System.out.println(username + " logged in");
                }

                @Override
                public void onClientLogout(String username) {
                    System.out.println(username + " logged out");
                }
            });
        }

        private void queryClients(DeepstreamClient client) throws DeepstreamError {
            String[] clients = client.presence.getAll();
            System.out.println(String.format("Clients currently connected: ", Arrays.toString(clients)));
        }

    }
}
