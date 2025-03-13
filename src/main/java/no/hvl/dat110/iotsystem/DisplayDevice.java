package no.hvl.dat110.iotsystem;

import no.hvl.dat110.client.Client;
import no.hvl.dat110.messages.Message;
import no.hvl.dat110.messages.PublishMsg;
import no.hvl.dat110.common.TODO;

public class DisplayDevice {

    private static final int COUNT = 10;

    public static void main (String[] args) {

        System.out.println("Display starting ...");

        Client client = new Client("display", "localhost", 8080);

        client.connect();

        String topic = "temperature";
        client.createTopic(topic);

        client.subscribe(topic);

        for(int i = 0; i < COUNT; i++) {
            PublishMsg msg = (PublishMsg) client.receive();
            System.out.println("Received: " + msg.getMessage());

        }

        client.unsubscribe(topic);

        client.disconnect();

        System.out.println("Display stopping ... ");

    }
}
