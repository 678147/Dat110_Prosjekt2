package no.hvl.dat110.broker;

import java.util.Collection;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import no.hvl.dat110.common.TODO;
import no.hvl.dat110.common.Logger;
import no.hvl.dat110.messagetransport.Connection;

public class Storage {

	// data structure for managing subscriptions
	// maps from a topic to set of subscribed users
	protected ConcurrentHashMap<String, Set<String>> subscriptions;
	
	// data structure for managing currently connected clients
	// maps from user to corresponding client session object
	
	protected ConcurrentHashMap<String, ClientSession> clients;

	public Storage() {
		subscriptions = new ConcurrentHashMap<String, Set<String>>();
		clients = new ConcurrentHashMap<String, ClientSession>();
	}

	public Collection<ClientSession> getSessions() {
		return clients.values();
	}

	public Set<String> getTopics() {

		return subscriptions.keySet();

	}

	// get the session object for a given user
	// session object can be used to send a message to the user
	
	public ClientSession getSession(String user) {

		ClientSession session = clients.get(user);

		return session;
	}

	public Set<String> getSubscribers(String topic) {

		return (subscriptions.get(topic));

	}

	public void addClientSession(String user, Connection connection) {
		clients.putIfAbsent(user, new ClientSession(user, connection));
		
		//  add corresponding client session to the storage
		// See ClientSession class
	}
	public void removeClientSession(String user) {
        if (getSession(user) != null) {
            getSession(user).disconnect();
            clients.remove(user);
        }
		//  disconnet the client (user) 
		// and remove client session for user from the storage
	}

	public void createTopic(String topic) {
		subscriptions.putIfAbsent(topic, ConcurrentHashMap.newKeySet());
		// : create topic in the storage
	}

	public void deleteTopic(String topic) {
		subscriptions.remove(topic);
		// delete topic from the storage
	}

	public void addSubscriber(String user, String topic) {
		if(subscriptions.containsKey(topic) && !subscriptions.get(topic).contains(user)) {
			subscriptions.get(topic).add(user);
		}
		//add the user as subscriber to the topic
	
	}

	public void removeSubscriber(String user, String topic) {
		if(subscriptions.containsKey(topic) && subscriptions.get(topic).contains(user)) {
			subscriptions.get(topic).remove(user);
		}
		// remove the user as subscriber to the topic
	}
}
