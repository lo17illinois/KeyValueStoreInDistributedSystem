/**********************************
 * FILE NAME: MP2Node.h
 *
 * DESCRIPTION: MP2Node class header file (Revised 2020)
 *
 * MP2 Starter template version
 **********************************/

#ifndef MP2NODE_H_
#define MP2NODE_H_

/**
 * Header files
 */
#include "stdincludes.h"
#include "EmulNet.h"
#include "Node.h"
#include "HashTable.h"
#include "Log.h"
#include "Params.h"
#include "Member.h"
#include "Message.h"
#include "Queue.h"
#include <sstream>

// transaction structure for MP2
struct transactionStruct{
    public:
    string key;   
    MessageType trans_type;
    pair<int,string> val;
    int quorum_count;      
    int timestamp;
    transactionStruct(string k, string value, MessageType ttype, int qc, int ts): key(k), val(0,value), trans_type(ttype), quorum_count(0), timestamp(ts){}
};

// Extended message class for MP2
class Message2:public Message{
    public:
    enum Message2Type{UpdateReplica, Gen};
    Message2(Message2Type msgType,Message msg);
    Message2(string message);
    Message2Type msgType;
    static string RemoveHdr(string message); 
    string toString();
};

/**
 * CLASS NAME: MP2Node
 *
 * DESCRIPTION: This class encapsulates all the key-value store functionality
 * 				including:
 * 				1) Ring
 * 				2) Stabilization Protocol
 * 				3) Server side CRUD APIs
 * 				4) Client side CRUD APIs
 */
class MP2Node {
private:
	// Vector holding the next two neighbors in the ring who have my replicas
	vector<Node> hasMyReplicas;
	// Vector holding the previous two neighbors in the ring whose replicas I have
	vector<Node> haveReplicasOf;
	// Ring
	vector<Node> ring;
	// Hash Table
	HashTable * ht;
	// Member representing this member
	Member *memberNode;
	// Params object
	Params *par;
	// Object of EmulNet
	EmulNet * emulNet;
	// Object of Log
	Log * log;
    // string delimiter
	string delimiter;
	// Transaction map (altered to store transaction struct)
    map<int, transactionStruct>transMap;

public:
	MP2Node(Member *memberNode, Params *par, EmulNet *emulNet, Log *log, Address *addressOfMember);
	Member * getMemberNode() {
		return this->memberNode;
	}

	// ring functionalities
	void updateRing();
	vector<Node> getMembershipList();
	size_t hashFunction(string key);
	bool compareNode(const Node& first, const Node& second) {
		return first.nodeHashCode < second.nodeHashCode;
	}

	// client side CRUD APIs
	void clientCreate(string key, string value);
	void clientRead(string key);
	void clientUpdate(string key, string value);
	void clientDelete(string key);

	// receive messages from Emulnet
	bool recvLoop();
	static int enqueueWrapper(void *env, char *buff, int size);

	// handle messages from receiving queue
	void checkMessages();

	// find the addresses of nodes that are responsible for a key
	vector<Node> findNodes(string key);

	// server
	bool createKeyValue(string key, string value, ReplicaType replica);
	string readKey(string key);
	bool updateKeyValue(string key, string value, ReplicaType replica);
	bool deletekey(string key);

	// stabilization protocol - handle multiple failures
	void stabilizationProtocol();


    // Send Message2 type messages
    void sendMessage2(Message2 message,Address& toaddr);
    // Update transaction map
    void TransactionMapRefresh();
    // Handle CRUD
    void HandleCreate(Message message);
    void HandleRead(Message message);
    void HandleUpdate(Message message);
    void HandleDelete(Message message);
    void HandleReply(Message message);
    void HandleReadReply(Message message);
    
	~MP2Node();
};

#endif /* MP2NODE_H_ */