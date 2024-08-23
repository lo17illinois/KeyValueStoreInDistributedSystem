/**********************************
 * FILE NAME: MP2Node.cpp
 *
 * DESCRIPTION: MP2Node class definition (Revised 2020)
 *
 * MP2 Starter template version
 **********************************/
#include "MP2Node.h"


// Extended messsage class for MP2
Message2::Message2(Message2::Message2Type mt, Message msg):Message(msg),msgType(mt){
}
string Message2::RemoveHdr(string message){ 
    return message.substr(message.find(',')+1);
}
Message2::Message2(string message):Message(Message2::RemoveHdr(message)){
    std::stringstream ss(message);
    char delim;
    int delimpos;
    ss >> delimpos >> delim;
    msgType = static_cast<Message2Type>(delimpos);
}
string Message2::toString(){
    return to_string(msgType) + ',' + Message::toString();
}

/**
 * constructor
 */
MP2Node::MP2Node(Member *memberNode, Params *par, EmulNet * emulNet, Log * log, Address * address) {
	this->memberNode = memberNode;
	this->par = par;
	this->emulNet = emulNet;
	this->log = log;
	ht = new HashTable();
	this->memberNode->addr = *address;
	this->delimiter = "::";
}

/**
 * Destructor
 */
MP2Node::~MP2Node() {
	delete ht;
}

/**
* FUNCTION NAME: updateRing
*
* DESCRIPTION: This function does the following:
*                 1) Gets the current membership list from the Membership Protocol (MP1Node)
*                    The membership list is returned as a vector of Nodes. See Node class in Node.h
*                 2) Constructs the ring based on the membership list
*                 3) Calls the Stabilization Protocol
*/
void MP2Node::updateRing() {
	/*
	 * Implement this. Parts of it are already implemented
	 */
	vector<Node> curMemList;
	bool change = false;
	/*
	 *  Step 1. Get the current membership list from Membership Protocol / MP1
	 */
	curMemList = getMembershipList();

	/*
	 * Step 2: Construct the ring
	 */
	// Sort the list based on the hashCode
	sort(curMemList.begin(), curMemList.end());
    /* right now create the ring as a copy of the sorted member list */
    ring = curMemList; 
    /*Check the status of replicas relative to your position in the ring */
	/*
	 * Step 3: Run the stabilization protocol IF REQUIRED
	 */
    stabilizationProtocol(); 
}

/**
* FUNCTION NAME: getMembershipList
*
* DESCRIPTION: This function goes through the membership list from the Membership protocol/MP1 and
*                 i) generates the hash code for each member
*                 ii) populates the ring member in MP2Node class
*                 It returns a vector of Nodes. Each element in the vector contain the following fields:
*                 a) Address of the node
*                 b) Hash code obtained by consistent hashing of the Address
*/
vector<Node> MP2Node::getMembershipList() {
	unsigned int i;
	vector<Node> curMemList;
	for ( i = 0 ; i < this->memberNode->memberList.size(); i++ ) {
		Address addressOfThisMember;
		int id = this->memberNode->memberList.at(i).getid();
		short port = this->memberNode->memberList.at(i).getport();
		memcpy(&addressOfThisMember.addr[0], &id, sizeof(int));
		memcpy(&addressOfThisMember.addr[4], &port, sizeof(short));
		curMemList.emplace_back(Node(addressOfThisMember));
	}
	return curMemList;
}

/**
* FUNCTION NAME: hashFunction
*
* DESCRIPTION: This functions hashes the key and returns the position on the ring
*                 HASH FUNCTION USED FOR CONSISTENT HASHING
*
* RETURNS:
* size_t position on the ring
*/
size_t MP2Node::hashFunction(string key) {
	std::hash<string> hashFunc;
	size_t ret = hashFunc(key);
	return ret%RING_SIZE;
}

/**
* FUNCTION NAME: clientCreate
*
* DESCRIPTION: client side CREATE API
*                 The function does the following:
*                 1) Constructs the message
*                 2) Finds the replicas of this key
*                 3) Sends a message to the replica
*/
void MP2Node::clientCreate(string key, string value) {
	/*
	 * Implement this
	 */
    //Construct the message
    g_transID++;
    Message2 createMsg(Message2::Gen,Message(g_transID,this->memberNode->addr,MessageType::CREATE,key,value));
    //Finds all replicas of this key and sends message to all replicas
    vector<Node> replicas = findNodes(key);
    for (int i=0;i<3;++i){
        sendMessage2(createMsg,replicas[i].nodeAddress);
    } 
    //Add transaction to transaction map
    transactionStruct transac(key,value,MessageType::CREATE,0, par->getcurrtime());
    transMap.emplace(g_transID, transac);
}
/**
* FUNCTION NAME: clientRead
*
* DESCRIPTION: client side READ API
*                 The function does the following:
*                 1) Constructs the message
*                 2) Finds the replicas of this key
*                 3) Sends a message to the replica
*/
void MP2Node::clientRead(string key){
	/*
	 * Implement this
	 */
    //Construct the message
    g_transID++;
    Message2 createMsg(Message2::Gen,Message(g_transID,this->memberNode->addr,MessageType::READ,key));
    //Finds all replicas of this key and sends message to all replicas
    vector<Node> recipients = findNodes(key);
    for (size_t i=0;i<recipients.size();++i){
        sendMessage2(createMsg,recipients[i].nodeAddress); 
    }
    //Add transaction to transaction map
    transactionStruct transac(key,"",MessageType::READ,0, par->getcurrtime());
    transMap.emplace(g_transID, transac);
}

/**
* FUNCTION NAME: clientUpdate
*
* DESCRIPTION: client side UPDATE API
*                 The function does the following:
*                 1) Constructs the message
*                 2) Finds the replicas of this key
*                 3) Sends a message to the replica
*/
void MP2Node::clientUpdate(string key, string value){
	/*
	 * Implement this
	 */
    //Construct the message
    g_transID++;
    Message2 createMsg(Message2::Gen,Message(g_transID,this->memberNode->addr,MessageType::UPDATE,key,value));
    //Finds all replicas of this key and sends message to all replicas
    vector<Node> recipients = findNodes(key);
    for (int i=0;i<3;++i){
        sendMessage2(createMsg,recipients[i].nodeAddress);
    }
    //Add transaction to transaction map
    transactionStruct transac(key,value,MessageType::UPDATE,0, par->getcurrtime());
    transMap.emplace(g_transID, transac);
}

/**
* FUNCTION NAME: clientDelete
*
* DESCRIPTION: client side DELETE API
*                 The function does the following:
*                 1) Constructs the message
*                 2) Finds the replicas of this key
*                 3) Sends a message to the replica
*/
void MP2Node::clientDelete(string key){
	/*
	 * Implement this
	 */
    //Construct the message
    g_transID++;
    Message2 createMsg(Message2::Gen,Message(g_transID,this->memberNode->addr,MessageType::DELETE,key));
    //Finds all replicas of this key and sends message to all replicas
    vector<Node> recipients = findNodes(key);
    for (int i=0;i<recipients.size();++i){
        sendMessage2(createMsg,recipients[i].nodeAddress); 
    }
    //Add transaction to transaction map
    transactionStruct transac(key,"",MessageType::DELETE,0, par->getcurrtime());
    transMap.emplace(g_transID, transac);
}

/**
* FUNCTION NAME: createKeyValue
*
* DESCRIPTION: Server side CREATE API
*                    The function does the following:
*                    1) Inserts key value into the local hash table
*                    2) Return true or false based on success or failure
*/
bool MP2Node::createKeyValue(string key, string value, ReplicaType replica) {
	/*
	 * Implement this
	 */
	// Create Entry with value + timestamp + replicaType
    Entry e(value, par->getcurrtime(), replica);
    // Insert Entry as value associated with key into HashTable
    ht->create(key, e.convertToString());
    return true;
}

/**
* FUNCTION NAME: readKey
*
* DESCRIPTION: Server side READ API
*                 This function does the following:
*                 1) Read key from local hash table
*                 2) Return value
*/
string MP2Node::readKey(string key) {
	/*
	 * Implement this
	 */
	// Read key from HashTable
    string entryString = ht->read(key);
    if (!entryString.empty()) {
        return entryString;
    } else {
        return "";
    }
}

/**
* FUNCTION NAME: updateKeyValue
*
* DESCRIPTION: Server side UPDATE API
*                 This function does the following:
*                 1) Update the key to the new value in the local hash table
*                 2) Return true or false based on success or failure
*/
bool MP2Node::updateKeyValue(string key, string value, ReplicaType replica) {
	/*
	 * Implement this
	 */
	// Create Entry with value + timestamp + replicaType
    Entry e(value, par->getcurrtime(), replica);
    // Update key in HashTable with Entry as value
    return ht->update(key, e.convertToString());
}

/**
* FUNCTION NAME: deleteKey
*
* DESCRIPTION: Server side DELETE API
*                 This function does the following:
*                 1) Delete the key from the local hash table
*                 2) Return true or false based on success or failure
*/
bool MP2Node::deletekey(string key) {
	/*
	 * Implement this
	 */
	// Delete the key/value pair from HashTable
    return ht->deleteKey(key);
}

/**
* FUNCTION NAME: checkMessages
*
* DESCRIPTION: This function is the message handler of this node.
*                 This function does the following:
*                 1) Pops messages from the queue
*                 2) Handles the messages according to message types
*/
void MP2Node::checkMessages() {
	/*
	 * Implement this. Parts of it are already implemented
	 */
	char * data;
	int size;

	/*
	 * Declare your local variables here
	 */

	// dequeue all messages and handle them
	while ( !memberNode->mp2q.empty() ) {
		/*
		 * Pop a message from the queue
		 */
		data = (char *)memberNode->mp2q.front().elt;
		size = memberNode->mp2q.front().size;
		memberNode->mp2q.pop();

		string message(data, data + size);

		/*
		 * Handle the message types here
		 */
        Message2 msg(message);
        //if msgtype is generaltype
        if (msg.msgType == Message2::Gen) {
            switch (msg.type) {
                //Handle CRUD
                case MessageType::CREATE: HandleCreate(msg); break;
                case MessageType::READ: HandleRead(msg); break;
                case MessageType::UPDATE: HandleUpdate(msg); break;
                case MessageType::DELETE: HandleDelete(msg); break;
                //Handle READREPLY + REPLY
                case MessageType::READREPLY: HandleReadReply(msg); break;
                case MessageType::REPLY: HandleReply(msg); break;
            }
        //elseif msgtype is a UpdateReplica type
        } else if (msg.msgType == Message2::UpdateReplica) {
            //calls serverside CREATE on replicas
            createKeyValue(msg.key,msg.value,msg.replica);
        } else {
            return;
        }
	}
	/*
	 * This function should also ensure all READ and UPDATE operation
	 * get QUORUM replies
	 */
}

/**
* FUNCTION NAME: findNodes
*
* DESCRIPTION: Find the replicas of the given keyfunction
*                 This function is responsible for finding the replicas of a key
*/
vector<Node> MP2Node::findNodes(string key) {
	size_t pos = hashFunction(key);
	vector<Node> addr_vec;
	if (ring.size() >= 3) {
		// if pos <= min || pos > max, the leader is the min
		if (pos <= ring.at(0).getHashCode() || pos > ring.at(ring.size()-1).getHashCode()) {
			addr_vec.emplace_back(ring.at(0));
			addr_vec.emplace_back(ring.at(1));
			addr_vec.emplace_back(ring.at(2));
		}
		else {
			// go through the ring until pos <= node
			for (int i=1; i<ring.size(); i++){
				Node addr = ring.at(i);
				if (pos <= addr.getHashCode()) {
					addr_vec.emplace_back(addr);
					addr_vec.emplace_back(ring.at((i+1)%ring.size()));
					addr_vec.emplace_back(ring.at((i+2)%ring.size()));
					break;
				}
			}
		}
	}
	return addr_vec;
}

/**
 * FUNCTION NAME: recvLoop
 *
 * DESCRIPTION: Receive messages from EmulNet and push into the queue (mp2q)
 */
bool MP2Node::recvLoop() {
    if ( memberNode->bFailed ) {
    	return false;
    }
    else {
        //Calls TransactionMapRefresh to remove timeout nodes
        TransactionMapRefresh();
    	return emulNet->ENrecv(&(memberNode->addr), this->enqueueWrapper, NULL, 1, &(memberNode->mp2q));
    }
}

/**
 * FUNCTION NAME: enqueueWrapper
 *
 * DESCRIPTION: Enqueue the message from Emulnet into the queue of MP2Node
 */
int MP2Node::enqueueWrapper(void *env, char *buff, int size) {
	Queue q;
	return q.enqueue((queue<q_elt> *)env, (void *)buff, size);
}
// removes timedout transactions in the transaction map
void MP2Node::TransactionMapRefresh(){
    //loop through all entries in transaction map
    auto transMapIterator = transMap.begin();
    while (transMapIterator != transMap.end()) {
        //if timedout for more than 15s
        if ((par->getcurrtime() - transMapIterator->second.timestamp) > 15) {
            MessageType msgtype = transMapIterator->second.trans_type;
            int transid = transMapIterator->first;
            //log appriopriate failure
            switch (msgtype) {
                case MessageType::CREATE: log->logCreateFail(&memberNode->addr, true, transid, transMapIterator->second.key, transMapIterator->second.val.second); break;
                case MessageType::UPDATE: log->logUpdateFail(&memberNode->addr, true, transid, transMapIterator->second.key, transMapIterator->second.val.second); break;
                case MessageType::READ: log->logReadFail(&memberNode->addr, true, transid, transMapIterator->second.key); break;
                case MessageType::DELETE: log->logDeleteFail(&memberNode->addr, true, transid, transMapIterator->second.key); break;
            }
            transMap.erase(transMapIterator++);
            //do nothing if not timedout
        } else {
            ++transMapIterator;
        }
    }
}
//Handle CREATE replies
void MP2Node::HandleCreate(Message message){
    // Construct reply message
    Message2 replyCreateMsg(Message2::Gen, Message(message.transID, memberNode->addr, MessageType::REPLY, false));
    //Log if CREATE was success or failure
    bool success = createKeyValue(message.key, message.value, message.replica);
    replyCreateMsg.success = success;
    if(success){
        log->logCreateSuccess(&memberNode->addr, false, message.transID, message.key, message.value);
    } else{
        log->logCreateFail(&memberNode->addr, false, message.transID, message.key, message.value);
    }
    // Send CREATE reply
    sendMessage2(replyCreateMsg, message.fromAddr);
}
//Handle READ replies
void MP2Node::HandleRead(Message message){
    // Construct reply message
    string keyval = readKey(message.key);
    Message2 replyReadMsg(Message2::Gen,Message(message.transID,(this->memberNode->addr),keyval)); 
    //Log if READ was success or failure
    if(!keyval.empty()){
        log->logReadSuccess(&memberNode->addr,false,message.transID,message.key,keyval);
    } else{
        log->logReadFail(&memberNode->addr,false,message.transID,message.key);
    }
    // Send READ reply
    sendMessage2(replyReadMsg,message.fromAddr);
}
//Handle UPDATE replies
void MP2Node::HandleUpdate(Message message){
    // Construct reply message
    Message2 replyUpdateMsg(Message2::Gen,Message(message.transID,(this->memberNode->addr),MessageType::REPLY,false)); 
    //Log if UPDATE was success or failure
    bool success = updateKeyValue(message.key,message.value,message.replica);
    replyUpdateMsg.success = success;
    if(success){
        log->logUpdateSuccess(&memberNode->addr,false,message.transID,message.key,message.value);
    } else{
        log->logUpdateFail(&memberNode->addr,false,message.transID,message.key,message.value);
    }
    // Send UPDATE reply
    sendMessage2(replyUpdateMsg,message.fromAddr);
}
//Handle DELETE replies
void MP2Node::HandleDelete(Message message){
    // Construct reply message
    Message2 replyDeleteMsg(Message2::Gen,Message(message.transID,(this->memberNode->addr),MessageType::REPLY,false)); 
    //Log if DELETE was success or failure
    bool success = deletekey(message.key);
    replyDeleteMsg.success = success;
    if(success){
        log->logDeleteSuccess(&memberNode->addr,false,message.transID,message.key);
    } else{
        log->logDeleteFail(&memberNode->addr,false,message.transID,message.key);
    }
    // Send DELETE reply
    sendMessage2(replyDeleteMsg,message.fromAddr);      
}
//Handle READREPLY
void MP2Node::HandleReadReply(Message message){
    string value = message.value;
    if (value.empty()) {
        return;
    }
    //Get key/value and timestamp from message
    istringstream valueStream(value);
    string keyval, timestampStr;
    getline(valueStream, keyval, ':');
    getline(valueStream, timestampStr, ':');
    int timestamp = stoi(timestampStr);
    //Find transaction associated with message
    int transid_int = message.transID;
    auto transMapIterator = transMap.find(transid_int);
    //Not in transMap
    if (transMapIterator == transMap.end()) {
        return;
    }
    //Increment quorum count
    ++(transMapIterator->second.quorum_count);
    if (transMapIterator->second.quorum_count == 2) {
        //Quorum reached -> success
        log->logReadSuccess(&memberNode->addr, true, transid_int, transMapIterator->second.key, transMapIterator->second.val.second);
        //Erase from transMap
        transMap.erase(transMapIterator);
    } else {
        //Update latest timestamp/value if its out of date
        if (timestamp >= transMapIterator->second.val.first) {
            transMapIterator->second.val = make_pair(timestamp, keyval);
        }
    }
}
//Handle REPLY
void MP2Node::HandleReply(Message message){
    //Find transaction associated with message
    int transid = message.transID;
    auto transMapIterator = transMap.find(transid);
    //Not in transMap
    if (transMapIterator == transMap.end()) {
        return;
    }
    if (!message.success) {
    } else if (++(transMapIterator->second.quorum_count) == 2) {    //Quorum reached -> success
        //Log appropriate success
        switch (transMapIterator->second.trans_type) {
            case MessageType::CREATE:
                log->logCreateSuccess(&memberNode->addr, true, transid, transMapIterator->second.key, transMapIterator->second.val.second);
                break;
            case MessageType::UPDATE:
                log->logUpdateSuccess(&memberNode->addr, true, transid, transMapIterator->second.key, transMapIterator->second.val.second);
                break;
            case MessageType::DELETE:
                log->logDeleteSuccess(&memberNode->addr, true, transid, transMapIterator->second.key);
                break;
        }
        //Erase from transMap
        transMap.erase(transMapIterator);
    } else {
    }
}
// Send Message2 type messages
void MP2Node::sendMessage2(Message2 message,Address& toaddr){
    Address* sendaddr = &(this->memberNode->addr);
    string strrep = message.toString();
    char * msgstr = (char*)strrep.c_str();
    size_t msglen = strlen(msgstr);
    this->emulNet->ENsend(sendaddr,&toaddr,msgstr,msglen);       
}

/**
* FUNCTION NAME: stabilizationProtocol
*
* DESCRIPTION: This runs the stabilization protocol in case of Node joins and leaves
*                 It ensures that there always 3 copies of all keys in the DHT at all times
*                 The function does the following:
*                1) Ensures that there are three "CORRECT" replicas of all the keys in spite of failures and joins
*                Note:- "CORRECT" replicas implies that every key is replicated in its two neighboring nodes in the ring
*/
void MP2Node::stabilizationProtocol() {
    /*
     * Implement the stabilization protocol using the strategy from Code 1
     */
    //Find the current node in the ring
    auto it = std::find_if(ring.begin(), ring.end(),[this](const Node& node) { return node.nodeAddress == memberNode->addr; });
    //Current node not in ring
    if (it == ring.end()) {
        return;
    }
    //Get index of current node
    int currentIndex = std::distance(ring.begin(), it);
    //Send replicas to 3 successors in the ring
    for (int j = 1; j < 4; ++j) {
        //locate a successor
        Node& neighbor = ring[(currentIndex + j) % ring.size()];
        //Replicates local HashTable at each successor
        for (const auto& entry : ht->hashTable) {
            const string& key = entry.first;
            const string& value = entry.second;
            Message2 keyUpdate(Message2::UpdateReplica, Message(-1, memberNode->addr, MessageType::CREATE, key, value));
            sendMessage2(keyUpdate, neighbor.nodeAddress);
        }
    }
}