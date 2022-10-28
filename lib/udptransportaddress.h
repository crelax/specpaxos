//
// Created by xuanhe on 10/27/22.
//

#ifndef _LIB_UDPTRANSPORTADDRESS_H_
#define _LIB_UDPTRANSPORTADDRESS_H_

#include <netinet/in.h>
#include "lib/transport.h"

class UDPTransportAddress : public TransportAddress
{
public:
    UDPTransportAddress * clone() const;
private:
    UDPTransportAddress(const sockaddr_in &addr);
    sockaddr_in addr;
    friend class MsgtoSend;
    friend class MsgtoHandle;
    friend class UDPTransport;
    friend class UDPTransportV2;
    friend bool operator==(const UDPTransportAddress &a,
                           const UDPTransportAddress &b);
    friend bool operator!=(const UDPTransportAddress &a,
                           const UDPTransportAddress &b);
    friend bool operator<(const UDPTransportAddress &a,
                          const UDPTransportAddress &b);
};

class MsgtoSend{
public:
    int fd;
    UDPTransportAddress* dst;
    uint64_t msgId;
    std::shared_ptr<google::protobuf::Message> m;
    ~MsgtoSend(){
        m = nullptr;
        delete dst;
    }
};

class MsgtoHandle{
public:
    string type;
    string data;
    UDPTransportAddress* src;
    ~MsgtoHandle(){
        delete src;
    }
};

//struct TasktoDo {
//public:
//    int flag; // 0: exit, 1: msg, 2: timercall
//    timer_callback_t tcb;
//    MsgtoHandle* msg;
//    explicit TasktoDo(MsgtoHandle* msg): flag(2), tcb(nullptr), msg(msg) {}
//    explicit TasktoDo(timer_callback_t cb): flag(2), tcb(cb), msg(nullptr) {}
//};

#endif // _LIB_UDPTRANSPORTADDRESS_H_
