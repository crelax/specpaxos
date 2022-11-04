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
    using sprtMsg = std::shared_ptr<google::protobuf::Message>;
public:
    explicit MsgtoSend(UDPTransportAddress* dst, uint64_t msgId, sprtMsg m)
            : dst(dst), msgId(msgId), m(std::move(m)){}
    explicit MsgtoSend()
            : dst(nullptr), msgId(0), m(nullptr){};
    UDPTransportAddress* dst;
    uint64_t msgId;
    std::shared_ptr<google::protobuf::Message> m;
    ~MsgtoSend(){
        m = nullptr;
        delete dst;
    }
};

class MsgOrCB{
public:
    UDPTransportAddress* src = nullptr;
    string type, data;
    timer_callback_t cb;
    bool iscb = false;

    explicit MsgOrCB(): cb(nullptr), iscb(false) {}
    explicit MsgOrCB(UDPTransportAddress* src): src(src), cb(nullptr), iscb(false) {}
    explicit MsgOrCB(bool iscb, timer_callback_t cb) :src(nullptr), cb(std::move(cb)), iscb(true){
        if (!iscb)
            Panic("wrong init of MsgToHandle");
    }

    ~MsgOrCB(){
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
