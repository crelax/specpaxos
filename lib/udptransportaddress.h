//
// Created by xuanhe on 10/27/22.
//

#ifndef LIB_UDPTRANSPORTADDRESS_H
#define LIB_UDPTRANSPORTADDRESS_H

#include <netinet/in.h>
#include "lib/transport.h"

class UDPTransportAddress : public TransportAddress
{
public:
    UDPTransportAddress * clone() const;
private:
    UDPTransportAddress(const sockaddr_in &addr);
    sockaddr_in addr;
    friend class UDPTransport;
    friend bool operator==(const UDPTransportAddress &a,
                           const UDPTransportAddress &b);
    friend bool operator!=(const UDPTransportAddress &a,
                           const UDPTransportAddress &b);
    friend bool operator<(const UDPTransportAddress &a,
                          const UDPTransportAddress &b);
};

//using std::pair;
//
//UDPTransportAddress::UDPTransportAddress(const sockaddr_in &addr)
//        : addr(addr) {
//    memset((void *) addr.sin_zero, 0, sizeof(addr.sin_zero));
//}
//
//UDPTransportAddress *
//UDPTransportAddress::clone() const {
//    UDPTransportAddress *c = new UDPTransportAddress(*this);
//    return c;
//}
//
//bool operator==(const UDPTransportAddress &a, const UDPTransportAddress &b) {
//    return (memcmp(&a.addr, &b.addr, sizeof(a.addr)) == 0);
//}
//
//bool operator!=(const UDPTransportAddress &a, const UDPTransportAddress &b) {
//    return !(a == b);
//}
//
//bool operator<(const UDPTransportAddress &a, const UDPTransportAddress &b) {
//    return (memcmp(&a.addr, &b.addr, sizeof(a.addr)) < 0);
//}


#endif //LIB_UDPTRANSPORTADDRESS_H
