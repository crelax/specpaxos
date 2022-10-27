//
// Created by xuanhe on 10/27/22.
//

#include "lib/udptransport.h"
#include "lib/udptransportaddress.h"

using std::pair;

UDPTransportAddress::UDPTransportAddress(const sockaddr_in &addr)
        : addr(addr) {
    memset((void *) addr.sin_zero, 0, sizeof(addr.sin_zero));
}

UDPTransportAddress *
UDPTransportAddress::clone() const {
    UDPTransportAddress *c = new UDPTransportAddress(*this);
    return c;
}

bool operator==(const UDPTransportAddress &a, const UDPTransportAddress &b) {
    return (memcmp(&a.addr, &b.addr, sizeof(a.addr)) == 0);
}

bool operator!=(const UDPTransportAddress &a, const UDPTransportAddress &b) {
    return !(a == b);
}

bool operator<(const UDPTransportAddress &a, const UDPTransportAddress &b) {
    return (memcmp(&a.addr, &b.addr, sizeof(a.addr)) < 0);
}

