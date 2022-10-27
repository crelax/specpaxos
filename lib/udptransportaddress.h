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

#endif //LIB_UDPTRANSPORTADDRESS_H
