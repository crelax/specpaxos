// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * udptransportv2.cc:
 *   message-passing network interface that uses UDP message delivery
 *   and libasync
 *
 * Copyright 2013-2016 Dan R. K. Ports  <drkp@cs.washington.edu>
 *
 * Permission is hereby granted, free of charge, to any person
 * obtaining a copy of this software and associated documentation
 * files (the "Software"), to deal in the Software without
 * restriction, including without limitation the rights to use, copy,
 * modify, merge, publish, distribute, sublicense, and/or sell copies
 * of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS
 * BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN
 * ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
 * CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 *
 **********************************************************************/

#include "lib/assert.h"
#include "lib/configuration.h"
#include "lib/message.h"
#include "lib/udptransportv2.h"
#include "lib/udptransportaddress.h"

#include <google/protobuf/message.h>
#include <event2/event.h>
#include <event2/thread.h>

#include <random>
#include <cinttypes>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>
#include <signal.h>

const size_t MAX_UDP_MESSAGE_SIZE = 9000; // XXX
const int SOCKET_BUF_SIZE = 10485760;

const uint64_t NONFRAG_MAGIC = 0x20050318;
const uint64_t FRAG_MAGIC = 0x20101010;

using std::pair;


UDPTransportAddress
UDPTransportV2::LookupAddress(const specpaxos::ReplicaAddress &addr) {
    int res;
    struct addrinfo hints;
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_DGRAM;
    hints.ai_protocol = 0;
    hints.ai_flags = 0;
    struct addrinfo *ai;
    if ((res = getaddrinfo(addr.host.c_str(), addr.port.c_str(), &hints, &ai))) {
        Panic("Failed to resolve %s:%s: %s",
              addr.host.c_str(), addr.port.c_str(), gai_strerror(res));
    }
    if (ai->ai_addr->sa_family != AF_INET) {
        Panic("getaddrinfo returned a non IPv4 address");
    }
    UDPTransportAddress out =
            UDPTransportAddress(*((sockaddr_in *) ai->ai_addr));
    freeaddrinfo(ai);
    return out;
}

UDPTransportAddress
UDPTransportV2::LookupAddress(const specpaxos::Configuration &config,
                            int idx) {
    const specpaxos::ReplicaAddress &addr = config.replica(idx);
    return LookupAddress(addr);
}

static void
BindToPort(int fd, const string &host, const string &port) {
    struct sockaddr_in sin;

    if ((host == "") && (port == "any")) {
        // Set up the sockaddr so we're OK with any UDP socket
        memset(&sin, 0, sizeof(sin));
        sin.sin_family = AF_INET;
        sin.sin_port = 0;
    } else {
        // Otherwise, look up its hostname and port number (which
        // might be a service name)
        struct addrinfo hints;
        hints.ai_family = AF_INET;
        hints.ai_socktype = SOCK_DGRAM;
        hints.ai_protocol = 0;
        hints.ai_flags = AI_PASSIVE;
        struct addrinfo *ai;
        int res;
        if ((res = getaddrinfo(host.c_str(), port.c_str(),
                               &hints, &ai))) {
            Panic("Failed to resolve host/port %s:%s: %s",
                  host.c_str(), port.c_str(), gai_strerror(res));
        }
        ASSERT(ai->ai_family == AF_INET);
        ASSERT(ai->ai_socktype == SOCK_DGRAM);
        if (ai->ai_addr->sa_family != AF_INET) {
            Panic("getaddrinfo returned a non IPv4 address");
        }
        sin = *(sockaddr_in *) ai->ai_addr;

        freeaddrinfo(ai);
    }

    Notice("Binding to %s:%d", inet_ntoa(sin.sin_addr), htons(sin.sin_port));

    if (bind(fd, (sockaddr *) &sin, sizeof(sin)) < 0) {
        PPanic("Failed to bind to socket");
    }
}

UDPTransportV2::UDPTransportV2(double dropRate, double reorderRate,
                           int dscp, event_base *evbase,
                          int loopcpu, int handlecpu, int sendcpu)
        : dropRate(dropRate), reorderRate(reorderRate), dscp(dscp),
          loopcpu(loopcpu), handlecpu(handlecpu), sendcpu(sendcpu),
          sendq(SendMsgQ(100000)), handleq(HandleMsgQ (100000)) {

//    lastTimerId = 0;
    lastTimerId.store(0);
    lastFragMsgId = 0;

    uniformDist = std::uniform_real_distribution<double>(0.0, 1.0);
    randomEngine.seed(time(NULL));
    reorderBuffer.valid = false;
    if (dropRate > 0) {
        Warning("Dropping packets with probability %g", dropRate);
    }
    if (reorderRate > 0) {
        Warning("Reordering packets with probability %g", reorderRate);
    }

    // Set up libevent
    event_set_log_callback(LogCallback);
    event_set_fatal_callback(FatalCallback);
    // XXX Hack for Naveen: allow the user to specify an existing
    // libevent base. This will probably not work exactly correctly
    // for error messages or signals, but that doesn't much matter...
    if (evbase) {
        libeventBase = evbase;
    } else {
        evthread_use_pthreads();
        libeventBase = event_base_new();
        evthread_make_base_notifiable(libeventBase);
    }

    // Set up signal handler
    signalEvents.push_back(evsignal_new(libeventBase, SIGTERM,
                                        SignalCallback, this));
    signalEvents.push_back(evsignal_new(libeventBase, SIGINT,
                                        SignalCallback, this));
    for (event *x: signalEvents) {
        event_add(x, NULL);
    }

//
//    sendq = SendMsgQ (100000);
//    handleq = HandleMsgQ (100000);

    cpunum = std::thread::hardware_concurrency();
    int sendernum = 1;
    sendernum = std::max(1, std::min(cpunum - 3, sendernum));
    Notice("arranging %d threads to send packages", sendernum);

    int cpu = 0;
    sleep(1);

    actor = std::thread(&UDPTransportV2::MsgHandler, this, handlecpu, std::ref(handleq), std::ref(sendq));

    Notice("adding thread to send pkgs to cpu %d", sendcpu);
    senderpool.emplace_back(std::thread(&UDPTransportV2::MsgSender, this, sendcpu, std::ref(sendq)));
    Notice("Starting sender thread %llu", senderpool.back().get_id());

    avoid_cpu.insert(loopcpu);
    avoid_cpu.insert(handlecpu);

//    for (int i = 0; i < 1 + sendernum; i++) {
//        do {
//            cpu = (cpu + 1) % cpunum;
//        } while (avoid_cpu.count(cpu));
//
//        Notice("adding thread to cpu %d", cpu);
//        if (i == 0) {
//            Notice("adding thread to send sequential pkgs to cpu %d", cpu);
//            senderpool.emplace_back(std::thread(&UDPTransportV2::msgsender, this, cpu, std::ref(sendq)));
//        } else {
//            Notice("adding thread to send non-sequential pkgs to cpu %d", cpu);
//            senderpool.emplace_back(std::thread(&UDPTransportV2::msgsender, this, cpu, std::ref(sendq)));
//        }
//        Notice("Starting sender thread %llu", senderpool.back().get_id());
//    }
}

UDPTransportV2::~UDPTransportV2() {
    // XXX Shut down libevent?

    // for (auto kv : timers) {
    //     delete kv.second;
    // }

}

void
UDPTransportV2::Register(TransportReceiver *receiver,
                       const specpaxos::Configuration &config,
                       int replicaIdx) {
    ASSERT(replicaIdx < config.n);
    struct sockaddr_in sin;

    const specpaxos::Configuration *canonicalConfig =
            RegisterConfiguration(receiver, config, replicaIdx);

    // Create socket
    int fd;
    if ((fd = socket(AF_INET, SOCK_DGRAM, 0)) < 0) {
        PPanic("Failed to create socket to listen");
    }

    // Put it in non-blocking mode
    if (fcntl(fd, F_SETFL, O_NONBLOCK, 1)) {
        PWarning("Failed to set O_NONBLOCK");
    }

    // Enable outgoing broadcast traffic
    int n = 1;
    if (setsockopt(fd, SOL_SOCKET,
                   SO_BROADCAST, (char *) &n, sizeof(n)) < 0) {
        PWarning("Failed to set SO_BROADCAST on socket");
    }

    if (dscp != 0) {
        n = dscp << 2;
        if (setsockopt(fd, IPPROTO_IP,
                       IP_TOS, (char *) &n, sizeof(n)) < 0) {
            PWarning("Failed to set DSCP on socket");
        }
    }

    // Increase buffer size
    n = SOCKET_BUF_SIZE;
    if (setsockopt(fd, SOL_SOCKET,
                   SO_RCVBUF, (char *) &n, sizeof(n)) < 0) {
        PWarning("Failed to set SO_RCVBUF on socket");
    }
    if (setsockopt(fd, SOL_SOCKET,
                   SO_SNDBUF, (char *) &n, sizeof(n)) < 0) {
        PWarning("Failed to set SO_SNDBUF on socket");
    }

    if (replicaIdx != -1) {
        // Registering a replica. Bind socket to the designated
        // host/port
        const string &host = config.replica(replicaIdx).host;
        const string &port = config.replica(replicaIdx).port;
        BindToPort(fd, host, port);
    } else {
        // Registering a client. Bind to any available host/port
        BindToPort(fd, "", "any");
    }

    // Set up a libevent callback
    event *ev = event_new(libeventBase, fd, EV_READ | EV_PERSIST,
                          SocketCallback, (void *) this);
    event_add(ev, NULL);
    listenerEvents.push_back(ev);

    // Tell the receiver its address
    socklen_t sinsize = sizeof(sin);
    if (getsockname(fd, (sockaddr *) &sin, &sinsize) < 0) {
        PPanic("Failed to get socket name");
    }
    UDPTransportAddress *addr = new UDPTransportAddress(sin);
    receiver->SetAddress(addr);

    // Update mappings
    receivers[fd] = receiver;
    outstandingReceiver = receiver;
    outstandingReceiverFd = fd;
    fds[receiver] = fd;

    Notice("Listening on UDP port %hu", ntohs(sin.sin_port));

    // If we are registering a replica, check whether we need to set
    // up a socket to listen on the multicast port.
    // V2: no need
}

static size_t
SerializeMessage(const ::google::protobuf::Message &m, char **out) {
    string data = m.SerializeAsString();
    string type = m.GetTypeName();
    size_t typeLen = type.length();
    size_t dataLen = data.length();
    ssize_t totalLen = (sizeof(uint32_t) +
                        typeLen + sizeof(typeLen) +
                        dataLen + sizeof(dataLen));

    char *buf = new char[totalLen];

    char *ptr = buf;
    *(uint32_t *) ptr = NONFRAG_MAGIC;
    ptr += sizeof(uint32_t);
    *((size_t *) ptr) = typeLen;
    ptr += sizeof(size_t);
    ASSERT(ptr - buf < totalLen);
    ASSERT(ptr + typeLen - buf < totalLen);
    memcpy(ptr, type.c_str(), typeLen);
    ptr += typeLen;
    *((size_t *) ptr) = dataLen;
    ptr += sizeof(size_t);
    ASSERT(ptr - buf < totalLen);
    ASSERT(ptr + dataLen - buf == totalLen);
    memcpy(ptr, data.c_str(), dataLen);
    ptr += dataLen;

    *out = buf;
    return totalLen;
}

bool
UDPTransportV2::SendMessageInternalT(TransportReceiver *src,
                                   const UDPTransportAddress &dst,
                                   const Message &m,
                                   bool multicast) {
    sockaddr_in sin = dynamic_cast<const UDPTransportAddress &>(dst).addr;

    // Serialize message
    char *buf;
    size_t msgLen = SerializeMessage(m, &buf);

    int fd = fds[src];

    // XXX All of this assumes that the socket is going to be
    // available for writing, which since it's a UDP socket it ought
    // to be.
    if (msgLen <= MAX_UDP_MESSAGE_SIZE) {
        if (sendto(fd, buf, msgLen, 0,
                   (sockaddr *) &sin, sizeof(sin)) < 0) {
            PWarning("Failed to send message");
            goto fail;
        }
    } else {
        msgLen -= sizeof(uint32_t);
        char *bodyStart = buf + sizeof(uint32_t);
        int numFrags = ((msgLen - 1) / MAX_UDP_MESSAGE_SIZE) + 1;
        Notice("Sending large %s message in %d fragments",
               m.GetTypeName().c_str(), numFrags);
        uint64_t msgId = ++lastFragMsgId;
        for (size_t fragStart = 0; fragStart < msgLen;
             fragStart += MAX_UDP_MESSAGE_SIZE) {
            size_t fragLen = std::min(msgLen - fragStart,
                                      MAX_UDP_MESSAGE_SIZE);
            size_t fragHeaderLen = 2 * sizeof(size_t) + sizeof(uint64_t) + sizeof(uint32_t);
            char fragBuf[fragLen + fragHeaderLen];
            char *ptr = fragBuf;
            *((uint32_t *) ptr) = FRAG_MAGIC;
            ptr += sizeof(uint32_t);
            *((uint64_t *) ptr) = msgId;
            ptr += sizeof(uint64_t);
            *((size_t *) ptr) = fragStart;
            ptr += sizeof(size_t);
            *((size_t *) ptr) = msgLen;
            ptr += sizeof(size_t);
            memcpy(ptr, &bodyStart[fragStart], fragLen);

            if (sendto(fd, fragBuf, fragLen + fragHeaderLen, 0,
                       (sockaddr *) &sin, sizeof(sin)) < 0) {
                PWarning("Failed to send message fragment %ld",
                         fragStart);
                goto fail;
            }
        }
    }

    delete[] buf;
    return true;

    fail:
    delete[] buf;
    return false;
}

void
UDPTransportV2::Run() {
    event_base_dispatch(libeventBase);
}

static void
DecodePacket(const char *buf, size_t sz, string &type, string &msg) {
    ssize_t ssz = sz;
    const char *ptr = buf;
    size_t typeLen = *((size_t *) ptr);
    ptr += sizeof(size_t);
    ASSERT(ptr - buf < ssz);

    ASSERT(ptr + typeLen - buf < ssz);
    type = string(ptr, typeLen);
    ptr += typeLen;

    size_t msgLen = *((size_t *) ptr);
    ptr += sizeof(size_t);
    ASSERT(ptr - buf < ssz);

    ASSERT(ptr + msgLen - buf <= ssz);
    msg = string(ptr, msgLen);
    ptr += msgLen;

}

void
UDPTransportV2::OnReadable(int fd) {
    const int BUFSIZE = 65536;

    ssize_t sz;
    char buf[BUFSIZE];
    sockaddr_in sender;
    socklen_t senderSize = sizeof(sender);

    sz = recvfrom(fd, buf, BUFSIZE, 0,
                  (struct sockaddr *) &sender, &senderSize);
    if (sz == -1) {
        if (errno == EAGAIN || errno == EWOULDBLOCK) {
            return;
        } else {
            PWarning("Failed to receive message from socket");
        }
    }

    MsgtoHandle *m = new MsgtoHandle();
    UDPTransportAddress *senderAddr = new UDPTransportAddress(sender);
    m->src = senderAddr;
    string &msgType = m->type;
    string &msg = m->data;

    // Take a peek at the first field. If it's all zeros, this is
    // a fragment. Otherwise, we can decode it directly.
    ASSERT(sizeof(uint32_t) - sz > 0);
    uint32_t magic = *(uint32_t *) buf;
    if (magic == NONFRAG_MAGIC) {
        // Not a fragment. Decode the packet
        DecodePacket(buf + sizeof(uint32_t), sz - sizeof(uint32_t),
                     msgType, msg);
    } else if (magic == FRAG_MAGIC) {
        // This is a fragment. Decode the header
        const char *ptr = buf;
        ptr += sizeof(uint32_t);
        ASSERT(ptr - buf < sz);
        uint64_t msgId = *((uint64_t *) ptr);
        ptr += sizeof(uint64_t);
        ASSERT(ptr - buf < sz);
        size_t fragStart = *((size_t *) ptr);
        ptr += sizeof(size_t);
        ASSERT(ptr - buf < sz);
        size_t msgLen = *((size_t *) ptr);
        ptr += sizeof(size_t);
        ASSERT(ptr - buf < sz);
        ASSERT(buf + sz - ptr == (ssize_t) std::min(msgLen - fragStart,
                                                    MAX_UDP_MESSAGE_SIZE));
        Notice("Received fragment of %zd byte packet %" PRIx64 " starting at %zd",
               msgLen, msgId, fragStart);
        UDPTransportFragInfo &info = fragInfo[*senderAddr];
        if (info.msgId == 0) {
            info.msgId = msgId;
            info.data.clear();
        }
        if (info.msgId != msgId) {
            ASSERT(msgId > info.msgId);
            Warning("Failed to reconstruct packet %" PRIx64 "", info.msgId);
            info.msgId = msgId;
            info.data.clear();
        }

        if (fragStart != info.data.size()) {
            Warning("Fragments out of order for packet %" PRIx64 "; "
                                                                 "expected start %zd, got %zd",
                    msgId, info.data.size(), fragStart);
            return;
        }

        info.data.append(string(ptr, buf + sz - ptr));
        if (info.data.size() == msgLen) {
            Debug("Completed packet reconstruction");
            DecodePacket(info.data.c_str(), info.data.size(),
                         msgType, msg);
            info.msgId = 0;
            info.data.clear();
        } else {
            return;
        }
    } else {
        Warning("Received packet with bad magic number");
    }

    // Dispatch
    if (dropRate > 0.0) {
        double roll = uniformDist(randomEngine);
        if (roll < dropRate) {
            Debug("Simulating packet drop of message type %s",
                  msgType.c_str());
            delete m;
            return;
        }
    }

    if (!reorderBuffer.valid && (reorderRate > 0.0)) {
        double roll = uniformDist(randomEngine);
        if (roll < reorderRate) {
            Debug("Simulating reorder of message type %s",
                  msgType.c_str());
            ASSERT(!reorderBuffer.valid);
            reorderBuffer.valid = true;
            reorderBuffer.msg = m;
            return;
        }
    }

    // deliver:
    while(!handleq.enqueue(m));
    // TransportReceiver *receiver = receivers[fd];
    // receiver->ReceiveMessage(senderAddr, msgType, msg);
//    outstandingReceiver->ReceiveMessage(m->src, m->type, m->data);
//    delete m;

    if (reorderBuffer.valid) {

        while(!handleq.enqueue(m));
//        while(!taskq.enqueue(TasktoDo(m)));
        Debug("Delivering reordered packet of type %s",
              reorderBuffer.msg->type.c_str());
//        delete reorderBuffer.msg;
        reorderBuffer.valid = false;
        reorderBuffer.msg = nullptr;
    }
}

int
UDPTransportV2::Timer(uint64_t ms, timer_callback_t cb) {
    UDPTransportTimerInfo *info = new UDPTransportTimerInfo();

    struct timeval tv;
    tv.tv_sec = ms / 1000;
    tv.tv_usec = (ms % 1000) * 1000;

    info->transport = this;
    info->id = lastTimerId.fetch_add(1);;
    info->cb = cb;
    info->ev = event_new(libeventBase, -1, 0,
                         TimerCallback, info);

    timers[info->id] = info;

    event_add(info->ev, &tv);

    return info->id;
}


bool
UDPTransportV2::CancelTimer(int id) {
    UDPTransportTimerInfo *info = timers[id];

    if (info == NULL) {
        return false;
    }

//    Notice("erase %d in cancel timer", info->id);
    timers.erase(info->id);
    canceledtimers.insert(id);
    event_del(info->ev);
    event_free(info->ev);
    delete info;
//    Notice("erase %d in cancel timer done", info->id);
    return true;
}

void
UDPTransportV2::CancelAllTimers() {
    while (!timers.empty()) {
        auto kv = timers.begin();
        CancelTimer(kv->first);
    }
}

void
UDPTransportV2::OnTimer(UDPTransportTimerInfo *info) {
//    Notice("erase %d in On timer", info->id);
    timers.erase(info->id);
    event_del(info->ev);
    event_free(info->ev);

    info->cb();

    delete info;
}

void
UDPTransportV2::SocketCallback(evutil_socket_t fd, short what, void *arg) {
    UDPTransportV2 *transport = (UDPTransportV2 *) arg;
    if (what & EV_READ) {
        transport->OnReadable(fd);
    }
}

void
UDPTransportV2::TimerCallback(evutil_socket_t fd, short what, void *arg) {
    UDPTransportV2::UDPTransportTimerInfo *info =
            (UDPTransportV2::UDPTransportTimerInfo *) arg;

    ASSERT(what & EV_TIMEOUT);

    info->transport->OnTimer(info);
}

void
UDPTransportV2::LogCallback(int severity, const char *msg) {
    Message_Type msgType;
    switch (severity) {
        case _EVENT_LOG_DEBUG:
            msgType = MSG_DEBUG;
            break;
        case _EVENT_LOG_MSG:
            msgType = MSG_NOTICE;
            break;
        case _EVENT_LOG_WARN:
            msgType = MSG_WARNING;
            break;
        case _EVENT_LOG_ERR:
            msgType = MSG_WARNING;
            break;
        default:
            NOT_REACHABLE();
    }

    _Message(msgType, "libevent", 0, NULL, "%s", msg);
}

void
UDPTransportV2::FatalCallback(int err) {
    Panic("Fatal libevent error: %d", err);
}

void
UDPTransportV2::SignalCallback(evutil_socket_t fd, short what, void *arg) {
    Notice("Terminating on SIGTERM/SIGINT");
    UDPTransportV2 *transport = (UDPTransportV2 *) arg;
    event_base_loopbreak(transport->libeventBase);
    transport->JoinWorkers();
}

//bool __SendMessageInternal(int fd, char *buf, size_t msgLen, sockaddr_in sin) {
//    if (sendto(fd, buf, msgLen, 0,
//               (sockaddr *) &sin, sizeof(sin)) < 0) {
//        PWarning("Failed to send message");
//        goto fail;
//    }
//
//    delete[] buf;
//    return true;
//
//    fail:
//    delete[] buf;
//    return false;
//}
//
//bool __SendMessageInternalLarge(int fd, char *buf, size_t msgLen,
//                                sockaddr_in sin, uint64_t msgId) {
//    msgLen -= sizeof(uint32_t);
//    char *bodyStart = buf + sizeof(uint32_t);
//    int numFrags = ((msgLen - 1) / MAX_UDP_MESSAGE_SIZE) + 1;
////        Notice("Sending large message in %d fragments", numFrags);
//    for (size_t fragStart = 0; fragStart < msgLen;
//         fragStart += MAX_UDP_MESSAGE_SIZE) {
//        size_t fragLen = std::min(msgLen - fragStart,
//                                  MAX_UDP_MESSAGE_SIZE);
//        size_t fragHeaderLen = 2 * sizeof(size_t) + sizeof(uint64_t) + sizeof(uint32_t);
//        char fragBuf[fragLen + fragHeaderLen];
//        char *ptr = fragBuf;
//        *((uint32_t *) ptr) = FRAG_MAGIC;
//        ptr += sizeof(uint32_t);
//        *((uint64_t *) ptr) = msgId;
//        ptr += sizeof(uint64_t);
//        *((size_t *) ptr) = fragStart;
//        ptr += sizeof(size_t);
//        *((size_t *) ptr) = msgLen;
//        ptr += sizeof(size_t);
//        memcpy(ptr, &bodyStart[fragStart], fragLen);
//
//        if (sendto(fd, fragBuf, fragLen + fragHeaderLen, 0,
//                   (sockaddr *) &sin, sizeof(sin)) < 0) {
//            PWarning("Failed to send message fragment %ld",
//                     fragStart);
//            goto fail;
//        }
//    }
//
//    delete[] buf;
//    return true;
//
//    fail:
//    delete[] buf;
//    return false;
//}

void
UDPTransportV2::SendMessageInternal(TransportReceiver *src,
                                    const UDPTransportAddress &dst,
                                    const std::shared_ptr<Message> m,
                                    bool multicast,
                                    bool isseq) {
    // Serialize message
//    int fd = fds[src];

    MsgtoSend* t = new MsgtoSend{outstandingReceiverFd, dst.clone(), 0, m};
    if (m->ByteSizeLong() > MAX_UDP_MESSAGE_SIZE - 1000) {
        t->msgId = ++lastFragMsgId;
    }
    while (!sendq.enqueue(t));
//    if (isseq)
//        sequential_taskq.enqueue(t);
//    else
//        random_taskq.enqueue(t);
}

//
//void do_send(MsgtoSend* t, ssize_t msgLen, char* cptr) {
//    char *buf = cptr;
//    auto sin = t->dst->addr;
//    //    auto msgLen = t->msgLe;
//    if (msgLen <= MAX_UDP_MESSAGE_SIZE) {
//        if (sendto(t->fd, buf, msgLen, 0,
//                   (sockaddr *)&sin, sizeof(sin)) < 0) {
//            PWarning("Failed to send message");
//            goto out;
//        }
//    } else {
//        msgLen -= sizeof(uint32_t);
//        char *bodyStart = buf + sizeof(uint32_t);
//        int numFrags = ((msgLen - 1) / MAX_UDP_MESSAGE_SIZE) + 1;
//        Notice("Sending large message in %d fragments", numFrags);
//
//        for (size_t fragStart = 0; fragStart < msgLen;
//             fragStart += MAX_UDP_MESSAGE_SIZE) {
//            size_t fragLen = std::min(msgLen - fragStart,
//                                      MAX_UDP_MESSAGE_SIZE);
//            size_t fragHeaderLen = 2*sizeof(size_t) + sizeof(uint64_t) + sizeof(uint32_t);
//            char fragBuf[fragLen + fragHeaderLen];
//            char *ptr = fragBuf;
//            *((uint32_t *)ptr) = FRAG_MAGIC;
//            ptr += sizeof(uint32_t);
//            *((uint64_t *)ptr) = t->msgId;
//            ptr += sizeof(uint64_t);
//            *((size_t *)ptr) = fragStart;
//            ptr += sizeof(size_t);
//            *((size_t *)ptr) = msgLen;
//            ptr += sizeof(size_t);
//            memcpy(ptr, &bodyStart[fragStart], fragLen);
//
//            if (sendto(t->fd, fragBuf, fragLen + fragHeaderLen, 0,
//                       (sockaddr *)&(sin), sizeof(sin)) < 0) {
//                PWarning("Failed to send message fragment %ld",
//                         fragStart);
//                goto out;
//            }
//        }
//    }
//
//    out:
//    //    delete dst;
//    //    delete[] buf;
//    return;
//}

void UDPTransportV2::JoinWorkers() {
    while(!sendq.enqueue(nullptr));
//    actor.join();
    for (auto& t : senderpool) {
        t.join();
    }
}

void
UDPTransportV2::MsgHandler(int cpu, HandleMsgQ& recvq, SendMsgQ& sendq) {
    if (cpu >= 0) {
        cpu_set_t mask;
        CPU_ZERO(&mask);
        CPU_SET(cpu, &mask);
        pthread_setaffinity_np(pthread_self(), sizeof(mask), &mask);
    }

    MsgtoHandle* msg;
    while(true) {
        if (!recvq.try_dequeue(msg))
            continue;

        if (msg == nullptr) {
            Notice("Handler received signal to quit");
            while(!sendq.enqueue(nullptr));
            //
            break;
        }

//        TransportReceiver *receiver = receivers[fd];
//        receiver->ReceiveMessage(senderAddr, msgType, msg);

        outstandingReceiver->ReceiveMessage(msg->src, msg->type, msg->data);
        delete msg;
        msg = nullptr;
    }
}

void
UDPTransportV2::MsgSender(int cpu, SendMsgQ& sendq) {
    if (cpu >= 0) {
        cpu_set_t mask;
        CPU_ZERO(&mask);
        CPU_SET(cpu, &mask);
        pthread_setaffinity_np(pthread_self(), sizeof(mask), &mask);
    }

    string data, type;
    size_t typeLen, dataLen;
    ssize_t totalLen;

    data.reserve(32767);
    string msgTypename;
    char short_buf[MAX_UDP_MESSAGE_SIZE + 100];

    char* long_buf = new char[100000];
    size_t long_buf_size = 100000;
    // Serialize message
    char *buf;

    MsgtoSend* t;
    while (true) {
        if (sendq.try_dequeue(t))
        {
            if (t == nullptr)  {
                Notice("%d, Received to close", cpu);
                break;
            }

            // Serialize
            type = t->m->GetTypeName();
            t->m->SerializeToString(&data);

            typeLen = type.length();
            dataLen = data.length();
            totalLen = (sizeof(uint32_t) +
                        typeLen + sizeof(typeLen) +
                        dataLen + sizeof(dataLen));
            ssize_t msgLen = totalLen;

            if (totalLen <= MAX_UDP_MESSAGE_SIZE) {
                buf = short_buf;
            } else {
                if (long_buf_size < totalLen) {
                    delete[] long_buf;
                    long_buf = new char[totalLen];
                    long_buf_size = totalLen;
                }
                buf = long_buf;
            }

            {
                // [MAGIC_NUM] [typeLen] [type] [dataLen] [data]
                char *ptr = buf;
                *(uint32_t *) ptr = NONFRAG_MAGIC;
                ptr += sizeof(uint32_t);
                *((size_t *) ptr) = typeLen;
                ptr += sizeof(size_t);
                ASSERT(ptr - buf < totalLen);
                ASSERT(ptr + typeLen - buf < totalLen);
                memcpy(ptr, type.c_str(), typeLen);
                ptr += typeLen;
                *((size_t *) ptr) = dataLen;
                ptr += sizeof(size_t);
                ASSERT(ptr - buf < totalLen);
                ASSERT(ptr + dataLen - buf == totalLen);
                memcpy(ptr, data.c_str(), dataLen);
                ptr += dataLen;
            }

//            if (t->seqId == 0) {
//                // no order
////                do_send(t, totalLen, buf);
//            } else {
//                while (nowSendId.load(std::memory_order_acquire) < t->seqId)
//                    ;
//                do_send(t, totalLen, buf);
//
//                nowSendId.fetch_add(1, std::memory_order_release);
//            }

            auto sin = t->dst->addr;

//            bool follow_order = t->seqId != 0;
//            if (follow_order) {
//                uint64_t x;
//                do {
//                    x = nowSendId.load(std::memory_order_acquire);
//                } while (x < t->seqId);
////                Notice("send since atomic int= %" PRIx64 ", seqId =%" PRIx64 ".", nowSendId.load(), t->seqId);
//            }

            // do send
            if (msgLen <= MAX_UDP_MESSAGE_SIZE) {
//                if (follow_order)
//                    nowSendId.fetch_add(1, std::memory_order_release);
                if (sendto(t->fd, buf, msgLen, 0,
                           (sockaddr *)&sin, sizeof(sin)) < 0) {
                    PWarning("Failed to send message");
                    goto out;
                }
            } else {
                msgLen -= sizeof(uint32_t);
                char *bodyStart = buf + sizeof(uint32_t);
                int numFrags = ((msgLen - 1) / MAX_UDP_MESSAGE_SIZE) + 1;
//                Notice("Sending large message in %d fragments", numFrags);
                Notice("Sending large %s message in %d fragments",
                       t->m->GetTypeName().c_str(), numFrags);

                for (size_t fragStart = 0; fragStart < msgLen;
                     fragStart += MAX_UDP_MESSAGE_SIZE) {
                    size_t fragLen = std::min(msgLen - fragStart,
                                              MAX_UDP_MESSAGE_SIZE);
                    size_t fragHeaderLen = 2*sizeof(size_t) + sizeof(uint64_t) + sizeof(uint32_t);
//                    char fragBuf[fragLen + fragHeaderLen];
                    char *fragBuf = short_buf;
                    char *ptr = fragBuf;
                    *((uint32_t *)ptr) = FRAG_MAGIC;
                    ptr += sizeof(uint32_t);
                    *((uint64_t *)ptr) = t->msgId;
                    ptr += sizeof(uint64_t);
                    *((size_t *)ptr) = fragStart;
                    ptr += sizeof(size_t);
                    *((size_t *)ptr) = msgLen;
                    ptr += sizeof(size_t);
                    memcpy(ptr, &bodyStart[fragStart], fragLen);

//                    if (follow_order && fragLen + fragStart == msgLen)
//                        nowSendId.fetch_add(1, std::memory_order_release);

                    if (sendto(t->fd, fragBuf, fragLen + fragHeaderLen, 0,
                               (sockaddr *)&(sin), sizeof(sin)) < 0) {
                        PWarning("Failed to send message fragment %ld",
                                 fragStart);
//                        if (follow_order && fragLen + fragStart < msgLen)
//                            nowSendId.fetch_add(1, std::memory_order_release);
                        goto out;
                    }
                }
            }

            out:
//            if (follow_order)
//                nowSendId.fetch_add(1, std::memory_order_release);
            delete t;
            t = nullptr;
        }
    }
}