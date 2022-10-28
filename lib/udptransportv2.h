// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * udptransport.h:
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

#ifndef _LIB_UDPTRANSPORTV2_H_
#define _LIB_UDPTRANSPORTV2_H_

#include "lib/configuration.h"
#include "lib/transport.h"
#include "lib/transportcommon.h"
#include "lib/udptransportaddress.h"

#include <event2/event.h>

#include <map>
#include <list>
#include <vector>
#include <unordered_map>
#include <random>
#include <netinet/in.h>
#include <future>
#include <string>

using HandleMsgQ = moodycamel::ReaderWriterQueue<MsgtoHandle*>;
using SendMsgQ = moodycamel::ReaderWriterQueue<MsgtoSend*>;

class UDPTransportV2 : public TransportCommonV2<UDPTransportAddress>
{
public:
    UDPTransportV2(double dropRate = 0.0, double reorderRate = 0.0,
                 int dscp = 0, event_base *evbase = nullptr,
                 int = 0, int  = 2, int = 4);
    virtual ~UDPTransportV2();
    void Register(TransportReceiver *receiver,
                  const specpaxos::Configuration &config,
                  int replicaIdx);
    void Run();
    int Timer(uint64_t ms, timer_callback_t cb);
    int Timer(uint64_t ms, timer_callback_t cb, string type);
    bool CancelTimer(int id);
    void CancelAllTimers();
    
private:
    struct UDPTransportTimerInfo
    {
        UDPTransportV2 *transport;
        timer_callback_t cb;
        event *ev;
        int id;
        string type;
    };

    double dropRate;
    double reorderRate;
    std::uniform_real_distribution<double> uniformDist;
    std::default_random_engine randomEngine;
    struct
    {
        bool valid;
        UDPTransportAddress *addr;
        string msgType;
        string message;
        int fd;
    } reorderBufferv1;

    struct
    {
        MsgtoHandle* msg;
//        UDPTransportAddress *addr;
//        string msgType;
//        string message;
//        int fd;
        bool valid;
    } reorderBuffer;
    int dscp;

    event_base *libeventBase;
    std::vector<event *> listenerEvents;
    std::vector<event *> signalEvents;
    std::map<int, TransportReceiver*> receivers; // fd -> receiver
    std::map<TransportReceiver*, int> fds; // receiver -> fd

    TransportReceiver* outstandingReceiver;
    int replicafd;

    std::map<const specpaxos::Configuration *, int> multicastFds;
    std::map<int, const specpaxos::Configuration *> multicastConfigs;
    int _lastTimerId;
    std::atomic<int> lastTimerId;
    std::map<int, UDPTransportTimerInfo *> timers;
    uint64_t lastFragMsgId;
    struct UDPTransportFragInfo
    {
        uint64_t msgId;
        string data;
    };
    std::map<UDPTransportAddress, UDPTransportFragInfo> fragInfo;

    void SendMessageInternal(TransportReceiver *src, const UDPTransportAddress &dst,
                                const std::shared_ptr<Message> m, bool multicast = false, bool isseq = false);

    HandleMsgQ handleq;
    SendMsgQ sendq;

    std::vector<std::thread> senderpool;
    std::thread actor;

    int cpunum;
    int loopcpu;
    int handlecpu;
    int sendcpu;

    std::set<int> sender_cpu = {};
    int sendernum = 1;
    std::set<int> avoid_cpu = {0, 5};

    bool SendMessageInternalT(TransportReceiver *src,
                             const UDPTransportAddress &dst,
                             const Message &m, bool multicast = false);

    UDPTransportAddress
    LookupAddress(const specpaxos::ReplicaAddress &addr);
    UDPTransportAddress
    LookupAddress(const specpaxos::Configuration &cfg,
                  int replicaIdx);

    void OnReadable(int fd);
    void OnTimer(UDPTransportTimerInfo *info);
    static void SocketCallback(evutil_socket_t fd,
                               short what, void *arg);
    static void TimerCallback(evutil_socket_t fd,
                              short what, void *arg);
    static void LogCallback(int severity, const char *msg);
    static void FatalCallback(int err);
    static void SignalCallback(evutil_socket_t fd,
                               short what, void *arg);

    void MsgSender(int cpu, SendMsgQ& sendq);
    void MsgHandler(int cpu, HandleMsgQ& handleq, SendMsgQ& sendq);
    void JoinWorkers();
};

#endif  // _LIB_UDPTRANSPORTV2_H_
