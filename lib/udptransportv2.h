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
#include "lib/concurrentqueue.h"
#include "lib/blockingconcurrentqueue.h"

#include <event2/event.h>

#include <map>
#include <list>
#include <vector>
#include <unordered_map>
#include <random>
#include <netinet/in.h>
#include <future>
#include <string>
#include <variant>

//using func_handle_msg = std::function<void (MsgOrCB)>;
using HandleQ = moodycamel::ConcurrentQueue<MsgOrCB*>;
//using TaskQ = moodycamel::ReaderWriterQueue<TasktoDo>;
//using CallalbeQ = moodycamel::ReaderWriterQueue<(int)>;
using SendQ = moodycamel::ConcurrentQueue<MsgtoSend*>;
using PToken = moodycamel::ProducerToken;

class UDPTransportV2 : public TransportCommonV2<UDPTransportAddress>
{
public:
    UDPTransportV2(double dropRate = 0.0, double reorderRate = 0.0,
                 int dscp = 0, event_base *evbase = nullptr, bool enable = true);
    virtual ~UDPTransportV2();
    void Register(TransportReceiver *receiver,
                  const specpaxos::Configuration &config,
                  int replicaIdx);
    void Run();
    int Timer(uint64_t ms, timer_callback_t cb);
    event* GenTimerEvent(void *t, timer_callback_t cb);
//    int Timer(uint64_t ms, timer_callback_t cb, string type);
    bool CancelTimer(int id);
    void CancelAllTimers();
    
private:


    struct UDPTransportTimerInfoV2
    {
        UDPTransportV2 *transport;
        timer_callback_t cb;
        event *ev;
    };

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
    } reorderBuffer;

//    struct
//    {
//        * msg;
////        UDPTransportAddress *addr;
////        string msgType;
////        string message;
////        int fd;
//        bool valid;
//    } reorderBuffer;
    int dscp;

    event_base *libeventBase;
    event_base *timerBase;
    std::vector<event *> listenerEvents;
    std::vector<event *> signalEvents;
    std::map<int, TransportReceiver*> receivers; // fd -> receiver
    std::map<TransportReceiver*, int> fds; // receiver -> fd
    std::vector<int> senderfds;

    TransportReceiver* outstandingReceiver;
    int outstandingReceiverFd;

    std::map<const specpaxos::Configuration *, int> multicastFds;
    std::map<int, const specpaxos::Configuration *> multicastConfigs;
//    std::mutex timeermx;
    std::atomic<int> lastTimerId;
    std::map<int, UDPTransportTimerInfo *> timers;
    std::set<int> canceledtimers;
    uint64_t lastFragMsgId;
    struct UDPTransportFragInfo
    {
        uint64_t msgId;
        string data;
    };
    std::map<UDPTransportAddress, UDPTransportFragInfo> fragInfo;

    void SendMessageInternal(TransportReceiver *src, const UDPTransportAddress &dst,
                                const std::shared_ptr<Message> m, int idx);

    std::vector<SendQ> repSendq;
    std::vector<PToken> repSendqToken;
    SendQ cliSendq = SendQ (4096, 1, 1);
    const PToken cliSendqToken = PToken(cliSendq);

    HandleQ handleq = HandleQ(4096, 1, 1);
    const PToken handleqToken = PToken(handleq);

    std::vector<std::thread> repSender;
    std::thread cliSender;
    std::thread replicator;

    std::vector<std::map<size_t, int>> counters{};

    const bool counter_enable;

    int cpunum;
    int loopcpu;
    int handlecpu = 2;
    int SendClientCPU = 1;

    std::set<int> sender_cpu = {};
    int sendernum = 1;
    std::set<int> avoid_cpu = {0, 1, 5};

    bool SendMessageInternalT(TransportReceiver *src,
                             const UDPTransportAddress &dst,
                             const Message &m, bool multicast = false);

    UDPTransportAddress
    LookupAddress(const specpaxos::ReplicaAddress &addr);
    UDPTransportAddress
    LookupAddress(const specpaxos::Configuration &cfg,
                  int replicaIdx);

    void OnReadable(int fd);
    void OnReadableSync(int fd);
    void OnTimer(UDPTransportTimerInfo *info);
    void OnTimerV2(TimeoutV2 *t);
    void OnTimerV2Sync(TimeoutV2 *t);
    static void SocketCallback(evutil_socket_t fd,
                               short what, void *arg);
    static void TimerCallback(evutil_socket_t fd,
                              short what, void *arg);
    static void TimerCallbackV2(evutil_socket_t fd, short what, void* arg);
    static void TimerCallbackV2Sync(evutil_socket_t fd, short what, void* arg);
    static void LogCallback(int severity, const char *msg);
    static void FatalCallback(int err);
    static void SignalCallback(evutil_socket_t fd,
                               short what, void *arg);

    void MsgSender(int stid, int cpu, const PToken & token, SendQ& send);
    void MsgHandler(int cpu);
    void JoinWorkers();
};

#endif  // _LIB_UDPTRANSPORTV2_H_
