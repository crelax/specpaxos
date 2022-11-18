// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * transport.h:
 *   message-passing network interface definition
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

#ifndef _LIB_TRANSPORT_H_
#define _LIB_TRANSPORT_H_

#include "lib/configuration.h"
#include "lib/message.h"

#include <google/protobuf/message.h>
#include <functional>
#include <event.h>

class TransportAddress
{
public:
    virtual ~TransportAddress() { }
    virtual TransportAddress *clone() const = 0;
};

class TransportReceiver
{
protected:
    typedef ::google::protobuf::Message Message;
    
public:
    virtual ~TransportReceiver();
    virtual void SetAddress(const TransportAddress *addr);
    virtual const TransportAddress& GetAddress();

    virtual void ReceiveMessage(const TransportAddress &remote,
                                const string &type, const string &data) = 0;

    virtual void ReceiveMessage(const TransportAddress* remote,
                                const string &type, const string &data) {
        Panic("Not supposed to be here");
    };

    
protected:
    const TransportAddress *myAddress;
};

typedef std::function<void (void)> timer_callback_t;

//class TransportV2
//{
//protected:
//    typedef ::google::protobuf::Message Message;
//public:
//    virtual ~TransportV2() {}
//    virtual void Register(TransportReceiver *receiver,
//                          const specpaxos::Configuration &config,
//                          int replicaIdx) = 0;
//    virtual bool SendMessage(TransportReceiver *src, const TransportAddress &dst,
//                             const std::shared_ptr<Message> m, bool sequence = true) = 0;
//
//    virtual bool SendMessageToReplica(TransportReceiver *src, int replicaIdx, const std::shared_ptr<Message> m,
//                                      bool sequence = true) = 0;
//
//    virtual bool SendMessageToAll(TransportReceiver *src, const std::shared_ptr<Message> m, bool sequence = true) = 0;
//
//    virtual int Timer(uint64_t ms, timer_callback_t cb) = 0;
//    virtual bool CancelTimer(int id) = 0;
//    virtual void CancelAllTimers() = 0;
//};

class Transport
{
protected:
    typedef ::google::protobuf::Message Message;
public:
    virtual ~Transport() {}
    virtual void Register(TransportReceiver *receiver,
                          const specpaxos::Configuration &config,
                          int replicaIdx) = 0;
    virtual bool SendMessage(TransportReceiver *src, const TransportAddress &dst,
                             const Message &m) = 0;
    virtual bool SendMessageToReplica(TransportReceiver *src, int replicaIdx, const Message &m) = 0;
    virtual bool SendMessageToAll(TransportReceiver *src, const Message &m) = 0;

    virtual bool SendMessage(TransportReceiver *src, const TransportAddress &dst,
                             std::shared_ptr<Message> m, bool sequence = true) = 0;

    virtual bool SendMessageToReplica(TransportReceiver *src, int replicaIdx, std::shared_ptr<Message> m,
                                      bool sequence = true) = 0;

    virtual bool SendMessageToAll(TransportReceiver *src, std::shared_ptr<Message> m, bool sequence = true) = 0;
    friend class TimeoutV2;
    virtual event* GenTimerEvent(void * timeout, timer_callback_t t) { return nullptr; }

    virtual int Timer(uint64_t ms, timer_callback_t cb) = 0;
    virtual bool CancelTimer(int id) = 0;
    virtual void CancelAllTimers() = 0;
};

class Timeout
{
public:
    Timeout(Transport *transport, uint64_t ms, timer_callback_t cb);
    virtual ~Timeout();
    virtual void SetTimeout(uint64_t ms);
    virtual uint64_t Start();
    virtual uint64_t Reset();
    virtual void Stop();
    virtual bool Active() const;
    
private:
    Transport *transport;
    uint64_t ms;
    timer_callback_t cb;
    int timerId;
};

class TimeoutV2
{
public:
    TimeoutV2(Transport *transport, uint64_t ms, timer_callback_t cb);
    virtual ~TimeoutV2();
    virtual void SetTimeout(uint64_t ms);
    virtual uint64_t Start();
    virtual uint64_t Reset();
    virtual void Stop();
    virtual bool Active() const;
    timer_callback_t cb;
    Transport *transport;

private:
    uint64_t ms;
    struct timeval tv{};

    timer_callback_t original_cb;
    event* ev;
    bool active = false;

    friend class U;
};
#endif  // _LIB_TRANSPORT_H_
