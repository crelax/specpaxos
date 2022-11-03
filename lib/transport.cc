// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * transport.cc:
 *   message-passing network interface; common definitions
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
#include "lib/transport.h"

#include <event.h>

#include <utility>

TransportReceiver::~TransportReceiver()
{
    delete this->myAddress;
}

void
TransportReceiver::SetAddress(const TransportAddress *addr)
{
    this->myAddress = addr;
}

const TransportAddress &
TransportReceiver::GetAddress()
{
    return *(this->myAddress);
}

TimeoutV2::TimeoutV2(Transport *_transport, uint64_t _ms, timer_callback_t _cb)
        : transport(_transport), ms(_ms), original_cb(std::move(_cb)), active(false) {

    tv.tv_sec = ms / 1000;
    tv.tv_usec = (ms % 1000) * 1000;

    cb = [this]{
        active = false;
        Reset();
        original_cb();
    };

    ev = transport->GenTimerEvent(this, [this](){
        this->Reset();
        this->cb();
    });
}

TimeoutV2::~TimeoutV2()
{
    Stop();
    event_del(ev);
    event_free(ev);
}

void
TimeoutV2::SetTimeout(uint64_t ms)
{
    ASSERT(!Active());
    this->ms = ms;
    tv.tv_sec = ms / 1000;
    tv.tv_usec = (ms % 1000) * 1000;
    event_del(ev);
    ev = transport->GenTimerEvent(this, [this](){
        active = false;
        this->Reset();
        cb();
    });
}

uint64_t
TimeoutV2::Start()
{
    return this->Reset();
}


uint64_t
TimeoutV2::Reset()
{
    Stop();

    event_add(ev, &tv);
    active = true;

    return ms;
}

void
TimeoutV2::Stop()
{
    if (!active)
        return;

    event_del(ev);

    active = false;
}

bool
TimeoutV2::Active() const
{
    return ev != nullptr && active;
}


// Timeout V1

Timeout::Timeout(Transport *transport, uint64_t ms, timer_callback_t cb)
    : transport(transport), ms(ms), cb(cb)
{
    timerId = 0;
}

Timeout::~Timeout()
{
    Stop();
}

void
Timeout::SetTimeout(uint64_t ms)
{
    ASSERT(!Active());
    this->ms = ms;
}

uint64_t
Timeout::Start()
{
    return this->Reset();
}


uint64_t
Timeout::Reset()
{
    Stop();
    
    timerId = transport->Timer(ms, [this]() {
            timerId = 0;
            Reset();
            cb();
        });
    
    return ms;
}

void
Timeout::Stop()
{
    if (timerId > 0) {
        transport->CancelTimer(timerId);
        timerId = 0;
    }
}

bool
Timeout::Active() const
{
    return (timerId != 0);
}
