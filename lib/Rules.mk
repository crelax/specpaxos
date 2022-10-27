d := $(dir $(lastword $(MAKEFILE_LIST)))

SRCS += $(addprefix $(d), \
	lookup3.cc message.cc memory.cc \
	latency.cc configuration.cc transport.cc udptransportaddress.cc \
	udptransport.cc simtransport.cc transportv2.cc udptransportv2.cc)

PROTOS += $(addprefix $(d), \
          latency-format.proto)

LIB-hash := $(o)lookup3.o

LIB-message := $(o)message.o $(LIB-hash)

LIB-hashtable := $(LIB-hash) $(LIB-message)

LIB-memory := $(o)memory.o

LIB-latency := $(o)latency.o $(o)latency-format.o $(LIB-message)

LIB-configuration := $(o)configuration.o $(LIB-message)

LIB-transport := $(o)transport.o $(LIB-message) $(LIB-configuration)

LIB-simtransport := $(o)simtransport.o $(LIB-transport)

LIB-udptransportaddress := $(o)udptransportaddress.o $(LIB-transport)

LIB-udptransport := $(o)udptransport.o $(LIB-udptransportaddress) $(LIB-transport)

LIB-transportv2 := $(o)transportv2.o $(LIB-message) $(LIB-configuration)

LIB-udptransportv2 := $(o)udptransportv2.o $(LIB-udptransportaddress) $(LIB-transportv2)

include $(d)tests/Rules.mk
