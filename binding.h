// Copyright 2011 Mariano Iglesias <mgiglesias@gmail.com>
#ifndef BINDING_H_
#define BINDING_H_

#include <node.h>
#include <v8.h>
#include <node_buffer.h>
#include <node_version.h>
#include <string>
#include "./node_defs.h"
#include "./connection.h"
#include "./events.h"
#include "./exception.h"
#include "./query.h"

namespace node_db {
class Binding : public EventEmitter {
    public:
        Connection* connection;

    protected:
        struct connect_request_t {
            v8::Persistent<v8::Object> context;
            Binding* binding;
            const char* error;
        };
        NanCallback* cbConnect;

        Binding();
        ~Binding();
        static void Init(v8::Handle<v8::Object> target, v8::Local<v8::FunctionTemplate> constructorTemplate);
        static NAN_METHOD(Connect);
        static NAN_METHOD(Disconnect);
        static NAN_METHOD(IsConnected);
        static NAN_METHOD(Escape);
        static NAN_METHOD(Name);
        static NAN_METHOD(Query);
	static uv_async_t g_async;
        static void uvConnect(uv_work_t* uvRequest);
        static void uvConnectFinished(uv_work_t* uvRequest, int status);
        static void connect(connect_request_t* request);
        static void connectFinished(connect_request_t* request);
        virtual v8::Handle<v8::Value> set(const v8::Local<v8::Object> options) = 0;
        virtual v8::Local<v8::Object> createQuery() const = 0;
};
}

#endif  // BINDING_H_
