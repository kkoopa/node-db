// Copyright 2011 Mariano Iglesias <mgiglesias@gmail.com>
#include "./binding.h"

node_db::Binding::Binding(): node_db::EventEmitter(), connection(NULL), cbConnect(NULL) {
}

node_db::Binding::~Binding() {
    if (this->cbConnect != NULL) {
        delete this->cbConnect;
    }
}

uv_async_t node_db::Binding::g_async;

void node_db::Binding::Init(v8::Handle<v8::Object> target, v8::Local<v8::FunctionTemplate> constructorTemplate) {
    NODE_ADD_CONSTANT(constructorTemplate, COLUMN_TYPE_STRING, node_db::Result::Column::STRING);
    NODE_ADD_CONSTANT(constructorTemplate, COLUMN_TYPE_BOOL, node_db::Result::Column::BOOL);
    NODE_ADD_CONSTANT(constructorTemplate, COLUMN_TYPE_INT, node_db::Result::Column::INT);
    NODE_ADD_CONSTANT(constructorTemplate, COLUMN_TYPE_BIGINT, node_db::Result::Column::BIGINT);
    NODE_ADD_CONSTANT(constructorTemplate, COLUMN_TYPE_NUMBER, node_db::Result::Column::NUMBER);
    NODE_ADD_CONSTANT(constructorTemplate, COLUMN_TYPE_DATE, node_db::Result::Column::DATE);
    NODE_ADD_CONSTANT(constructorTemplate, COLUMN_TYPE_TIME, node_db::Result::Column::TIME);
    NODE_ADD_CONSTANT(constructorTemplate, COLUMN_TYPE_DATETIME, node_db::Result::Column::DATETIME);
    NODE_ADD_CONSTANT(constructorTemplate, COLUMN_TYPE_TEXT, node_db::Result::Column::TEXT);
    NODE_ADD_CONSTANT(constructorTemplate, COLUMN_TYPE_SET, node_db::Result::Column::SET);

    NODE_ADD_PROTOTYPE_METHOD(constructorTemplate, "connect", Connect);
    NODE_ADD_PROTOTYPE_METHOD(constructorTemplate, "disconnect", Disconnect);
    NODE_ADD_PROTOTYPE_METHOD(constructorTemplate, "isConnected", IsConnected);
    NODE_ADD_PROTOTYPE_METHOD(constructorTemplate, "escape", Escape);
    NODE_ADD_PROTOTYPE_METHOD(constructorTemplate, "name", Name);
    NODE_ADD_PROTOTYPE_METHOD(constructorTemplate, "query", Query);
}

NAN_METHOD(node_db::Binding::Connect) {
    NanScope();

    node_db::Binding* binding = node::ObjectWrap::Unwrap<node_db::Binding>(args.This());
    assert(binding);

    bool async = true;
    int optionsIndex = -1, callbackIndex = -1;

    if (args.Length() > 0) {
        if (args.Length() > 1) {
            ARG_CHECK_OBJECT(0, options);
            ARG_CHECK_FUNCTION(1, callback);
            optionsIndex = 0;
            callbackIndex = 1;
        } else if (args[0]->IsFunction()) {
            ARG_CHECK_FUNCTION(0, callback);
            callbackIndex = 0;
        } else {
            ARG_CHECK_OBJECT(0, options);
            optionsIndex = 0;
        }

        if (optionsIndex >= 0) {
            v8::Local<v8::Object> options = args[optionsIndex]->ToObject();

            v8::Handle<v8::Value> set = binding->set(options);
            if (!set.IsEmpty()) {
                NanReturnValue(set);
            }

            ARG_CHECK_OBJECT_ATTR_OPTIONAL_BOOL(options, async);

            if (options->Has(async_key) && options->Get(async_key)->IsFalse()) {
                async = false;
            }
        }

        if (callbackIndex >= 0) {
            binding->cbConnect = new NanCallback(args[callbackIndex].As<v8::Function>());
        }
    }

    connect_request_t* request = new connect_request_t();
    if (request == NULL) {
        THROW_EXCEPTION("Could not create EIO request")
    }

    NanAssignPersistent(v8::Object, request->context, args.This());
    request->binding = binding;
    request->error = NULL;

    if (async) {
        request->binding->Ref();

        uv_work_t* req = new uv_work_t();
        req->data = request;
        uv_queue_work(uv_default_loop(), req, uvConnect, (uv_after_work_cb)uvConnectFinished);

#if NODE_VERSION_AT_LEAST(0, 7, 9)
	uv_ref((uv_handle_t *)&g_async);
#else
	uv_ref(uv_default_loop());
#endif

    } else {
        connect(request);
        connectFinished(request);
    }

    NanReturnValue(v8::Undefined());
}

void node_db::Binding::connect(connect_request_t* request) {
    try {
        request->binding->connection->open();
    } catch(node_db::Exception const& exception) {
        request->error = exception.what();
    }
}

void node_db::Binding::connectFinished(connect_request_t* request) {
    NanScope();

    bool connected = request->binding->connection->isAlive();
    v8::Local<v8::Value> argv[2];

    if (connected) {
        v8::Local<v8::Object> server = v8::Object::New();
        server->Set(v8::String::New("version"), v8::String::New(request->binding->connection->version().c_str()));
        server->Set(v8::String::New("hostname"), v8::String::New(request->binding->connection->getHostname().c_str()));
        server->Set(v8::String::New("user"), v8::String::New(request->binding->connection->getUser().c_str()));
        server->Set(v8::String::New("database"), v8::String::New(request->binding->connection->getDatabase().c_str()));

        argv[0] = v8::Local<v8::Value>::New(v8::Null());
        argv[1] = server;

        request->binding->Emit("ready", 1, &argv[1]);
    } else {
        argv[0] = v8::String::New(request->error != NULL ? request->error : "(unknown error)");

        request->binding->Emit("error", 1, argv);
    }

    if (request->binding->cbConnect != NULL && !(request->binding->cbConnect->GetFunction()).IsEmpty()) {
        v8::TryCatch tryCatch;
        (*(request->binding->cbConnect->GetFunction()))->Call(NanPersistentToLocal(request->context), connected ? 2 : 1, argv);
        if (tryCatch.HasCaught()) {
            node::FatalException(tryCatch);
        }
    }

    NanDispose(request->context);

    delete request;
}

void node_db::Binding::uvConnect(uv_work_t* uvRequest) {
    connect_request_t* request = static_cast<connect_request_t*>(uvRequest->data);
    assert(request);

    connect(request);
}

void node_db::Binding::uvConnectFinished(uv_work_t* uvRequest, int status) {
    NanScope();

    connect_request_t* request = static_cast<connect_request_t*>(uvRequest->data);
    assert(request);

#if NODE_VERSION_AT_LEAST(0, 7, 9)
    uv_unref((uv_handle_t *)&g_async);
#else
    uv_unref(uv_default_loop());
#endif

    request->binding->Unref();

    connectFinished(request);
}

NAN_METHOD(node_db::Binding::Disconnect) {
    NanScope();

    node_db::Binding* binding = node::ObjectWrap::Unwrap<node_db::Binding>(args.This());
    assert(binding);

    binding->connection->close();

    NanReturnValue(v8::Undefined());
}

NAN_METHOD(node_db::Binding::IsConnected) {
    NanScope();

    node_db::Binding* binding = node::ObjectWrap::Unwrap<node_db::Binding>(args.This());
    assert(binding);

    NanReturnValue(binding->connection->isAlive(true) ? v8::True() : v8::False());
}

NAN_METHOD(node_db::Binding::Escape) {
    NanScope();

    ARG_CHECK_STRING(0, string);

    node_db::Binding* binding = node::ObjectWrap::Unwrap<node_db::Binding>(args.This());
    assert(binding);

    std::string escaped;

    try {
        v8::String::Utf8Value string(args[0]->ToString());
        std::string unescaped(*string);
        escaped = binding->connection->escape(unescaped);
    } catch(node_db::Exception const& exception) {
        THROW_EXCEPTION(exception.what())
    }

    NanReturnValue(v8::String::New(escaped.c_str()));
}

NAN_METHOD(node_db::Binding::Name) {
    NanScope();

    ARG_CHECK_STRING(0, table);

    node_db::Binding* binding = node::ObjectWrap::Unwrap<node_db::Binding>(args.This());
    assert(binding);

    std::ostringstream escaped;

    try {
        v8::String::Utf8Value string(args[0]->ToString());
        std::string unescaped(*string);
        escaped << binding->connection->escapeName(unescaped);
    } catch(node_db::Exception const& exception) {
        THROW_EXCEPTION(exception.what())
    }

    NanReturnValue(v8::String::New(escaped.str().c_str()));
}

NAN_METHOD(node_db::Binding::Query) {
    NanScope();

    node_db::Binding* binding = node::ObjectWrap::Unwrap<node_db::Binding>(args.This());
    assert(binding);

    v8::Local<v8::Object> query = binding->createQuery();
    if (query.IsEmpty()) {
        THROW_EXCEPTION("Could not create query");
    }

    node_db::Query* queryInstance = node::ObjectWrap::Unwrap<node_db::Query>(query);
    queryInstance->setConnection(binding->connection);

    v8::Local<v8::Value> set = queryInstance->set(args);
    if (!set.IsEmpty()) {
        NanReturnValue(set);
    }

    NanReturnValue(query);
}
