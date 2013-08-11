// Copyright 2011 Mariano Iglesias <mgiglesias@gmail.com>
#ifndef QUERY_H_
#define QUERY_H_

#include <v8.h>
#include <stdlib.h>
#include <node.h>
#include <node_buffer.h>
#include <node_version.h>
#include <algorithm>
#include <cctype>
#include <iomanip>
#include <string>
#include <sstream>
#include <vector>
#include "./node_defs.h"
#include "./connection.h"
#include "./events.h"
#include "./exception.h"
#include "./result.h"
#include "nan.h"

namespace node_db {
class Query : public EventEmitter {
    public:
        static void Init(v8::Handle<v8::Object> target, v8::Local<v8::FunctionTemplate> constructorTemplate);
        void setConnection(Connection* connection);
        v8::Local<v8::Value> set(_NAN_METHOD_ARGS);

    protected:
        struct row_t {
            char** columns;
            unsigned long* columnLengths;
        };
        struct execute_request_t {
            v8::Persistent<v8::Object> context;
            Query* query;
            Result* result;
            std::string* error;
            uint16_t columnCount;
            bool buffered;
            std::vector<row_t*>* rows;
        };
        Connection* connection;
        std::ostringstream sql;
        std::vector< v8::Persistent<v8::Value> > values;
        bool async;
        bool cast;
        bool bufferText;
        NanCallback *cbStart;
        NanCallback *cbExecute;
        NanCallback *cbFinish;

        Query();
        ~Query();
        static NAN_METHOD(Select);
        static NAN_METHOD(From);
        static NAN_METHOD(Join);
        static NAN_METHOD(Where);
        static NAN_METHOD(And);
        static NAN_METHOD(Or);
        static NAN_METHOD(Order);
        static NAN_METHOD(Limit);
        static NAN_METHOD(Add);
        static NAN_METHOD(Insert);
        static NAN_METHOD(Update);
        static NAN_METHOD(Set);
        static NAN_METHOD(Delete);
        static NAN_METHOD(Sql);
        static NAN_METHOD(Execute);
        static uv_async_t g_async;
        static void uvExecute(uv_work_t* uvRequest);
        static void uvExecuteFinished(uv_work_t* uvRequest, int status);
        void executeAsync(execute_request_t* request);
        static void freeRequest(execute_request_t* request, bool freeAll = true);
        std::string fieldName(v8::Local<v8::Value> value) const throw(Exception&);
        std::string tableName(v8::Local<v8::Value> value, bool escape = true) const throw(Exception&);
        v8::Handle<v8::Value> addCondition(_NAN_METHOD_ARGS, const char* separator);
        v8::Local<v8::Object> row(Result* result, row_t* currentRow) const;
        virtual std::string parseQuery() const throw(Exception&);
        virtual std::vector<std::string::size_type> placeholders(std::string* parsed) const throw(Exception&);
        virtual Result* execute() const throw(Exception&);
        std::string value(v8::Local<v8::Value> value, bool inArray = false, bool escape = true, int precision = -1) const throw(Exception&);


    private:
        static bool gmtDeltaLoaded;
        static int gmtDelta;

        std::string fromDate(const double timeStamp) const throw(Exception&);
};
}

#endif  // QUERY_H_
