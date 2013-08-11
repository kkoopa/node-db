// Copyright 2011 Mariano Iglesias <mgiglesias@gmail.com>
// Copyright 2011 Georg Wicherski <gw@oxff.net>
#include "./query.h"

bool node_db::Query::gmtDeltaLoaded = false;
int node_db::Query::gmtDelta;

uv_async_t node_db::Query::g_async;

v8::Local<v8::String> v8StringFromUInt64(uint64_t num, std::ostringstream &reusableStream) {
    reusableStream.clear();
    reusableStream.seekp(0);
    reusableStream << num << std::ends;
    return v8::String::New(reusableStream.str().c_str());
}

void node_db::Query::Init(v8::Handle<v8::Object> target, v8::Local<v8::FunctionTemplate> constructorTemplate) {
    NODE_ADD_PROTOTYPE_METHOD(constructorTemplate, "select", Select);
    NODE_ADD_PROTOTYPE_METHOD(constructorTemplate, "from", From);
    NODE_ADD_PROTOTYPE_METHOD(constructorTemplate, "join", Join);
    NODE_ADD_PROTOTYPE_METHOD(constructorTemplate, "where", Where);
    NODE_ADD_PROTOTYPE_METHOD(constructorTemplate, "and", And);
    NODE_ADD_PROTOTYPE_METHOD(constructorTemplate, "or", Or);
    NODE_ADD_PROTOTYPE_METHOD(constructorTemplate, "order", Order);
    NODE_ADD_PROTOTYPE_METHOD(constructorTemplate, "limit", Limit);
    NODE_ADD_PROTOTYPE_METHOD(constructorTemplate, "add", Add);
    NODE_ADD_PROTOTYPE_METHOD(constructorTemplate, "insert", Insert);
    NODE_ADD_PROTOTYPE_METHOD(constructorTemplate, "update", Update);
    NODE_ADD_PROTOTYPE_METHOD(constructorTemplate, "set", Set);
    NODE_ADD_PROTOTYPE_METHOD(constructorTemplate, "delete", Delete);
    NODE_ADD_PROTOTYPE_METHOD(constructorTemplate, "sql", Sql);
    NODE_ADD_PROTOTYPE_METHOD(constructorTemplate, "execute", Execute);
}

node_db::Query::Query(): node_db::EventEmitter(),
    connection(NULL), async(true), cast(true), bufferText(false), cbStart(NULL), cbExecute(NULL), cbFinish(NULL) {
}

node_db::Query::~Query() {
    for (std::vector< v8::Persistent<v8::Value> >::iterator iterator = this->values.begin(), end = this->values.end(); iterator != end; ++iterator) {
        iterator->Dispose();
    }

    if (this->cbStart != NULL) {
        delete this->cbStart;
    }
    if (this->cbExecute != NULL) {
        delete this->cbExecute;
    }
    if (this->cbFinish != NULL) {
        delete this->cbFinish;
    }
}

void node_db::Query::setConnection(node_db::Connection* connection) {
    this->connection = connection;
}

NAN_METHOD(node_db::Query::Select) {
    NanScope();

    if (args.Length() > 0) {
        if (args[0]->IsArray()) {
            ARG_CHECK_ARRAY(0, fields);
        } else if (args[0]->IsObject()) {
            ARG_CHECK_OBJECT(0, fields);
        } else {
            ARG_CHECK_STRING(0, fields);
        }
    } else {
        ARG_CHECK_STRING(0, fields);
    }

    node_db::Query *query = node::ObjectWrap::Unwrap<node_db::Query>(args.This());
    assert(query);

    query->sql << "SELECT ";

    if (args[0]->IsArray()) {
        v8::Local<v8::Array> fields = args[0].As<v8::Array>();
        if (fields->Length() == 0) {
            THROW_EXCEPTION("No fields specified in select")
        }

        for (uint32_t i = 0, limiti = fields->Length(); i < limiti; i++) {
            if (i > 0) {
                query->sql << ",";
            }

            try {
                query->sql << query->fieldName(fields->Get(i));
            } catch(const node_db::Exception& exception) {
                THROW_EXCEPTION(exception.what())
            }
        }
    } else if (args[0]->IsObject()) {
        try {
            query->sql << query->fieldName(args[0]);
        } catch(const node_db::Exception& exception) {
            THROW_EXCEPTION(exception.what())
        }
    } else {
        v8::String::Utf8Value fields(args[0]->ToString());
        query->sql << *fields;
    }

    NanReturnValue(args.This());
}

NAN_METHOD(node_db::Query::From) {
    NanScope();

    if (args.Length() > 0) {
        if (args[0]->IsArray()) {
            ARG_CHECK_ARRAY(0, fields);
        } else if (args[0]->IsObject()) {
            ARG_CHECK_OBJECT(0, tables);
        } else {
            ARG_CHECK_STRING(0, tables);
        }
    } else {
        ARG_CHECK_STRING(0, tables);
    }

    ARG_CHECK_OPTIONAL_BOOL(1, escape);

    node_db::Query *query = node::ObjectWrap::Unwrap<node_db::Query>(args.This());
    assert(query);

    bool escape = true;
    if (args.Length() > 1) {
        escape = args[1]->IsTrue();
    }

    query->sql << " FROM ";

    try {
        query->sql << query->tableName(args[0], escape);
    } catch(const node_db::Exception& exception) {
        THROW_EXCEPTION(exception.what());
    }

    NanReturnValue(args.This());
}

NAN_METHOD(node_db::Query::Join) {
    NanScope();

    ARG_CHECK_OBJECT(0, join);
    ARG_CHECK_OPTIONAL_ARRAY(1, values);

    v8::Local<v8::Object> join = args[0]->ToObject();

    ARG_CHECK_OBJECT_ATTR_OPTIONAL_STRING(join, type);
    ARG_CHECK_OBJECT_ATTR_STRING(join, table);
    ARG_CHECK_OBJECT_ATTR_OPTIONAL_STRING(join, alias);
    ARG_CHECK_OBJECT_ATTR_OPTIONAL_STRING(join, conditions);
    ARG_CHECK_OBJECT_ATTR_OPTIONAL_BOOL(join, escape);

    node_db::Query *query = node::ObjectWrap::Unwrap<node_db::Query>(args.This());
    assert(query);

    std::string type = "INNER";
    bool escape = true;

    if (join->Has(type_key)) {
        v8::String::Utf8Value currentType(join->Get(type_key)->ToString());
        type = *currentType;
        std::transform(type.begin(), type.end(), type.begin(), toupper);
    }

    if (join->Has(escape_key)) {
        escape = join->Get(escape_key)->IsTrue();
    }

    v8::String::Utf8Value table(join->Get(table_key)->ToString());

    query->sql << " " << type << " JOIN ";
    query->sql << (escape ? query->connection->escapeName(*table) : *table);

    if (join->Has(alias_key)) {
        v8::String::Utf8Value alias(join->Get(alias_key)->ToString());
        query->sql << " AS ";
        query->sql << (escape ? query->connection->escapeName(*alias) : *alias);
    }

    if (join->Has(conditions_key)) {
        v8::String::Utf8Value conditions(join->Get(conditions_key)->ToObject());
        std::string currentConditions = *conditions;
        if (args.Length() > 1) {
            v8::Local<v8::Array> currentValues = args[1].As<v8::Array>();
            for (uint32_t i = 0, limiti = currentValues->Length(); i < limiti; i++) {
                NanAssignPersistent(v8::Value, query->values[i], currentValues->Get(i));
            }
        }

        query->sql << " ON (" << currentConditions << ")";
    }

    NanReturnValue(args.This());
}

NAN_METHOD(node_db::Query::Where) {
    NanScope();

    node_db::Query *query = node::ObjectWrap::Unwrap<node_db::Query>(args.This());
    assert(query);

    NanReturnValue(query->addCondition(args, "WHERE"));
}

NAN_METHOD(node_db::Query::And) {
    NanScope();

    node_db::Query *query = node::ObjectWrap::Unwrap<node_db::Query>(args.This());
    assert(query);

    NanReturnValue(query->addCondition(args, "AND"));
}

NAN_METHOD(node_db::Query::Or) {
    NanScope();

    node_db::Query *query = node::ObjectWrap::Unwrap<node_db::Query>(args.This());
    assert(query);

    NanReturnValue(query->addCondition(args, "OR"));
}

NAN_METHOD(node_db::Query::Order) {
    NanScope();

    if (args.Length() > 0 && args[0]->IsObject()) {
        ARG_CHECK_OBJECT(0, fields);
    } else {
        ARG_CHECK_STRING(0, fields);
    }

    ARG_CHECK_OPTIONAL_BOOL(1, escape);

    node_db::Query *query = node::ObjectWrap::Unwrap<node_db::Query>(args.This());
    assert(query);

    bool escape = true;
    if (args.Length() > 1) {
        escape = args[1]->IsTrue();
    }

    query->sql << " ORDER BY ";

    if (args[0]->IsObject()) {
        v8::Local<v8::Object> fields = args[0]->ToObject();
        v8::Local<v8::Array> properties = fields->GetPropertyNames();
        if (properties->Length() == 0) {
            THROW_EXCEPTION("Non empty objects should be used for fields in order");
        }

        for (uint32_t i = 0, limiti = properties->Length(); i < limiti; i++) {
            v8::Local<v8::Value> propertyName = properties->Get(i);
            v8::String::Utf8Value fieldName(propertyName);
            v8::Local<v8::Value> currentValue = fields->Get(propertyName);

            if (i > 0) {
                query->sql << ",";
            }

            bool innerEscape = escape;
            v8::Local<v8::Value> order;
            if (currentValue->IsObject()) {
                v8::Local<v8::Object> currentObject = currentValue->ToObject();
                v8::Local<v8::String> escapeKey = v8::String::New("escape");
                v8::Local<v8::String> orderKey = v8::String::New("order");
                v8::Local<v8::Value> optionValue;

                if (!currentObject->Has(orderKey)) {
                    THROW_EXCEPTION("The \"order\" option for the order field object must be specified");
                }

                order = currentObject->Get(orderKey);

                if (currentObject->Has(escapeKey)) {
                    optionValue = currentObject->Get(escapeKey);
                    if (!optionValue->IsBoolean()) {
                        THROW_EXCEPTION("Specify a valid boolean value for the \"escape\" option in the order field object");
                    }
                    innerEscape = optionValue->IsTrue();
                }
            } else {
                order = currentValue;
            }

            query->sql << (innerEscape ? query->connection->escapeName(*fieldName) : *fieldName);
            query->sql << " ";

            if (order->IsBoolean()) {
                query->sql << (order->IsTrue() ? "ASC" : "DESC");
            } else if (order->IsString()) {
                v8::String::Utf8Value currentOrder(order->ToString());
                query->sql << *currentOrder;
            } else {
                THROW_EXCEPTION("Invalid value specified for \"order\" property in order field");
            }
        }
    } else {
        v8::String::Utf8Value sql(args[0]->ToString());
        query->sql << *sql;
    }

    NanReturnValue(args.This());
}

NAN_METHOD(node_db::Query::Limit) {
    NanScope();

    if (args.Length() > 1) {
        ARG_CHECK_UINT32(0, offset);
        ARG_CHECK_UINT32(1, rows);
    } else {
        ARG_CHECK_UINT32(0, rows);
    }

    node_db::Query *query = node::ObjectWrap::Unwrap<node_db::Query>(args.This());
    assert(query);

    query->sql << " LIMIT ";
    if (args.Length() > 1) {
        query->sql << args[0]->ToInt32()->Value();
        query->sql << ",";
        query->sql << args[1]->ToInt32()->Value();
    } else {
        query->sql << args[0]->ToInt32()->Value();
    }

    NanReturnValue(args.This());
}

NAN_METHOD(node_db::Query::Add) {
    NanScope();

    node_db::Query* innerQuery = NULL;

    if (args.Length() > 0 && args[0]->IsObject()) {
        v8::Local<v8::Object> object = args[0]->ToObject();
        v8::Handle<v8::String> key = v8::String::New("sql");
        if (!object->Has(key) || !object->Get(key)->IsFunction()) {
            ARG_CHECK_STRING(0, sql);
        }

        innerQuery = node::ObjectWrap::Unwrap<node_db::Query>(object);
        assert(innerQuery);
    } else {
        ARG_CHECK_STRING(0, sql);
    }

    node_db::Query *query = node::ObjectWrap::Unwrap<node_db::Query>(args.This());
    assert(query);

    if (innerQuery != NULL) {
        query->sql << innerQuery->sql.str();
    } else {
        v8::String::Utf8Value sql(args[0]->ToString());
        query->sql << *sql;
    }

    NanReturnValue(args.This());
}

NAN_METHOD(node_db::Query::Delete) {
    NanScope();

    if (args.Length() > 0) {
        if (args[0]->IsArray()) {
            ARG_CHECK_ARRAY(0, tables);
        } else if (args[0]->IsObject()) {
            ARG_CHECK_OBJECT(0, tables);
        } else {
            ARG_CHECK_STRING(0, tables);
        }
        ARG_CHECK_OPTIONAL_BOOL(1, escape);
    }

    node_db::Query *query = node::ObjectWrap::Unwrap<node_db::Query>(args.This());
    assert(query);

    bool escape = true;
    if (args.Length() > 1) {
        escape = args[1]->IsTrue();
    }

    query->sql << "DELETE";

    if (args.Length() > 0) {
        try {
            query->sql << " " << query->tableName(args[0], escape);
        } catch(const node_db::Exception& exception) {
            THROW_EXCEPTION(exception.what());
        }
    }

    NanReturnValue(args.This());
}

NAN_METHOD(node_db::Query::Insert) {
    NanScope();
    uint32_t argsLength = args.Length();

    int fieldsIndex = -1, valuesIndex = -1;

    if (argsLength > 0) {
        ARG_CHECK_STRING(0, table);

        if (argsLength > 2) {
            if (args[1]->IsArray()) {
                ARG_CHECK_ARRAY(1, fields);
            } else if (args[1]->IsObject()) {
                ARG_CHECK_OBJECT(1, fields);
            } else if (!args[1]->IsFalse()) {
                ARG_CHECK_STRING(1, fields);
            }
            fieldsIndex = 1;

            if (!args[2]->IsFalse()) {
                valuesIndex = 2;
                ARG_CHECK_ARRAY(2, values);
            }

            ARG_CHECK_OPTIONAL_BOOL(3, escape);
        } else if (argsLength > 1) {
            ARG_CHECK_ARRAY(1, values);
            valuesIndex = 1;
        }
    } else {
        ARG_CHECK_STRING(0, table);
    }

    node_db::Query *query = node::ObjectWrap::Unwrap<node_db::Query>(args.This());
    assert(query);

    bool escape = true;
    if (argsLength > 3) {
        escape = args[3]->IsTrue();
    }

    try {
        query->sql << "INSERT INTO " << query->tableName(args[0], escape);
    } catch(const node_db::Exception& exception) {
        THROW_EXCEPTION(exception.what());
    }

    if (argsLength > 1) {
        if (fieldsIndex != -1) {
            query->sql << "(";
            if (args[fieldsIndex]->IsArray()) {
                v8::Local<v8::Array> fields = args[fieldsIndex].As<v8::Array>();
                if (fields->Length() == 0) {
                    THROW_EXCEPTION("No fields specified in insert")
                }

                for (uint32_t i = 0, limiti = fields->Length(); i < limiti; i++) {
                    if (i > 0) {
                        query->sql << ",";
                    }

                    try {
		        v8::String::Utf8Value fieldName(fields->Get(i));
                        //query->fieldName(fields->Get(i));
		        query->sql << *fieldName;
                    } catch(const node_db::Exception& exception) {
                        THROW_EXCEPTION(exception.what())
                    }
                }
            } else {
                v8::String::Utf8Value fields(args[fieldsIndex]->ToString());
                query->sql << *fields;
            }
            query->sql << ")";
        }

        query->sql << " ";

        if (valuesIndex != -1) {
            v8::Local<v8::Array> values = args[valuesIndex].As<v8::Array>();
            uint32_t valuesLength = values->Length();
            if (valuesLength > 0) {
                bool multipleRecords = values->Get(0)->IsArray();

                query->sql << "VALUES ";
                if (!multipleRecords) {
                    query->sql << "(";
                }

                for (uint32_t i = 0; i < valuesLength; i++) {
                    if (i > 0) {
                        query->sql << ",";
                    }
                    query->sql << query->value(values->Get(i));
                }

                if (!multipleRecords) {
                    query->sql << ")";
                }
            }
        }
    } else {
        query->sql << " ";
    }

    NanReturnValue(args.This());
}

NAN_METHOD(node_db::Query::Update) {
    NanScope();

    if (args.Length() > 0) {
        if (args[0]->IsArray()) {
            ARG_CHECK_ARRAY(0, tables);
        } else if (args[0]->IsObject()) {
            ARG_CHECK_OBJECT(0, tables);
        } else {
            ARG_CHECK_STRING(0, tables);
        }
    } else {
        ARG_CHECK_STRING(0, tables);
    }

    ARG_CHECK_OPTIONAL_BOOL(1, escape);

    node_db::Query *query = node::ObjectWrap::Unwrap<node_db::Query>(args.This());
    assert(query);

    bool escape = true;
    if (args.Length() > 1) {
        escape = args[1]->IsTrue();
    }

    query->sql << "UPDATE ";

    try {
        query->sql << query->tableName(args[0], escape);
    } catch(const node_db::Exception& exception) {
        THROW_EXCEPTION(exception.what());
    }

    NanReturnValue(args.This());
}

NAN_METHOD(node_db::Query::Set) {
    NanScope();

    ARG_CHECK_OBJECT(0, values);
    ARG_CHECK_OPTIONAL_BOOL(1, escape);

    node_db::Query *query = node::ObjectWrap::Unwrap<node_db::Query>(args.This());
    assert(query);

    bool escape = true;
    if (args.Length() > 1) {
        escape = args[1]->IsTrue();
    }

    query->sql << " SET ";

    v8::Local<v8::Object> values = args[0]->ToObject();
    v8::Local<v8::Array> valueProperties = values->GetPropertyNames();
    if (valueProperties->Length() == 0) {
        THROW_EXCEPTION("Non empty objects should be used for values in set");
    }

    for (uint32_t j = 0, limitj = valueProperties->Length(); j < limitj; j++) {
        v8::Local<v8::Value> propertyName = valueProperties->Get(j);
        v8::String::Utf8Value fieldName(propertyName);
        v8::Local<v8::Value> currentValue = values->Get(propertyName);

        if (j > 0) {
            query->sql << ",";
        }

        query->sql << (escape ? query->connection->escapeName(*fieldName) : *fieldName);
        query->sql << "=";
        query->sql << query->value(currentValue);
    }

    NanReturnValue(args.This());
}

NAN_METHOD(node_db::Query::Sql) {
    NanScope();

    node_db::Query *query = node::ObjectWrap::Unwrap<node_db::Query>(args.This());
    assert(query);

    NanReturnValue(v8::String::New(query->sql.str().c_str()));
}

NAN_METHOD(node_db::Query::Execute) {
    NanScope();

    node_db::Query *query = node::ObjectWrap::Unwrap<node_db::Query>(args.This());
    assert(query);

    if (args.Length() > 0) {
        v8::Handle<v8::Value> set = query->set(args);
        if (!set.IsEmpty()) {
            NanReturnValue(set);
        }
    }

    std::string sql;

    try {
        sql = query->parseQuery();
    } catch(const node_db::Exception& exception) {
        THROW_EXCEPTION(exception.what())
    }

    if (query->cbStart != NULL && !query->cbStart->GetFunction().IsEmpty()) {
        v8::Local<v8::Value> argv[1];
        argv[0] = v8::String::New(sql.c_str());

        v8::TryCatch tryCatch;
        v8::Handle<v8::Value> result = (*(query->cbStart->GetFunction()))->Call(v8::Context::GetCurrent()->Global(), 1, argv);
        if (tryCatch.HasCaught()) {
            node::FatalException(tryCatch);
        }

        if (!result->IsUndefined()) {
            if (result->IsFalse()) {
                NanReturnValue(v8::Undefined());
            } else if (result->IsString()) {
                v8::String::Utf8Value modifiedQuery(result->ToString());
                sql = *modifiedQuery;
            }
        }
    }

    if (!query->connection->isAlive(false)) {
        THROW_EXCEPTION("Can't execute a query without being connected")
    }

    execute_request_t *request = new execute_request_t();
    if (request == NULL) {
        THROW_EXCEPTION("Could not create EIO request")
    }

    query->sql.str("");
    query->sql.clear();
    query->sql << sql;

    NanAssignPersistent(v8::Object, request->context, args.This());
    request->query = query;
    request->buffered = false;
    request->result = NULL;
    request->rows = NULL;
    request->error = NULL;

    if (query->async) {
        request->query->Ref();

        uv_work_t* req = new uv_work_t();
        req->data = request;
        uv_queue_work(uv_default_loop(), req, uvExecute, (uv_after_work_cb)uvExecuteFinished);

#if NODE_VERSION_AT_LEAST(0, 7, 9)
        uv_ref((uv_handle_t *)&g_async);
#else
        uv_ref(uv_default_loop());
#endif

    } else {
        request->query->executeAsync(request);
    }

    NanReturnValue(v8::Undefined());
}

void node_db::Query::uvExecute(uv_work_t* uvRequest) {
    execute_request_t *request = static_cast<execute_request_t *>(uvRequest->data);
    assert(request);

    try {
        request->query->connection->lock();
        request->result = request->query->execute();
        request->query->connection->unlock();

        if (!request->result->isEmpty() && request->result != NULL) {
            request->rows = new std::vector<row_t*>();
            if (request->rows == NULL) {
                throw node_db::Exception("Could not create buffer for rows");
            }

            request->buffered = request->result->isBuffered();
            request->columnCount = request->result->columnCount();
            while (request->result->hasNext()) {
                unsigned long* columnLengths = request->result->columnLengths();
                char** currentRow = request->result->next();

                row_t* row = new row_t();
                if (row == NULL) {
                    throw node_db::Exception("Could not create buffer for row");
                }

                row->columnLengths = new unsigned long[request->columnCount];
                if (row->columnLengths == NULL) {
                    throw node_db::Exception("Could not create buffer for column lengths");
                }

                if (request->buffered) {
                    row->columns = currentRow;

                    for (uint16_t i = 0; i < request->columnCount; i++) {
                        row->columnLengths[i] = columnLengths[i];
                    }
                } else {
                    row->columns = new char*[request->columnCount];
                    if (row->columns == NULL) {
                        throw node_db::Exception("Could not create buffer for columns");
                    }

                    for (uint16_t i = 0; i < request->columnCount; i++) {
                        row->columnLengths[i] = columnLengths[i];
                        if (currentRow[i] != NULL) {
                            row->columns[i] = new char[row->columnLengths[i]];
                            if (row->columns[i] == NULL) {
                                throw node_db::Exception("Could not create buffer for column");
                            }
                            memcpy(row->columns[i], currentRow[i], row->columnLengths[i]);
                        } else {
                            row->columns[i] = NULL;
                        }
                    }
                }

                request->rows->push_back(row);
            }

            if (!request->result->isBuffered()) {
                request->result->release();
            }
        }
    } catch(const node_db::Exception& exception) {
        request->query->connection->unlock();
        Query::freeRequest(request, false);
        request->error = new std::string(exception.what());
    }
}

void node_db::Query::uvExecuteFinished(uv_work_t* uvRequest, int status) {
    NanScope();

    execute_request_t *request = static_cast<execute_request_t *>(uvRequest->data);
    assert(request);

    if (request->error == NULL && request->result != NULL) {
        v8::Local<v8::Value> argv[3];
        argv[0] = v8::Local<v8::Value>::New(v8::Null());

        bool isEmpty = request->result->isEmpty();
        if (!isEmpty) {
            assert(request->rows);

            size_t totalRows = request->rows->size();
            v8::Local<v8::Array> rows = v8::Array::New(totalRows);

            uint64_t index = 0;
            std::ostringstream reusableStream;
            for (std::vector<row_t*>::iterator iterator = request->rows->begin(), end = request->rows->end(); iterator != end; ++iterator, index++) {
                row_t* currentRow = *iterator;
                v8::Local<v8::Object> row = request->query->row(request->result, currentRow);
                v8::Local<v8::Value> eachArgv[3];

                eachArgv[0] = row;
                eachArgv[1] = v8StringFromUInt64(index, reusableStream);
                eachArgv[2] = v8::Local<v8::Value>::New((index == totalRows - 1) ? v8::True() : v8::False());

                request->query->Emit("each", 3, eachArgv);

                rows->Set(index, row);
            }

            v8::Local<v8::Array> columns = v8::Array::New(request->columnCount);
            for (uint16_t j = 0; j < request->columnCount; j++) {
                node_db::Result::Column *currentColumn = request->result->column(j);

                v8::Local<v8::Object> column = v8::Object::New();
                column->Set(v8::String::New("name"), v8::String::New(currentColumn->getName().c_str()));
                column->Set(v8::String::New("type"), NODE_CONSTANT(currentColumn->getType()));

                columns->Set(j, column);
            }

            argv[1] = rows;
            argv[2] = columns;
        } else {
            v8::Local<v8::Object> result = v8::Object::New();
            std::ostringstream reusableStream;
            result->Set(v8::String::New("id"), v8StringFromUInt64(request->result->insertId(), reusableStream));
            result->Set(v8::String::New("affected"), v8StringFromUInt64(request->result->affectedCount(), reusableStream));
            result->Set(v8::String::New("warning"), v8StringFromUInt64(request->result->warningCount(), reusableStream));
            argv[1] = result;
        }

        request->query->Emit("success", !isEmpty ? 2 : 1, &argv[1]);

        if (request->query->cbExecute != NULL && !request->query->cbExecute->GetFunction().IsEmpty()) {
            v8::TryCatch tryCatch;
            (*(request->query->cbExecute->GetFunction()))->Call(NanPersistentToLocal(request->context), !isEmpty ? 3 : 2, argv);
            if (tryCatch.HasCaught()) {
                node::FatalException(tryCatch);
            }
        }
    } else {
        v8::Local<v8::Value> argv[1];
        argv[0] = v8::String::New(request->error != NULL ? request->error->c_str() : "(unknown error)");

        request->query->Emit("error", 1, argv);

        if (request->query->cbExecute != NULL && !request->query->cbExecute->GetFunction().IsEmpty()) {
            v8::TryCatch tryCatch;
            (*(request->query->cbExecute->GetFunction()))->Call(NanPersistentToLocal(request->context), 1, argv);
            if (tryCatch.HasCaught()) {
                node::FatalException(tryCatch);
            }
        }
    }

    if (request->query->cbFinish != NULL && !request->query->cbFinish->GetFunction().IsEmpty()) {
        v8::TryCatch tryCatch;
        (*(request->query->cbFinish->GetFunction()))->Call(v8::Context::GetCurrent()->Global(), 0, NULL);
        if (tryCatch.HasCaught()) {
            node::FatalException(tryCatch);
        }
    }

#if NODE_VERSION_AT_LEAST(0, 7, 9)
    uv_unref((uv_handle_t *)&g_async);
#else
    uv_unref(uv_default_loop());
#endif

    request->query->Unref();

    Query::freeRequest(request);
}

void node_db::Query::executeAsync(execute_request_t* request) {
    bool freeAll = true;
    try {
        this->connection->lock();
        request->result = this->execute();
        this->connection->unlock();

        if (request->result != NULL) {
            v8::Local<v8::Value> argv[3];
            argv[0] = v8::Local<v8::Value>::New(v8::Null());

            bool isEmpty = request->result->isEmpty();
            if (!isEmpty) {
                request->columnCount = request->result->columnCount();

                v8::Local<v8::Array> columns = v8::Array::New(request->columnCount);
                v8::Local<v8::Array> rows;
                try {
                    rows = v8::Array::New(request->result->count());
                } catch(const node_db::Exception& exception) {
                    rows = v8::Array::New();
                }

                for (uint16_t i = 0; i < request->columnCount; i++) {
                    node_db::Result::Column *currentColumn = request->result->column(i);

                    v8::Local<v8::Object> column = v8::Object::New();
                    column->Set(v8::String::New("name"), v8::String::New(currentColumn->getName().c_str()));
                    column->Set(v8::String::New("type"), NODE_CONSTANT(currentColumn->getType()));

                    columns->Set(i, column);
                }

                row_t row;
                uint64_t index = 0;
                std::ostringstream reusableStream;

                while (request->result->hasNext()) {
                    row.columnLengths = (unsigned long*) request->result->columnLengths();
                    row.columns = reinterpret_cast<char**>(request->result->next());

                    v8::Local<v8::Object> jsRow = this->row(request->result, &row);
                    v8::Local<v8::Value> eachArgv[3];

                    eachArgv[0] = jsRow;
                    eachArgv[1] = v8StringFromUInt64(index, reusableStream);
                    eachArgv[2] = v8::Local<v8::Value>::New(request->result->hasNext() ? v8::True() : v8::False());

                    this->Emit("each", 3, eachArgv);

                    rows->Set(index++, jsRow);
                }

                if (!request->result->isBuffered()) {
                    request->result->release();
                }

                argv[1] = rows;
                argv[2] = columns;
            } else {
                v8::Local<v8::Object> result = v8::Object::New();
                std::ostringstream reusableStream;
                result->Set(v8::String::New("id"), v8StringFromUInt64(request->result->insertId(), reusableStream));
                result->Set(v8::String::New("affected"), v8StringFromUInt64(request->result->affectedCount(), reusableStream));
                result->Set(v8::String::New("warning"), v8StringFromUInt64(request->result->warningCount(), reusableStream));
                argv[1] = result;
            }

            this->Emit("success", !isEmpty ? 2 : 1, &argv[1]);

            if (this->cbExecute != NULL && !this->cbExecute->GetFunction().IsEmpty()) {
                v8::TryCatch tryCatch;
                (*(this->cbExecute->GetFunction()))->Call(NanPersistentToLocal(request->context), !isEmpty ? 3 : 2, argv);
                if (tryCatch.HasCaught()) {
                    node::FatalException(tryCatch);
                }
            }
        }
    } catch(const node_db::Exception& exception) {
        this->connection->unlock();

        v8::Local<v8::Value> argv[1];
        argv[0] = v8::String::New(exception.what());

        this->Emit("error", 1, argv);

        if (this->cbExecute != NULL && !this->cbExecute->GetFunction().IsEmpty()) {
            v8::TryCatch tryCatch;
            (*(this->cbExecute->GetFunction()))->Call(NanPersistentToLocal(request->context), 1, argv);
            if (tryCatch.HasCaught()) {
                node::FatalException(tryCatch);
            }
        }

        freeAll = false;
    }

    Query::freeRequest(request, freeAll);
}

node_db::Result* node_db::Query::execute() const throw(node_db::Exception&) {
    return this->connection->query(this->sql.str());
}

void node_db::Query::freeRequest(execute_request_t* request, bool freeAll) {
    if (request->rows != NULL) {
        for (std::vector<row_t*>::iterator iterator = request->rows->begin(), end = request->rows->end(); iterator != end; ++iterator) {
            row_t* row = *iterator;
            if (!request->buffered) {
                for (uint16_t i = 0; i < request->columnCount; i++) {
                    if (row->columns[i] != NULL) {
                        delete row->columns[i];
                    }
                }
                delete [] row->columns;
            }
            delete [] row->columnLengths;
            delete row;
        }

        delete request->rows;
    }

    if (request->error != NULL) {
        delete request->error;
    }

    if (freeAll) {
        if (request->result != NULL) {
            delete request->result;
        }

        NanDispose(request->context);

        delete request;
    }
}

#undef THROW_EXCEPTION
#define THROW_EXCEPTION(message) \
    return v8::ThrowException(v8::Exception::Error(v8::String::New(message)));

v8::Local<v8::Value> node_db::Query::set(_NAN_METHOD_ARGS) {
    NanScope();

    if (args.Length() == 0) {
        return v8::Handle<v8::Value>();
    }

    int queryIndex = -1, optionsIndex = -1, valuesIndex = -1, callbackIndex = -1;

    if (args.Length() > 3) {
        ARG_CHECK_STRING(0, query);
        ARG_CHECK_ARRAY(1, values);
        ARG_CHECK_FUNCTION(2, callback);
        ARG_CHECK_OBJECT(3, options);
        queryIndex = 0;
        valuesIndex = 1;
        callbackIndex = 2;
        optionsIndex = 3;
    } else if (args.Length() == 3) {
        ARG_CHECK_STRING(0, query);
        queryIndex = 0;
        if (args[2]->IsFunction()) {
            ARG_CHECK_FUNCTION(2, callback);
            if (args[1]->IsArray()) {
                ARG_CHECK_ARRAY(1, values);
                valuesIndex = 1;
            } else {
                ARG_CHECK_OBJECT(1, options);
                optionsIndex = 1;
            }
            callbackIndex = 2;
        } else {
            ARG_CHECK_STRING(0, query);
            ARG_CHECK_ARRAY(1, values);
            ARG_CHECK_OBJECT(2, options);
            valuesIndex = 1;
            optionsIndex = 2;
        }
    } else if (args.Length() == 2) {
        if (args[1]->IsFunction()) {
            ARG_CHECK_FUNCTION(1, callback);
            callbackIndex = 1;
        } else if (args[1]->IsArray()) {
            ARG_CHECK_ARRAY(1, values);
            valuesIndex = 1;
        } else {
            ARG_CHECK_OBJECT(1, options);
            optionsIndex = 1;
        }

        if (args[0]->IsFunction() && callbackIndex == -1) {
            ARG_CHECK_FUNCTION(0, callback);
            callbackIndex = 0;
        } else {
            ARG_CHECK_STRING(0, query);
            queryIndex = 0;
        }
    } else if (args.Length() == 1) {
        if (args[0]->IsString()) {
            ARG_CHECK_STRING(0, query);
            queryIndex = 0;
        } else if (args[0]->IsFunction()) {
            ARG_CHECK_FUNCTION(0, callback);
            callbackIndex = 0;
        } else if (args[0]->IsArray()) {
            ARG_CHECK_ARRAY(0, values);
            valuesIndex = 0;
        } else {
            ARG_CHECK_OBJECT(0, options);
            optionsIndex = 0;
        }
    }

    if (queryIndex >= 0) {
        v8::String::Utf8Value initialSql(args[queryIndex]->ToString());
        this->sql.str("");
        this->sql.clear();
        this->sql << *initialSql;
    }

    if (optionsIndex >= 0) {
        v8::Local<v8::Object> options = args[optionsIndex]->ToObject();

        ARG_CHECK_OBJECT_ATTR_OPTIONAL_BOOL(options, async);
        ARG_CHECK_OBJECT_ATTR_OPTIONAL_BOOL(options, cast);
        ARG_CHECK_OBJECT_ATTR_OPTIONAL_BOOL(options, bufferText);
        ARG_CHECK_OBJECT_ATTR_OPTIONAL_FUNCTION(options, start);
        ARG_CHECK_OBJECT_ATTR_OPTIONAL_FUNCTION(options, finish);

        if (options->Has(async_key)) {
            this->async = options->Get(async_key)->IsTrue();
        }

        if (options->Has(cast_key)) {
            this->cast = options->Get(cast_key)->IsTrue();
        }

        if (options->Has(bufferText_key)) {
            this->bufferText = options->Get(bufferText_key)->IsTrue();
        }

        if (options->Has(start_key)) {
            if (this->cbStart != NULL) {
                delete this->cbStart;
            }
            this->cbStart = new NanCallback(options->Get(start_key).As<v8::Function>());
        }

        if (options->Has(finish_key)) {
            if (this->cbFinish != NULL) {
                delete this->cbFinish;
            }
            this->cbFinish = new NanCallback(options->Get(finish_key).As<v8::Function>());
        }
    }

    if (valuesIndex >= 0) {
        v8::Local<v8::Array> values = args[valuesIndex].As<v8::Array>();
        for (uint32_t i = 0, limiti = values->Length(); i < limiti; i++) {
            NanAssignPersistent(v8::Value, this->values[i], values->Get(i));
        }
    }

    if (callbackIndex >= 0) {
        this->cbExecute = new NanCallback(args[callbackIndex].As<v8::Function>());
    }

    return v8::Handle<v8::Value>();
}

std::string node_db::Query::fieldName(v8::Local<v8::Value> value) const throw(node_db::Exception&) {
    std::string buffer;

    if (value->IsObject()) {
        v8::Local<v8::Object> valueObject = value->ToObject();
        v8::Local<v8::Array> valueProperties = valueObject->GetPropertyNames();
        if (valueProperties->Length() == 0) {
            throw node_db::Exception("Non empty objects should be used for value aliasing in select");
        }

        for (uint32_t j = 0, limitj = valueProperties->Length(); j < limitj; j++) {
            v8::Local<v8::Value> propertyName = valueProperties->Get(j);
            v8::String::Utf8Value fieldName(propertyName);

            v8::Local<v8::Value> currentValue = valueObject->Get(propertyName);
            if (currentValue->IsObject() && !currentValue->IsArray() && !currentValue->IsFunction() && !currentValue->IsDate()) {
                v8::Local<v8::Object> currentObject = currentValue->ToObject();
                v8::Local<v8::String> escapeKey = v8::String::New("escape");
                v8::Local<v8::String> valueKey = v8::String::New("value");
                v8::Local<v8::String> precisionKey = v8::String::New("precision");
                v8::Local<v8::Value> optionValue;
                bool escape = false;
                int precision = -1;

                if (!currentObject->Has(valueKey)) {
                    throw node_db::Exception("The \"value\" option for the select field object must be specified");
                }

                if (currentObject->Has(escapeKey)) {
                    optionValue = currentObject->Get(escapeKey);
                    if (!optionValue->IsBoolean()) {
                        throw node_db::Exception("Specify a valid boolean value for the \"escape\" option in the select field object");
                    }
                    escape = optionValue->IsTrue();
                }

                if (currentObject->Has(precisionKey)) {
                    optionValue = currentObject->Get(precisionKey);
                    if (!optionValue->IsNumber() || optionValue->IntegerValue() < 0) {
                        throw new node_db::Exception("Specify a number equal or greater than 0 for precision");
                    }
                    precision = optionValue->IntegerValue();
                }

                if (j > 0) {
                    buffer += ',';
                }

                buffer += this->value(currentObject->Get(valueKey), false, escape, precision);
            } else {
                if (j > 0) {
                    buffer += ',';
                }

                buffer += this->value(currentValue, false, currentValue->IsString() ? false : true);
            }

            buffer += " AS ";
            buffer += this->connection->escapeName(*fieldName);
        }
    } else if (value->IsString()) {
        v8::String::Utf8Value fieldName(value->ToString());
        buffer += this->connection->escapeName(*fieldName);
    } else {
        throw node_db::Exception("Incorrect value type provided as field for select");
    }

    return buffer;
}

std::string node_db::Query::tableName(v8::Local<v8::Value> value, bool escape) const throw(node_db::Exception&) {
    std::string buffer;

    if (value->IsArray()) {
        v8::Local<v8::Array> tables = value.As<v8::Array>();
        if (tables->Length() == 0) {
            throw node_db::Exception("No tables specified");
        }

        for (uint32_t i = 0, limiti = tables->Length(); i < limiti; i++) {
            if (i > 0) {
                buffer += ',';
            }

            buffer += this->tableName(tables->Get(i), escape);
        }
    } else if (value->IsObject()) {
        v8::Local<v8::Object> valueObject = value->ToObject();
        v8::Local<v8::Array> valueProperties = valueObject->GetPropertyNames();
        if (valueProperties->Length() == 0) {
            throw node_db::Exception("Non empty objects should be used for aliasing");
        }

        v8::Local<v8::Value> propertyName = valueProperties->Get(0);
        v8::Local<v8::Value> propertyValue = valueObject->Get(propertyName);

        if (!propertyName->IsString() || !propertyValue->IsString()) {
            throw node_db::Exception("Only strings are allowed for table / alias name");
        }

        v8::String::Utf8Value table(propertyValue);
        v8::String::Utf8Value alias(propertyName);

        buffer += (escape ? this->connection->escapeName(*table) : *table);
        buffer += " AS ";
        buffer += (escape ? this->connection->escapeName(*alias) : *alias);
    } else {
        v8::String::Utf8Value tables(value->ToString());

        buffer += (escape ? this->connection->escapeName(*tables) : *tables);
    }

    return buffer;
}

v8::Handle<v8::Value> node_db::Query::addCondition(_NAN_METHOD_ARGS, const char* separator) {
    ARG_CHECK_STRING(0, conditions);
    ARG_CHECK_OPTIONAL_ARRAY(1, values);

    v8::String::Utf8Value conditions(args[0]->ToString());
    std::string currentConditions = *conditions;
    if (args.Length() > 1) {
        v8::Local<v8::Array> currentValues = args[1].As<v8::Array>();
        for (uint32_t i = 0, limiti = currentValues->Length(); i < limiti; i++) {
            NanAssignPersistent(v8::Value, this->values[i], currentValues->Get(i));
        }
    }

    this->sql << " " << separator << " ";
    this->sql << currentConditions;

    return args.This();
}

#undef THROW_EXCEPTION
#define THROW_EXCEPTION(message) \
    return NanThrowError(v8::String::New(message));


v8::Local<v8::Object> node_db::Query::row(node_db::Result* result, row_t* currentRow) const {
    v8::Local<v8::Object> row = v8::Object::New();

    for (uint16_t j = 0, limitj = result->columnCount(); j < limitj; j++) {
        node_db::Result::Column* currentColumn = result->column(j);
        v8::Local<v8::Value> value;

        if (currentRow->columns[j] != NULL) {
            const char* currentValue = currentRow->columns[j];
            unsigned long currentLength = currentRow->columnLengths[j];
            if (this->cast) {
                node_db::Result::Column::type_t columnType = currentColumn->getType();
                switch (columnType) {
                    case node_db::Result::Column::BOOL:
                        value = v8::Local<v8::Value>::New(currentValue == NULL || currentLength == 0 || currentValue[0] != '0' ? v8::True() : v8::False());
                        break;
                    case node_db::Result::Column::INT:
                        value = v8::String::New(currentValue, currentLength)->ToInteger();
                        break;
                    case node_db::Result::Column::NUMBER:
                        value = v8::String::New(currentValue, currentLength)->ToNumber();
                        break;
                    case node_db::Result::Column::TIME:
                        {
                            int hour, min, sec;
                            sscanf(currentValue, "%d:%d:%d", &hour, &min, &sec);
                            value = v8::Date::New(static_cast<uint64_t>((hour*60*60 + min*60 + sec) * 1000));
                        }
                        break;
                    case node_db::Result::Column::DATE:
                    case node_db::Result::Column::DATETIME:
                        // Code largely inspired from https://github.com/Sannis/node-mysql-libmysqlclient
                        try {
                            int day = 0, month = 0, year = 0, hour = 0, min = 0, sec = 0;
                            time_t rawtime;
                            struct tm timeinfo;

                            if (columnType == node_db::Result::Column::DATETIME) {
                                sscanf(currentValue, "%d-%d-%d %d:%d:%d", &year, &month, &day, &hour, &min, &sec);
                            } else {
                                sscanf(currentValue, "%d-%d-%d", &year, &month, &day);
                            }

                            time(&rawtime);
                            if (!localtime_r(&rawtime, &timeinfo)) {
                                throw node_db::Exception("Can't get local time");
                            }

                            if (!Query::gmtDeltaLoaded) {
                                int localHour, gmtHour, localMin, gmtMin;

                                localHour = timeinfo.tm_hour - (timeinfo.tm_isdst > 0 ? 1 : 0);
                                localMin = timeinfo.tm_min;

                                if (!gmtime_r(&rawtime, &timeinfo)) {
                                    throw node_db::Exception("Can't get GMT time");
                                }
                                gmtHour = timeinfo.tm_hour;
                                gmtMin = timeinfo.tm_min;

                                Query::gmtDelta = ((localHour - gmtHour) * 60 + (localMin - gmtMin)) * 60;
                                if (Query::gmtDelta <= -(12 * 60 * 60)) {
                                    Query::gmtDelta += 24 * 60 * 60;
                                } else if (Query::gmtDelta > (12 * 60 * 60)) {
                                    Query::gmtDelta -= 24 * 60 * 60;
                                }
                                Query::gmtDeltaLoaded = true;
                            }

                            timeinfo.tm_year = year - 1900;
                            timeinfo.tm_mon = month - 1;
                            timeinfo.tm_mday = day;
                            timeinfo.tm_hour = hour;
                            timeinfo.tm_min = min;
                            timeinfo.tm_sec = sec;

                            value = v8::Date::New(static_cast<double>(mktime(&timeinfo) + Query::gmtDelta) * 1000);
                        } catch(const node_db::Exception&) {
                            value = v8::String::New(currentValue, currentLength);
                        }
                        break;
                    case node_db::Result::Column::SET:
                        {
                            v8::Local<v8::Array> values = v8::Array::New();
                            std::istringstream stream(currentValue);
                            std::string item;
                            uint64_t index = 0;
                            std::ostringstream reusableStream;
                            while (std::getline(stream, item, ',')) {
                                if (!item.empty()) {
                                    values->Set(v8StringFromUInt64(index++, reusableStream), v8::String::New(item.c_str()));
                                }
                            }
                            value = values;
                        }
                        break;
                    case node_db::Result::Column::TEXT:
                        if (this->bufferText || currentColumn->isBinary()) {
                            value = v8::Local<v8::Value>::New(node::Buffer::New(v8::String::New(currentValue, currentLength)));
                        } else {
                            value = v8::String::New(currentValue, currentLength);
                        }
                        break;
                    default:
                        value = v8::String::New(currentValue, currentLength);
                        break;
                }
            } else {
                value = v8::String::New(currentValue, currentLength);
            }
        } else {
            value = v8::Local<v8::Value>::New(v8::Null());
        }
        row->Set(v8::String::New(currentColumn->getName().c_str()), value);
    }

    return row;
}

std::vector<std::string::size_type> node_db::Query::placeholders(std::string* parsed) const throw(node_db::Exception&) {
    std::string query = this->sql.str();
    std::vector<std::string::size_type> positions;
    char quote = 0;
    bool escaped = false;
    uint32_t delta = 0;

    *parsed = query;

    for (std::string::size_type i = 0, limiti = query.length(); i < limiti; i++) {
        char currentChar = query[i];
        if (escaped) {
            if (currentChar == '?') {
                parsed->replace(i - 1 - delta, 1, "");
                delta++;
            }
            escaped = false;
        } else if (currentChar == '\\') {
            escaped = true;
        } else if (quote && currentChar == quote) {
            quote = 0;
        } else if (!quote && (currentChar == this->connection->quoteString)) {
            quote = currentChar;
        } else if (!quote && currentChar == '?') {
            positions.push_back(i - delta);
        }
    }

    if (positions.size() != this->values.size()) {
        throw node_db::Exception("Wrong number of values to escape");
    }

    return positions;
}

std::string node_db::Query::parseQuery() const throw(node_db::Exception&) {
    std::string parsed;
    std::vector<std::string::size_type> positions = this->placeholders(&parsed);

    uint32_t index = 0, delta = 0;
    for (std::vector<std::string::size_type>::iterator iterator = positions.begin(), end = positions.end(); iterator != end; ++iterator, index++) {
        std::string value = this->value(NanPersistentToLocal(this->values[index]));

	if(!value.length()) {
		throw node_db::Exception("Internal error, atconstructorTemplateting to replace with zero length value");
	}

        parsed.replace(*iterator + delta, 1, value);
        delta += (value.length() - 1);
    }

    return parsed;
}

std::string node_db::Query::value(v8::Local<v8::Value> value, bool inArray, bool escape, int precision) const throw(node_db::Exception&) {
    std::ostringstream currentStream;

    if (value->IsNull()) {
        currentStream << "NULL";
    } else if (value->IsArray()) {
        v8::Local<v8::Array> array = value.As<v8::Array>();
        if (!inArray) {
            currentStream << '(';
        }
        for (uint32_t i = 0, limiti = array->Length(); i < limiti; i++) {
            v8::Local<v8::Value> child = array->Get(i);
            if (child->IsArray() && i > 0) {
                currentStream << "),(";
            } else if (i > 0) {
                currentStream << ',';
            }

            currentStream << this->value(child, true, escape);
        }
        if (!inArray) {
            currentStream << ')';
        }
    } else if (value->IsDate()) {
        currentStream << this->connection->quoteString << this->fromDate(v8::Date::Cast(*value)->NumberValue()) << this->connection->quoteString;
    } else if (value->IsObject()) {
        v8::Local<v8::Object> object = value->ToObject();
        v8::Handle<v8::String> valueKey = v8::String::New("value");
        v8::Handle<v8::String> escapeKey = v8::String::New("escape");

        if (object->Has(valueKey)) {
            v8::Handle<v8::String> precisionKey = v8::String::New("precision");
            int precision = -1;

            if (object->Has(precisionKey)) {
                v8::Local<v8::Value> optionValue = object->Get(precisionKey);
                if (!optionValue->IsNumber() || optionValue->IntegerValue() < 0) {
                    throw new node_db::Exception("Specify a number equal or greater than 0 for precision");
                }
                precision = optionValue->IntegerValue();
            }

            bool innerEscape = true;
            if (object->Has(escapeKey)) {
                v8::Local<v8::Value> escapeValue = object->Get(escapeKey);
                if (!escapeValue->IsBoolean()) {
                    throw node_db::Exception("Specify a valid boolean value for the \"escape\" option in the select field object");
                }
                innerEscape = escapeValue->IsTrue();
            }
            currentStream << this->value(object->Get(valueKey), false, innerEscape, precision);
        } else {
            v8::Handle<v8::String> sqlKey = v8::String::New("sql");
            if (!object->Has(sqlKey) || !object->Get(sqlKey)->IsFunction()) {
                throw node_db::Exception("Objects can't be converted to a SQL value");
            }

            node_db::Query *query = node::ObjectWrap::Unwrap<node_db::Query>(object);
            assert(query);
            if (escape) {
                currentStream << "(";
            }
            currentStream << query->sql.str();
            if (escape) {
                currentStream << ")";
            }
        }
    } else if (value->IsBoolean()) {
        currentStream << (value->IsTrue() ? '1' : '0');
    } else if (value->IsUint32() || value->IsInt32() || (value->IsNumber() && value->NumberValue() == value->IntegerValue())) {
        currentStream << value->IntegerValue();
    } else if (value->IsNumber()) {
        if (precision == -1) {
            v8::String::Utf8Value currentString(value->ToString());
            currentStream << *currentString;
        } else {
            currentStream << std::fixed << std::setprecision(precision) << value->NumberValue();
        }
    } else if (value->IsString()) {
        v8::String::Utf8Value currentString(value->ToString());
        std::string string = *currentString;
        if (escape) {
            try {
                currentStream << this->connection->quoteString << this->connection->escape(string) << this->connection->quoteString;
            } catch(node_db::Exception& exception) {
                currentStream << this->connection->quoteString << string << this->connection->quoteString;
            }
        } else {
            currentStream << string;
        }
    } else {
        v8::String::Utf8Value currentString(value->ToString());
        std::string string = *currentString;
        throw node_db::Exception("Unknown type for to convert to SQL, converting `" + string + "'");
    }

    return currentStream.str();
}

std::string node_db::Query::fromDate(const double timeStamp) const throw(node_db::Exception&) {
    char* buffer = new char[20];
    if (buffer == NULL) {
        throw node_db::Exception("Can\'t create buffer to write parsed date");
    }


    struct tm timeinfo;
    time_t rawtime = (time_t) (timeStamp / 1000);
    if (!localtime_r(&rawtime, &timeinfo)) {
        throw node_db::Exception("Can't get local time");
    }

    strftime(buffer, 20, "%Y-%m-%d %H:%M:%S", &timeinfo);

    std::string date(buffer);
    delete [] buffer;

    return date;
}

