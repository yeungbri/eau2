/*
 * Authors: Brian Yeung, Daniel Gao
 * Emails: yeung.bri@husky.neu.edu, gao.d@husky.neu.edu
 */

// lang::Cpp

#pragma once
#include "string.h"
#include "map.h"
#include "serializable.h"

/**
 * Stores serialized data and deserializes data
 */
class Archive : public Object {
public:
    char* _data;
    Map pointer_lookup;

    Archive() {
        _data = new char[0];
    }

    ~Archive() {
        delete[] _data;
    }

    // Helper method to append new data to data
    void append_data(char* other) {
        size_t other_len = strlen(other);
        size_t _data_len = strlen(_data);
        char* ret = new char[_data_len + other_len + 1];

        strcpy(ret, _data);
        strcat(ret, other);
        ret[_data_len + other_len] = '\0';

        delete[] _data;
        _data = ret;
    }

    // Serialization Methods
    
    // <size_t>12345</size_t>
    void s_size_t(size_t s) {
        append_data("<size_t>");
        char size_t_data[sizeof(s)];
        sprintf(size_t_data, "%zu", s);
        append_data(size_t_data);
        append_data("</size_t>");
    }

    //<double>0.12345</double>
    void s_double(double d) {
        append_data("<double>");
        char double_data[10];
        sprintf(double_data, "%lf" , d);
        append_data(double_data);
        append_data("</double>");
    }

    //<cstr>Hello</cstr>
    void s_cstr(char *cstr) {
        append_data("<c_str>");
        append_data(cstr);
        append_data("</c_str>");
    }

    // <string><size_t>5</size_t><c_str>Hello</c_str></string>
    void s_string(String s) {
        append_data("<string>");
        s_size_t(s.size_);
        s_cstr(s.c_str());
        append_data("</string>");
    }

    // <string_array><string>...</string><string>...</string><string>...</string></string_array>
    void s_string_array(StringArray sa) {
        append_data("<string_array>");
        s_size_t(sa.len_);
        for (int i=0; i<sa.len_; i++) {
            s_string(sa.vals_[i]);
        }
        append_data("</string_array>");
    }

    // <double_array><double>...</double><double>...</double></double_array>
    void s_double_array(DoubleArray da) {
        append_data("<double_array>");
        s_size_t(da.len_);
        for (int i=0; i<da.len_; i++) {
            s_double(da.vals_[i]);
        }
        append_data("</double_array>");
    }

    // <msg_kind>0</msg_kind>
    void s_msgkind(MsgKind m) {
        append_data("<msg_kind>");
        char msgkind_char;
        msgkind_char = ((int) m) + '0';
        append_data(&msgkind_char);
        append_data("</msg_kind>");
    }

    // <message><msg_kind>0</msg_kind><size_t>0</size_t><size_t>1</size_t><size_t>2</size_t></message>
    void s_message(Message m) {
        append_data("<message>");
        s_msgkind(m.kind_);
        s_size_t(m.sender_);
        s_size_t(m.target_);
        s_size_t(m.id_);
        append_data("</message>");
    }

    // <ack><message>...</message></ack>
    void s_ack(Ack am) {
        append_data("<ack>");
        s_message(Message(am.kind_, am.sender_, am.target_, am.id_));
        append_data("</ack>");
    }

    // <status><message>...</message><string>...</string></status>
    void s_status(Status sm) {
        append_data("<status>");
        s_message(Message(sm.kind_, sm.sender_, sm.target_, sm.id_));
        s_string(*sm.msg_);
        append_data("</status>");
    }

    // <sockaddr_in></sockaddr_in>
    void s_sockaddr_in(sockaddr_in s) {
        append_data("<sockaddr_in>");

        append_data("<sin_family>");
        char sf[10];
        sprintf(sf, "%u" , s.sin_family);
        append_data(sf);
        append_data("</sin_family>");

        append_data("<sin_port>");
        char sp[10];
        sprintf(sp, "%u" , s.sin_port);
        append_data(sp);
        append_data("</sin_port>");

        append_data("<sin_addr>");
        char sa[10];
        sprintf(sa, "%lu" , s.sin_addr.s_addr);
        append_data(sa);
        append_data("</sin_addr>");

        char sz[10];
        strcpy(sz, s.sin_zero);
        append_data(sz);

        append_data("</sockaddr_in>");
    }

    // <register><message>...</message><sockaddr_in>...</sockaddr_in><size_t>...</size_t></register>
    void s_register(Register rm) {
        append_data("<register>");
        s_message(Message(rm.kind_, rm.sender_, rm.target_, rm.id_));
        s_sockaddr_in(rm.client_);
        s_size_t(rm.port_);
        append_data("</register>");
    }

    // <directory><message>...</message><size_t>...</size_t><size_t>...</size_t><string_array>...</string_array></directory>
    void s_directory(Directory dm) {
        append_data("<directory>");
        s_message(Message(dm.kind_, dm.sender_, dm.target_, dm.id_));
        s_size_t(dm.client_);
        s_size_t(*dm.ports_);
        s_string_array(dm.addresses_);
        append_data("</directory>");
    }

    // Helper method to get data between delimiters
    // Attribution: https://stackoverflow.com/a/24696896/12602247 at 2/27 11:10AM
    void get_data(char *src, char *p1, char *p2, char *dst) {
        char *target = dst;
        char *start, *end;

        if ((start = strstr(src, p1)))
        {
            start += strlen(p1);
            if ((end = strstr(start, p2)))
            {
                memcpy(target, start, end - start);
                target[end - start] = '\0';
            }
        }
    }

    // Deserialization Methods
    size_t ds_size_t() {
        return ds_size_t(_data);
    }

    size_t ds_size_t(char *src) {
        char buffer[50];
        get_data(src, "<size_t>", "</size_t>", buffer);
        size_t ret;
        sscanf(buffer, "%zu", &ret);
        return ret;
    }

    double ds_double() {
        return ds_double(_data);
    }

    double ds_double(char *src) {
        char buffer[50];
        get_data(src, "<double>", "</double>", buffer);
        double ret;
        sscanf(buffer, "%lf", &ret);
        return ret;
    }

    void ds_cstr(char *src, char *dst) {
        get_data(src, "<c_str>", "</c_str>", dst);
    }

    String* ds_string() {
        return ds_string(_data);
    }

    String* ds_string(char *src) {
        char buffer[1024];
        get_data(src, "<string>", "</string>", buffer);
        size_t size = ds_size_t(buffer);
        char cstr_buf[1000];
        get_data(src, "<c_str>", "</c_str>", cstr_buf);
        return new String(cstr_buf, size);
    }

    char *split(char *str, char *delim) {
        char *p = strstr(str, delim);
        if (p == NULL) return NULL; // delimiter not found
        *p = '\0'; // terminate string after head
        return p + strlen(delim);
    }

    StringArray* ds_string_array() {
        return ds_string_array(_data);
    }

    StringArray* ds_string_array(char *src) {
        char buf[1024];
        get_data(src, "<string_array>", "</string_array>", buf);

        char *tail = split(buf, "</size_t>");
        char *head = tail;

        // recover size
        char *szt_str = new char[strlen(buf) + strlen("</size_t>") + 1];
        strcpy(szt_str, buf);
        strcat(szt_str, "</size_t>");
        size_t len = ds_size_t(szt_str);

        int i = 0;
        String *vals = new String[len];

        tail = split(head, "</string>");
        while (tail) {
            // grab first <string>...</string> and deserialize
            char *str = new char[strlen(head) + strlen("</string>") + 1];
            strcpy(str, head);
            strcat(str, "</string>");
            vals[i] = *ds_string(str);

            // split off the first <string>...</string> grabbed and continue
            head = tail;
            tail = split(head, "</string>");
            i++;
        }

        return new StringArray(vals, len);
    }

    DoubleArray* ds_double_array() {
        return ds_double_array(_data);
    }

    DoubleArray* ds_double_array(char *src) {
        char buf[1024];
        get_data(src, "<double_array>", "</double_array>", buf);

        char *tail = split(buf, "</size_t>");
        char *head = tail;

        // recover size
        char *szt_str = strdup(buf);
        strcat(szt_str, "</size_t>");
        size_t len = ds_size_t(szt_str);

        int i = 0;
        double *vals = new double[len];

        tail = split(head, "</double>");
        while (tail) {
            // grab first <double>...</double> and deserialize
            char *str = strdup(head);
            strcat(str, "</double>");
            vals[i] = ds_double(str);

            // split off the first <double>...</double> grabbed and continue
            head = tail;
            tail = split(head, "</double>");
            i++;
        }

        return new DoubleArray(vals, len);
    }

    MsgKind ds_msgkind() {
        return ds_msgkind(_data);
    }

    MsgKind ds_msgkind(char *src) {
        char buffer[20];
        get_data(src, "<msg_kind>", "</msg_kind>", buffer);
        int val = atoi(buffer);
        MsgKind ret = static_cast<MsgKind>(val);
        return ret;
    }

    Message* ds_message() {
        return ds_message(_data);
    }

    Message* ds_message(char *src) {
        char buffer[1024];
        char* message = "<message>";
        char* messageClose = "</message>";
        get_data(src, message, messageClose, buffer);
        
        char *tail = split(buffer, "</msg_kind>");
        char *head = tail;

        // recover msgkind
        char *mk_str = new char[strlen(buffer) + strlen("</msg_kind>") + 1];
        strcpy(mk_str, buffer);
        strcat(mk_str, "</msg_kind>");
        MsgKind mk = ds_msgkind(mk_str);
        delete[] mk_str;

        // recover sender
        tail = split(head, "</size_t>");
        char *s_str = new char[strlen(head) + strlen("</size_t>") + 1];
        strcpy(s_str, head);
        strcat(s_str, "</size_t>");
        size_t sender = ds_size_t(s_str);
        delete[] s_str;
        head = tail;

        // recover target
        tail = split(head, "</size_t>");
        char *t_str = strdup(head);
        strcat(t_str, "</size_t>");
        size_t target = ds_size_t(t_str);
        head = tail;

        // recover id
        tail = split(head, "</size_t>");
        char *i_str = strdup(head);
        strcat(i_str, "</size_t>");
        size_t id = ds_size_t(i_str);
        head = tail;

        return new Message(mk, sender, target, id);
    }

    Ack* ds_ack() {
        return ds_ack(_data);
    }

    Ack* ds_ack(char *src) {
        char buffer[1024];
        get_data(src, "<ack>", "</ack>", buffer);
        Message* m = ds_message(buffer);
        Ack* ret = new Ack(m->kind_, m->sender_, m->target_, m->id_);
        delete m;
        return ret;
    }

    Status* ds_status() {
        return ds_status(_data);
    }

    Status* ds_status(char *src) {
        char buffer[1024];
        get_data(src, "<status>", "</status>", buffer);

        // extract message
        char *tail = split(buffer, "</message>");
        char *head = tail;

        char *msg_str = new char[strlen(buffer) + strlen("</message>") + 1];
        strcpy(msg_str, buffer);
        strcat(msg_str, "</message>");
        Message *m = ds_message(msg_str);
        delete[] msg_str;

        // extract status string
        tail = split(head, "</string>");
        char *sm_str = new char[strlen(head) + strlen("</string>") + 1];
        strcpy(sm_str, head);
        strcat(sm_str, "</string>");
        String* status_msg = ds_string(sm_str);
        delete[] sm_str;
        head = tail;

        Status* ret = new Status(m->kind_, m->sender_, m->target_, m->id_, status_msg);
        delete m;
        return ret;
    }

    sockaddr_in ds_sockaddr_in(char *src) {
        sockaddr_in ret;
        char buffer[1024];
        get_data(src, "<sockaddr_in>", "</sockaddr_in>", buffer);

        char *tail = split(buffer, "</sin_family>");
        char *head = tail;

        char val[10];
        char *sf_str = new char[strlen(buffer) + strlen("</sin_family>") + 1];
        strcpy(sf_str, buffer);
        strcat(sf_str, "</sin_family>");
        get_data(sf_str, "<sin_family>", "</sin_family>", val);
        int sf = atoi(val);
        ret.sin_family = sf;
        delete[] sf_str;

        tail = split(head, "</sin_port>");
        char *sp_str = new char[strlen(head) + strlen("</sin_port>") + 1];
        strcpy(sp_str, head);
        strcat(sp_str, "</sin_port>");
        get_data(sp_str, "<sin_port>", "</sin_port>", val);
        int sp = atoi(val);
        ret.sin_port = sp;
        delete[] sp_str;
        head = tail;

        tail = split(head, "</sin_addr>");
        char *sa_str = new char[strlen(head) + strlen("</sin_addr>") + 1];
        strcpy(sa_str, head);
        strcat(sa_str, "</sin_addr>");
        get_data(sa_str, "<sin_addr>", "</sin_addr>", val);
        int sa = atoi(val);
        ret.sin_addr.s_addr = sa;
        delete[] sp_str;
        head = tail;

        return ret;
    }

    Register* ds_register() {
        return ds_register(_data);
    }

    Register* ds_register(char *src) {
        char buffer[1024];
        get_data(src, "<register>", "</register>", buffer);

        // extract message
        char *tail = split(buffer, "</message>");
        char *head = tail;

        char *msg_str = new char[strlen(buffer) + strlen("</message>") + 1];
        strcpy(msg_str, buffer);
        strcat(msg_str, "</message>");
        Message *m = ds_message(msg_str);
        delete[] msg_str;

        // extract client
        tail = split(head, "</sockaddr_in>");
        char *client_str = new char[strlen(head) + strlen("</sockaddr_in>") + 1];
        strcpy(client_str, head);
        strcat(client_str, "</sockaddr_in>");
        sockaddr_in client = ds_sockaddr_in(client_str);
        delete[] client_str;
        head = tail;

        // extract port
        tail = split(head, "</size_t>");
        char *port_str = new char[strlen(head) + strlen("</size_t>") + 1];
        strcpy(port_str, head);
        strcat(port_str, "</size_t>");
        size_t port = ds_size_t(port_str);
        delete[] port_str;
        head = tail;

        return new Register(m->kind_, m->sender_, m->target_, m->id_, client, port);
    }

    Directory* ds_directory() {
        return ds_directory(_data);
    }

    Directory* ds_directory(char *src) {
        char buffer[1024];
        get_data(src, "<directory>", "</directory>", buffer);

        // extract message
        char *tail = split(buffer, "</message>");
        char *head = tail;

        char *msg_str = new char[strlen(buffer) + strlen("</message>") + 1];
        strcpy(msg_str, buffer);
        strcat(msg_str, "</message>");
        Message *m = ds_message(msg_str);
        delete[] msg_str;

        // extract client
        tail = split(head, "</size_t>");
        char *client_str = new char[strlen(head) + strlen("</size_t>") + 1];
        strcpy(client_str, head);
        strcat(client_str, "</size_t>");
        size_t client = ds_size_t(client_str);
        delete[] client_str;
        head = tail;

        // extract ports
        tail = split(head, "</size_t>");
        char *ports_str = new char[strlen(head) + strlen("</size_t>") + 1];
        strcpy(ports_str, head);
        strcat(ports_str, "</size_t>");
        size_t *ports = new size_t(ds_size_t(ports_str));
        delete[] ports_str;
        head = tail;

        // extract addresses
        tail = split(head, "</string_array>");
        char *addrs_str = new char[strlen(head) + strlen("</string_array>") + 1];
        strcpy(addrs_str, head);
        strcat(addrs_str, "</string_array>");
        StringArray *addresses = ds_string_array(addrs_str);
        delete[] addrs_str;
        head = tail;

        Directory* ret = new Directory(m->kind_, m->sender_, m->target_, m->id_, client, ports, *addresses);
        delete m;
        return ret;
    }
};