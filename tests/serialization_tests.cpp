/*
 * Authors: Brian Yeung, Daniel Gao
 * Emails: yeung.bri@husky.neu.edu, gao.d@husky.neu.edu
 */

// lang::CwC

#include "string.h"
#include "serial.h"
#include "serializable.h"
#include "helper.h"

int main() {
    // serialization/deserialization examples

    // size_t
    Archive* a = new Archive();
    size_t test_size = 12345;
    a->s_size_t(test_size);
    size_t other = a->ds_size_t();
    
    assert(other == 12345);
    delete a;

    // double
    a = new Archive();
    double d = 0.12345;
    a->s_double(d);
    double d2 = a->ds_double();
    
    assert(d == d2);
    delete a;

    // String
    a = new Archive();
    String test_str("hello");
    a->s_string(test_str);
    String* str_other = a->ds_string();
    
    String *expect = new String("hello");
    assert(str_other->equals(expect) == true);
    delete str_other;
    delete expect;
    delete a;

    // StringArray
    a = new Archive();
    String s1("abcaaa");
    String s2("defaaa");
    String s3("ghiaaa");
    String* vals = new String[3];
    vals[0] = s1;
    vals[1] = s2;
    vals[2] = s3;

    StringArray sa(vals, 3);
    a->s_string_array(sa);
    StringArray *sa2 = a->ds_string_array();

    assert(sa2->vals_[0].equals(&vals[0]) == true);
    assert(sa2->vals_[1].equals(&vals[1]) == true);
    assert(sa2->vals_[2].equals(&vals[2]) == true);
    delete a;

    // DoubleArray
    a = new Archive();
    double d1 = 0.123;
    double d4 = 0.456;
    double d3 = 0.789;
    double* dvals = new double[3];
    dvals[0] = d1;
    dvals[1] = d4;
    dvals[2] = d3;

    DoubleArray da(dvals, 3);
    a->s_double_array(da);
    DoubleArray *da2 = a->ds_double_array();

    assert(da2->vals_[0] == d1);
    assert(da2->vals_[1] == d4);
    assert(da2->vals_[2] == d3);
    delete a;

    // MsgKind
    a = new Archive();
    MsgKind mk = MsgKind::Ack;
    a->s_msgkind(mk);
    MsgKind mk2 = a->ds_msgkind();
    
    assert(mk == mk2);
    delete a;

    // Message
    a = new Archive();
    size_t szt1 = 123;
    size_t szt2 = 456;
    size_t szt3 = 789;

    Message m(MsgKind::Ack, szt1, szt2, szt3);
    a->s_message(m);
    Message *m2 = a->ds_message();
    
    assert(m.kind_ == m2->kind_);
    assert(m.sender_ == m2->sender_);
    assert(m.target_ == m2->target_);
    assert(m.id_ == m2->id_);
    delete a;

    // Ack
    a = new Archive();
    szt1 = 99;
    szt2 = 98;
    szt3 = 97;

    Ack am(MsgKind::Ack, szt1, szt2, szt3);
    a->s_ack(am);
    Ack *am2 = a->ds_ack();
    
    assert(am.kind_ == am2->kind_);
    assert(am.sender_ == am2->sender_);
    assert(am.target_ == am2->target_);
    assert(am.id_ == am2->id_);
    delete a;

    // Status
    a = new Archive();
    szt1 = 89;
    szt2 = 88;
    szt3 = 87;

    String *status_msg = new String("Good");
    Status sm(MsgKind::Status, szt1, szt2, szt3, status_msg);
    a->s_status(sm);
    Status *sm2 = a->ds_status();
    
    assert(sm.kind_ == sm2->kind_);
    assert(sm.sender_ == sm2->sender_);
    assert(sm.target_ == sm2->target_);
    assert(sm.id_ == sm2->id_);
    assert(sm.msg_->equals(status_msg));
    delete status_msg;
    delete a;

    // Register
    a = new Archive();
    szt1 = 79;
    szt2 = 78;
    szt3 = 77;

    sockaddr_in client;
    client.sin_family = AF_INET;
    client.sin_port = 68;
    client.sin_addr.s_addr = 12345;
    size_t port = 123;

    Register rm(MsgKind::Register, szt1, szt2, szt3, client, port);
    a->s_register(rm);
    Register *rm2 = a->ds_register();

    assert(rm.kind_ == rm2->kind_);
    assert(rm.sender_ == rm2->sender_);
    assert(rm.target_ == rm2->target_);
    assert(rm.id_ == rm2->id_);
    assert(rm.client_.sin_family == rm2->client_.sin_family);
    assert(rm.client_.sin_port == rm2->client_.sin_port);
    assert(rm.client_.sin_addr.s_addr == rm2->client_.sin_addr.s_addr);
    assert(rm.port_ == rm2->port_);
    delete a;

    // Directory
    a = new Archive();
    szt1 = 69;
    szt2 = 68;
    szt3 = 67;

    size_t szt_client = 123;
    size_t *szt_ports = new size_t(456);
    
    String s4("abcaaa");
    String s5("defaaa");
    String s6("ghiaaa");
    vals = new String[3];
    vals[0] = s4;
    vals[1] = s5;
    vals[2] = s6;
    StringArray sa3(vals, 3);

    Directory dm(MsgKind::Directory, szt1, szt2, szt3, szt_client, szt_ports, sa3);
    a->s_directory(dm);
    Directory *dm2 = a->ds_directory();
    
    assert(dm.kind_ == dm2->kind_);
    assert(dm.sender_ == dm2->sender_);
    assert(dm.target_ == dm2->target_);
    assert(dm.id_ == dm2->id_);
    assert(dm.client_ == dm2->client_);
    assert(sa3.vals_[0].equals(&vals[0]) == true);
    assert(sa3.vals_[1].equals(&vals[1]) == true);
    assert(sa3.vals_[2].equals(&vals[2]) == true);
    delete a;

    return 0;
}