/*
 *   C++ sockets on Unix and Windows
 *   Copyright (C) 2002
 *
 *   This program is free software; you can redistribute it and/or modify
 *   it under the terms of the GNU General Public License as published by
 *   the Free Software Foundation; either version 2 of the License, or
 *   (at your option) any later version.
 *
 *   This program is distributed in the hope that it will be useful,
 *   but WITHOUT ANY WARRANTY; without even the implied warranty of
 *   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *   GNU General Public License for more details.
 *
 *   You should have received a copy of the GNU General Public License
 *   along with this program; if not, write to the Free Software
 *   Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 */

//#include "PracticalSocket.h"  // For Socket and SocketException
#include <iostream>           // For cerr and cout
#include <cstdlib>            // For atoi()
#include "../generated/comm.pb.h"
//#include <string.h>

#include "packedmessage.h"
//#include "stringdb.pb.h"
#include <cassert>
//#include <iostream>
#include <map>
#include <string>
#include <sstream>
#include <vector>
#include <boost/asio.hpp>
#include <boost/bind.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/cstdint.hpp>
#include <boost/enable_shared_from_this.hpp>
#include <pthread.h>
//using namespace std;

using namespace std;
namespace asio = boost::asio;
using asio::ip::tcp;
using boost::uint8_t;

const int RCVBUFSIZE = 1024;    // Size of receive buffer
class SockConn;
SockConn *conn;

Request* createRequest()
{
    Ping *ping = new Ping();
    ping->set_tag("test job");
    ping->set_number(5);
/*
    JobOperation *jobOp = new JobOperation();
    jobOp->set_action(JobOperation_JobAction_ADDJOB);

    JobDesc *jobDesc = new JobDesc();
    jobDesc->set_name_space("engineering");
    jobDesc->set_owner_id(0);
    jobDesc->set_job_id("zero");
    jobDesc->set_status(JobDesc_JobCode_JOBUNKNOWN);
*/
    Request *r = new Request();
    Payload *p = new Payload();
    p->set_allocated_ping(ping);
    //jobOp->set_allocated_data(jobDesc);
    //p->set_allocated_job_op(jobOp);
    r->set_allocated_body(p);

    Header *h = new Header();
    std::string *orignator = new std::string("client");
    h->set_allocated_originator(orignator);
    h->set_tag("test finger");
    h->set_routing_id(Header_Routing_JOBS);
    r->set_allocated_header(h);
    return r;

    //Message *msg = new Message();
    //Request *req = new Request();
}

class SockConn
{
public:
    typedef boost::shared_ptr<Request> RequestPointer;

    SockConn(asio::io_service& io_service, tcp::resolver::iterator endpoint_iterator)
        : m_socket(io_service),
        m_packed_request(boost::shared_ptr<Request> (new Request()))
    {
    }
    tcp::socket& get_socket()
    {
        return m_socket;
    }
    void sockConnect(tcp::resolver::iterator endpoint_iterator)
    {
        cout<<"connecting..."<<endl;
        tcp::resolver::iterator end;
        boost::system::error_code error = boost::asio::error::host_not_found;
        while(error && endpoint_iterator != end)
        {
            m_socket.close();
            m_socket.connect(*endpoint_iterator++,error);
        }
        if(error)
        {
            throw boost::system::system_error(error);
        }

    }


  void handle_request(Request *myreq)
    {

    cout<<"in handle_request()"<<endl;

        m_packed_request = boost::shared_ptr<Request> (myreq);
        RequestPointer req = m_packed_request.get_msg();

        vector<uint8_t> writebuf;
        PackedMessage<Request> resp_msg(req);
        resp_msg.pack(writebuf);
        asio::write(m_socket, asio::buffer(writebuf));


    }
    void handle_response()
    {
        cout<<"Handling Response"<<endl;
        m_packed_request.unpack(m_readbuf);
        RequestPointer req = m_packed_request.get_msg();
        Request *r = req.get();
        //cout<<endl<<(unsigned char*)r<<endl;
        if(r->has_body())
        {
            cout<<"Response success"<<endl;
            cout<<r->DebugString()<<endl;
        }
    }


    void handle_read_body(const boost::system::error_code& error)
    {
        (cerr << "handle body " << error << '\n');
        if (!error) {
            (cerr << "Got body!\n");
            (cerr << show_hex(m_readbuf) << endl);
            handle_response();
            //handle_request();
            //start_read_header();
        }
    }


    void start_read_body(unsigned msg_len)
    {
        // m_readbuf already contains the header in its first HEADER_SIZE
        // bytes. Expand it to fit in the body as well, and start async
        // read into the body.
        //
        boost::system::error_code error;
        m_readbuf.resize(HEADER_SIZE + msg_len);
        asio::mutable_buffers_1 buf = asio::buffer(&m_readbuf[HEADER_SIZE], msg_len);
        size_t len = m_socket.read_some(boost::asio::buffer(buf), error);
        handle_read_body(error);
        //asio::async_read(m_socket, buf,
          //      boost::bind(&SockConn::handle_read_body, this,
            //        asio::placeholders::error));
    }

    void handle_read_header(const boost::system::error_code& error)
    {
        (cerr << "handle read " << error.message() << '\n');
        if (!error) {
            (cerr << "Got header!\n");
            (cerr << show_hex(m_readbuf) << endl);
            unsigned msg_len = m_packed_request.decode_header(m_readbuf);
            (cerr << msg_len << " bytes\n");
            start_read_body(msg_len);
        }
    }

    void start_read_header()
    {
        cout<<"in start_read_header";
        m_readbuf.resize(HEADER_SIZE);

        boost::system::error_code error;
        size_t len = m_socket.read_some(boost::asio::buffer(m_readbuf), error);
        if(error)
        {
            cout<<"Header Read fialed";
        }
        else
        {
            cout<<"header read success";
            handle_read_header(error);

        }
        //asio::async_read(m_socket, asio::buffer(m_readbuf),
                //boost::bind(&SockConn::handle_read_header, this,
                  //  asio::placeholders::error));
    }

    private:

    tcp::socket m_socket;
    vector<uint8_t> m_readbuf;
    PackedMessage<Request> m_packed_request;


};


void *sendRequest(void* notused)
{
    int i = 1;
    while(i<4)
    {
        Request *req = createRequest();
        conn->handle_request(req);
        i++;
    }
}

void *readResponse(void *msg)
{   int i = 1;
    while(i<4)
    {
        conn->start_read_header();
    }
}

int main() {

    boost::asio::io_service io_service;

    tcp::resolver resolver(io_service);
    tcp::resolver::query query("192.168.0.24", "5572");
    tcp::resolver::iterator iterator1 = resolver.resolve(query);
    //Establish Connection
    conn = new SockConn(io_service,iterator1);
    conn->sockConnect(iterator1);
    cout<<"connected"<<endl;
    //Create and send Request
    Request *req = createRequest();
    pthread_t t1;
    pthread_create(&t1,NULL,&sendRequest,(void* )req);

    //Read Response
    pthread_t t2;
    pthread_create(&t2,NULL,&readResponse,(void*) "read Thread");

    pthread_join(t1,NULL);
    pthread_join(t2,NULL);

    sleep(1);
    return 0;
}



/*
Request* toRequest(unsigned char *bytes, int start_index, int length)
{
    //string *str = (string*)new char[length-start_index];
    //for(int i = start_index; i< length; i++)
    //    str[i-start_index] = bytes[i];
    cout<<endl<<"*********************"<<endl<<bytes<<endl<<"***************"<<endl;
    string str((const char*)bytes,length);
    cout<<endl<<"*********************"<<endl<<str<<endl<<"***************"<<endl;
    Request *req = new Request();
    req->ParseFromString(str.c_str());
    //cout<<*req;
    if(req->has_header())
        cout<<"it has header";
}
unsigned char* toBytes(Request &req, int len)
{
    //cout<<sizeof(req)<<endl;
    if(req.has_header())
        cout<<"Header is there";
    unsigned char* bytes = (unsigned char*)&req;
    unsigned char* output = new unsigned char[sizeof(req)+sizeof(len)];
    unsigned char a[4];// = len;
    int har = len;
    a[3] =  har & 0xff;
    a[2] = (har>>8)  & 0xff;
    a[1] = (har>>16) & 0xff;
    a[0] = (har>>24) & 0xff;
    //a[3] = 0x00 & 0xff;
    //a[2] = 0x00 & 0xff;;
    //a[1] = 0x00 & 0xff;;
    //a[0] = 0x00 & 0xff;;
    //cout<<"length in char is: "<<a;
    //long b = a;
    //count<<"length in long: "<<b;
    memcpy(output,&a,4);
    unsigned int i =0;
    //unsigned int b = *(output+7);
    //cout<<"output:"<<*output<<"  in int "<<b<<"    size of len"<<sizeof(len)<<endl;
    for(i = 0; i < sizeof(req); i++)
    {    output[i+sizeof(len)] = bytes[i];
        //cout <<output[i+sizeof(len)];
    }


    //output[i] = '\0';
    //cout<<"size of buffer: "<<i;
    return output;
}


  */
