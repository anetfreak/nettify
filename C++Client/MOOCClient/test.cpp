#include <iostream>
#include <stdio.h>
#include <ostream>

#include <boost/thread.hpp>
#include <boost/bind.hpp>
#include <boost/array.hpp>
#include <boost/asio.hpp>

using namespace std;

using boost::asio::ip::tcp;

class TCPClient
{
public:
    TCPClient(boost::asio::io_service& IO_Service, tcp::resolver::iterator EndPointIter);

    void Write();
    void Close();
private:
    boost::asio::io_service& m_IOService;
    tcp::socket m_Socket;

    boost::array<char, 128> m_Buffer;
    size_t m_BufLen;

private:
    void OnConnect(const boost::system::error_code& ErrorCode,
            tcp::resolver::iterator EndPointIter);

    void OnReceive(const boost::system::error_code& ErrorCode);

    void DoClose();
};

TCPClient::TCPClient(boost::asio::io_service& IO_Service, tcp::resolver::iterator EndPointIter)
: m_IOService(IO_Service), m_Socket(IO_Service)
{
	cout<<"connecting";
    tcp::endpoint EndPoint = *EndPointIter;

    m_Socket.async_connect(EndPoint,
            boost::bind(&TCPClient::OnConnect, this, boost::asio::placeholders::error, ++EndPointIter));
}

void TCPClient::Close()
{
    m_IOService.post(
        boost::bind(&TCPClient::DoClose, this));
}
void TCPClient::OnConnect(const boost::system::error_code& ErrorCode,
    tcp::resolver::iterator EndPointIter)
{
	cout<<"onconnect"<<endl;
    if (ErrorCode == 0)
    // Successful connected
    {
        m_Socket.async_receive(boost::asio::buffer(m_Buffer.data(), m_BufLen),
                boost::bind(&TCPClient::OnReceive, this, boost::asio::placeholders::error));


    } else if (EndPointIter != tcp::resolver::iterator())
    {
        m_Socket.close();
        tcp::endpoint EndPoint = *EndPointIter;

        m_Socket.async_connect(EndPoint,
                boost::bind(&TCPClient::OnConnect, this, boost::asio::placeholders::error, ++EndPointIter));
    }
}

void TCPClient::OnReceive(const boost::system::error_code& ErrorCode)
{
    if (ErrorCode == 0)
    {
        std::cout << m_Buffer.data() << std::endl;

        m_Socket.async_receive(boost::asio::buffer(m_Buffer.data(), m_BufLen),
                boost::bind(&TCPClient::OnReceive, this, boost::asio::placeholders::error));
    } else {
        DoClose();
    }
}

void TCPClient::DoClose()
{
    m_Socket.close();
}

int main()
{
    try {
        boost::asio::io_service IO_Service;

        tcp::resolver Resolver(IO_Service);
        tcp::resolver::query Query("127.0.0.1", "daytime");

        tcp::resolver::iterator EndPointIterator = Resolver.resolve(Query);

        TCPClient Client(IO_Service, EndPointIterator);

        //boost::thread ClientThread(
         //       boost::bind(&boost::asio::io_service::run, &IO_Service));

        std::cout << "Client started." << std::endl;

        std::string Input;
        while (Input != "exit")
        {
            std::cin >> Input;
        }

       // Client.Close();
        //ClientThread.join();
    } catch (std::exception& e)
    {
        std::cerr << e.what() << std::endl;
    }

}
