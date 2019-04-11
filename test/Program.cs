using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Threading;

using Sync;
using Sync.Pipeline;
using System.IO;
using System.Net.Sockets;
using System.Net;

namespace test
{
    class Program
    {
        static ManualResetEvent mre = new ManualResetEvent(false);
        static Random random = new Random();
        static string uuid()
        {
            byte[] byt = new byte[8];
            random.NextBytes(byt);
            return Convert.ToBase64String(byt);
        }
        static async Task server()
        {
            TcpListener listener = new TcpListener(IPAddress.Loopback, 20202);
            Dispatcher dispatcher = new Dispatcher();

            listener.Start();
            mre.Set();
            Console.WriteLine("Server up");
            await Dispatcher.Go(() =>
            {
                bool alive = true;
                while (alive)
                {
                    Client client;
                    string id = uuid();
                    try
                    {
                        client = Client.Create(new NetworkStream(listener.AcceptSocket(), true));
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine("Exception on accept: " + e.Message);
                        continue;
                    }

                    Console.WriteLine("Server accepted client " + id);

                    client.SignalHandler.Hook(Client.SignalMessageReceived, (msg) =>
                    {
                        Console.WriteLine("Received message: " + msg.ToString());
                    });

                    client.OnClose += () =>
                    {
                        Console.WriteLine("Client " + id + " closing");
                    };

                    client.OnException += (e) =>
                    {
                        Console.WriteLine("Client " + id + " exception ("+e.InnerException.GetType().Name+"): " + e.Message);
                    };

                    Dispatcher.Callback kill = null;
                    dispatcher.Hook("kill", kill = () =>
                    {
                        if (client.Alive)
                        {
                            client.Close();
                            Console.WriteLine("Client " + id + " killed");
                        }
                        dispatcher.Unhook("kill", kill);
                    });

                    Dispatcher.Go(() =>
                    {
                        while (client.In.IsOpen)
                        {
                            
                            var msg = client.In.Receive();
                            if (client.In.IsOpen)
                            {
                                if (msg.Is("chat", typeof(string)))
                                {
                                    Console.WriteLine("From " + id + ": " + msg.Get<string>("chat"));
                                }
                                if (msg.Is("close"))
                                {
                                    Console.WriteLine(id + ": Server stoping");
                                    alive = false;
                                    listener.Stop();
                                }
                            }
                        }
                    });
                }
                dispatcher.SignalSync("kill");
                Console.WriteLine("Server: kill answered");
            });
        }

        static void client()
        {
            Socket sock = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
            sock.Connect(new IPEndPoint(IPAddress.Loopback, 20202));

            var client = Client.Create(new NetworkStream(sock, true));

            client.SignalHandler.Hook(Client.SignalMessageSent, (msg) =>
            {
                Console.WriteLine("Sent message: " + msg.ToString());
            });

            bool alive = true;
            while (alive)
            {
                Console.Write("> ");
                string line = Console.ReadLine().Trim();
                var message = new Message();
                if (line.Equals("close"))
                {
                    message.Add("close", true);
                    alive = false;
                }
                message.Add("chat", line);

                client.Out.Send(message);
            }
            client.Close();
        }

        static void Main(string[] args)
        {
            var t = server();

            mre.WaitOne();

            client();
            t.Wait();
            Console.ReadKey();
        }
    }
}
