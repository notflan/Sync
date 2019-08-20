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
                        client =// Client.Create(new NetworkStream(listener.AcceptSocket(), true));
                        new Client(new NetworkStream(listener.AcceptSocket(), true));
                        Dispatcher.Go(client.Hook);
                        client.WaitOnInit();
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine("Exception on accept: " + e.Message);
                        continue;
                    }
                    client.OnLongStreamReadComplete += (t) =>
                    {
                        byte[] bytes = (t.Backing as MemoryStream).ToArray();
                        Console.WriteLine("LS: " + BitConverter.ToInt64(bytes, 0));
                        //t.Backing.Dispose();
                        //t.Backing = null;
                    };
                    Console.WriteLine("Server accepted client " + id);

                    client.SignalHandler.Hook(Client.SignalMessageReceived, (msg) =>
                    {
                        Console.WriteLine("Received message: " + msg.ToString());
                    });

                    client.OnClose += () =>
                    {
                        Console.WriteLine("Client " + id + " closing");
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


                    while (client.Alive)
                    {
                        try
                        {
                            var msg = client.In.Receive();
                            if (client.Alive)
                            {
                                if (msg.Is("chat", typeof(string)))
                                {
                                    Console.WriteLine("From " + id + ": " + msg.Get<string>("chat"));
                                }
                                if (msg.Is("adc"))
                                {
                                    var adc = msg.Get("adc");
                                    Console.WriteLine(adc.GetType().Name);
                                    if (adc is ADCString)
                                        Console.WriteLine((adc as ADCString).String);
                                }
                                if (msg.Is("_close"))
                                {
                                    Console.WriteLine(id + ": Server stoping");
                                    alive = false;
                                    client.Close();
                                    listener.Stop();
                                }
                                msg.Dispose();
                            }
                        }
                        catch (Exception)
                        {
                        }
                    }
                }
                dispatcher.SignalSync("kill");
                Console.WriteLine("Server: kill answered");
            });
        }

        static void client()
        {
            Socket sock = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
            sock.Connect(new IPEndPoint(IPAddress.Loopback, 20202));

            var client =// Client.Create(new NetworkStream(sock, true));
                new Client(new NetworkStream(sock, true));
            Dispatcher.Go(client.Hook);
            client.WaitOnInit();

            client.SignalHandler.Hook(Client.SignalMessageSent, (msg) =>
            {
                Console.WriteLine("Sent message: " + msg.ToString());
            });

            //bool alive = true;
            while (client.Alive)
            {
                Console.Write("> ");
                string line = Console.ReadLine().Trim();
                var strm = new MemoryStream();
                strm.Write(BitConverter.GetBytes((long)0xABAD1DEA), 0, sizeof(long));
                strm.Position = 0;
                var message = new Message();
                if (line.Equals("_close"))
                {
                    message.Add("_close", true);
                    //alive = false;
                }
                message.Add("chat", line);
                message.OnLongStreamWriteComplete += (t) =>
                {
                    //  t.Backing.Dispose();
                };
                if (line.Equals("long"))
                    message.Add("lstest", new Message.LongStream(strm, "lol lol mlmao"));
                else if (line.Equals("adc"))
                    message.Add("adc", new ADCString() { String = "Hello" });
                else strm.Dispose();

                client.Out.Send(message);
                message.Dispose();
            }
            client.Close();
        }

        static void fuck()
        {
            Channel<string> channel = new Channel<string>();

            Dispatcher.Go(() =>
            {
                while (channel.IsOpen)
                {
                    var recv = channel.Receive();
                    if (channel.IsOpen)
                    {
                        Console.WriteLine("str: " + recv);
                    }
                }
                Console.WriteLine("+++close");
            });

            while (channel.IsOpen)
            {
                var str = Console.ReadLine();
                if (str.ToLower().Trim().Equals("close"))
                    channel.Close();
                else
                    channel.Send(str);
            }
            Console.WriteLine("---close");

            Console.ReadKey();
        }
        static async Task testStream()
        {
            using (var stream = new ChanneledStream())
            {
                var receiver = Task.Run(() =>
                {
                    byte[] buffer = new byte[256];
                    int read = 0;
                    while ((read += stream.ReadForced(buffer, read, buffer.Length - read)) <256)
                    {
                        Console.WriteLine("READER: " + read);
                    }
                    Console.WriteLine("Reader read " + read + " bytes: ");
                    foreach (var b in buffer)
                        Console.Write(b + " ");
                    Console.WriteLine();
                });
                var sender = Task.Run(() =>
                {
                    byte[] nbuf = new byte[16].Select((x, i) => (byte)i).ToArray();
                    int wrote = 0;
                    while (wrote < 256)
                    {
                        stream.Write(nbuf, 0, 16);
                        wrote += 16;
                        Console.WriteLine("WRITER: " + wrote);
                    }
                    Console.WriteLine("Writer completed with " + wrote + " bytes written");
                });
                await Task.WhenAll(receiver, sender);
            }
        }
        static void Main(string[] args)
        {
            testStream().Wait();
           // fuck();
            //return;
            var t = server();

            mre.WaitOne();

            client();
            t.Wait();
            Console.ReadKey();
        }
    }
    public class ADCString : ArbitraryDataContainer
    {
        public string String { get; set; }
        public override void ReadStream(Stream from)
        {
            byte[] len = new byte[sizeof(int)];
            from.Read(len, 0, sizeof(int));
            byte[] serial = new byte[BitConverter.ToInt32(len, 0)];
            from.Read(serial, 0, serial.Length);

            String = Encoding.UTF8.GetString(serial);
        }

        public override void WriteStream(Stream to)
        {
            byte[] serial = Encoding.UTF8.GetBytes(String);
            byte[] len = BitConverter.GetBytes(serial.Length);

            to.Write(len, 0, sizeof(int));
            to.Write(serial, 0, serial.Length);
        }
    }
}
