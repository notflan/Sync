using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Threading;

using System.IO;
using System.Runtime.Serialization;

using Sync.Pipeline;

namespace Sync
{
    /// <summary>
    /// A serialisable hashtable for sending over Client's pipeline.
    /// Writes and reads itself from a stream blocking.
    /// </summary>
    public class Message
    {
        [Serializable]
        private class Internal : ISerializable
        {
            public Dictionary<string, object> Hashtable { get; set; }
            public Internal()
            {
                Hashtable = new Dictionary<string, object>();
            }

            public Internal(SerializationInfo info, StreamingContext context) :this()
            {
                lock (Hashtable)
                {
                    var keys = (string[])info.GetValue("__keys", typeof(string[]));

                    foreach (var key in keys)
                    {
                        var val = info.GetValue("K_" + key, typeof(object));
                        if (val is Internal)
                        {
                            Message m = new Message();
                            m.map = (Internal)val;
                            Hashtable.Add(key, m);
                        }
                        else
                            Hashtable.Add(key, val);
                    }
                }
            }

            public void GetObjectData(SerializationInfo info, StreamingContext context)
            {
                lock (Hashtable)
                {
                    info.AddValue("__keys", Hashtable.Keys.ToArray());

                    foreach (var kv in Hashtable)
                    {
                        if (kv.Value is Message)
                            info.AddValue("K_" + kv.Key, (kv.Value as Message).map);
                        else
                            info.AddValue("K_" + kv.Key, kv.Value);
                    }
                }
            }
        }
        internal Message(Stream s) : this()
        {
            Read(s);
        }

        private Internal map;

        /// <summary>
        /// The underlying hashtable for this message.
        /// </summary>
        public Dictionary<string, object> Hashtable { get { return map.Hashtable; } }

        /// <summary>
        /// Get a value from key. Returns null if key is not present.
        /// </summary>
        /// <returns></returns>
        public dynamic Get(string s)
        {
            if (Is(s, null))
                return Hashtable[s];
            else return null;
        }

        /// <summary>
        /// Check if key is present, and optionally if it is of type `t'.
        /// </summary>
        public bool Is(string s, Type t = null)
        {
            if (t == null) return Hashtable.ContainsKey(s);
            else if (Is(s, null)) return Get(s).GetType() == t;
            else return false;
        }

        /// <summary>
        /// Get a value of specific type. Returns default(T) if key does not exist or key is not of valid type.
        /// </summary>
        public T Get<T>(string name)
        {
            var d = Get(name);
            if (d == null) return default(T);
            else if (d is T) return (T)d;
            else return default(T);
        }

        /// <summary>
        /// Get a value of specific type. Returns `or' if key does not exist or key is not of valid type.
        /// </summary>
        public T Get<T>(string name, T or)
        {
            if (Is(name, typeof(T))) return Get(name);
            else return or;
        }

        /// <summary>
        /// Add new value to table.
        /// </summary>
        public Message Add<T>(string name, T d)
        {
            Hashtable.Add(name, d);
            return this;
        }
        /// <summary>
        /// Case for any keys in the table.
        /// Use like: Case("stringkey", new Action<string>((value)=> doStuff(value)),"stringkey2", new Action<string>((value)=> doStuff(value)),"intkey", new Action<int>((value)=> doStuffInt(value)));
        /// Note: Only the fist matching pair is called.
        /// </summary>
        /// <returns>True if handled, false if not.</returns>
        public bool Case(params dynamic[] stuff)
        {
            if (stuff.Length % 2 != 0) throw new InvalidOperationException("Bad case");

            for (int i = 0; i < stuff.Length; i++)
            {
                if (Is(stuff[i]))
                {
                    stuff[i + 1](Get(stuff[i]));
                    return true;
                }
            }
            return false;
        }

        private string toString(int indents)
        {
            StringBuilder sb = new StringBuilder();
            string indent = (indents < 1 ? "" : new string('\t', indents));
            string indent2 = (indents < 2 ? "" : new string('\t', indents-1));
            lock (Hashtable)
            {
                foreach (var kv in Hashtable)
                {
                    sb.Append(indent);
                    string str;

                    if (kv.Value is Message)
                        str = (kv.Value as Message).toString(indents + 1);
                    else str = kv.Value.ToString();

                    sb.Append("\"" + kv.Key + "\": " + str+",\n");
                }
            }

            return "{\n" + sb.ToString() + indent2 + "}";
        }

        /// <summary>
        /// Get the string represenation of this message.
        /// </summary>
        /// <returns></returns>
        public override string ToString()
        {
            return toString(1);
        }

        /// <summary>
        /// Create message with single key/value pair.
        /// </summary>
        public static Message Single<T>(string name, T value)
        {
            Message m = new Message();
            return m.Add(name, value);
        }

        /// <summary>
        /// Create new empty message.
        /// </summary>
        public Message()
        {
            map = new Internal();
        }

        private long readSize(Stream from)
        {
            byte[] sz = new byte[sizeof(long)];
            from.BlockingRead(sz, 0, sizeof(long));
            return BitConverter.ToInt32(sz, 0);
        }

        private void writeSize(Stream from, long i)
        {
            byte[] sz = BitConverter.GetBytes(i);
            from.Write(sz, 0, sizeof(long));
        }

        /// <summary>
        /// Read (blocking) from stream.
        /// </summary>
        public void Read(Stream from)
        {
            var size = readSize(from);
            if (size <= 0 || size > MaxSize) throw new InvalidDataException("Bad length.");
            else
            {
                lock (from)
                {
                    byte[] buffer = new byte[size];
                    from.BlockingRead(buffer, 0, buffer.Length);

                    using (MemoryStream ms = new MemoryStream(buffer))
                    {
                        ms.Position = 0;

                        var formatter = new System.Runtime.Serialization.Formatters.Binary.BinaryFormatter();
                        try
                        {
                            map = formatter.Deserialize(ms) as Internal;
                        }
                        catch (Exception e)
                        {
                            throw new InvalidDataException("Deserialisation failed.", e);
                        }
                        if (map == null) throw new InvalidDataException("Data received is of invalid type.");
                    }
                }
            }
        }

        /// <summary>
        /// Write to stream.
        /// </summary>
        public void Write(Stream to)
        {
            using (MemoryStream ms = new MemoryStream())
            {
                var formatter = new System.Runtime.Serialization.Formatters.Binary.BinaryFormatter();
                try
                {
                    formatter.Serialize(ms, map);
                }
                catch (Exception e)
                {
                    throw new InvalidDataException("Bad message: Could not serialise.", e);
                }

                ms.Position = 0;
                
                writeSize(to, ms.Length);
                ms.CopyTo(to);
            }
        }

        /// <summary>
        /// The max size allowed for reading messages. (default: int.MaxValue) (you probably shouldn't increase that)
        /// </summary>
        public static long MaxSize { get; set; } = int.MaxValue;
    }

    public static class Extensions
    {
        /// <summary>
        /// Read data from stream, block until all the data wanted is read.
        /// </summary>
        public static void BlockingRead(this Stream s, byte[] to, int offset, int length)
        {
            int read = 0;
            while ((read += s.Read(to, offset + read, length - read)) < length) ;
        }

        
    }

    /// <summary>
    /// Exception in Client I/O.
    /// </summary>
    public abstract class ClientException : Exception
    {
        public ClientException(Exception from, string s) : base(s, from) { }

        public override string ToString()
        {
            return this.GetType().Name+": "+InnerException.ToString();
        }
    }

    /// <summary>
    /// Exception in client writing.
    /// </summary>
    public class ClientWriteException : ClientException
    {
        public ClientWriteException(Exception from, string s) : base(from ,s ) { }
    }

    /// <summary>
    /// Exception in client reading.
    /// </summary>
    public class ClientReadException : ClientException
    {
        public ClientReadException(Exception from, string s) : base(from, s) { }
    }

    public class Preset : IDisposable
    {
        /// <summary>
        /// Function ran before operation has started.
        /// </summary>
        public Action Before { get; set; }
        /// <summary>
        /// Function ran after operation has completed.
        /// </summary>
        public Action After { get; set; }
        public Preset(Action start, Action end)
        {
            Before = start;
            After = end;
        }

        public Preset Use()
        {
            Before();
            return this;
        }

        public void Dispose()
        {
            After();
        }
    }

    /// <summary>
    /// Provides abstraction over reading and writing messages to/from a stream atomically.
    /// </summary>
    public class Client : IDisposable, IChannel<Message>
    {
        /// <summary>
        /// Channel for when message is received. Read from this.
        /// </summary>
        public Channel<Message> In { get; private set; }
        /// <summary>
        /// Channel to send message out. Write to this.
        /// </summary>
        public Channel<Message> Out { get; private set; }

        /// <summary>
        /// On send before/after funcs.
        /// </summary>
        public Preset OnSend { get; set; } = new Preset(() => { }, () => { });

        /// <summary>
        /// On receive before/after funcs.
        /// </summary>
        public Preset OnReceive { get; set; } = new Preset(() => { }, () => { });

        /// <summary>
        /// Signal name for new message read.
        /// </summary>
        public static readonly string SignalMessageReceived = "messageIn";

        /// <summary>
        /// Signal name for new message written.
        /// </summary>
        public static readonly string SignalMessageSent = "messageOut";

        /// <summary>
        /// Global signal handler on messages send/received.
        /// </summary>
        public Dispatcher<Message> SignalHandler { get; private set; }

        /// <summary>
        /// Is this connection alive?
        /// </summary>
        public bool Alive { get; private set; } = true;

        /// <summary>
        /// Event ran on closing (before resources released).
        /// </summary>
        public event Action OnClose;
        /// <summary>
        /// Event ran on exception (Read or Write call).
        /// </summary>
        public event Action<ClientException> OnException;
        
        private ManualResetEvent up;

        /// <summary>
        /// Wait until pipeline is set up.
        /// </summary>
        public void WaitOnInit()
        {
            up.WaitOne();
        }

        /// <summary>
        /// Keep backing stream alive after closing?
        /// </summary>
        public bool KeepAlive { get; set; }
        public Stream backing;
        /// <summary>
        /// Stream we are using.
        /// </summary>
        public Stream Backing { get { return backing; } }

        bool IChannel<Message>.IsOpen => Alive;

        /// <summary>
        /// Create a new client and setup pipeline asynchronously.
        /// </summary>
        /// <param name="stream">The stream to use.</param>
        /// <param name="keepAlive">Keep stream alive after closing?</param>
        /// <returns>The new client.</returns>
        public static Client Create(Stream stream, bool keepAlive =false)
        {
            var cli = new Client();
            cli.backing = stream;
            cli.KeepAlive = keepAlive;

            Dispatcher.Go(cli.Hook);

            return cli;
        }

        /// <summary>
        /// Initialise a new client. (note: this does not set up the pipeline, call client.Hook())
        /// </summary>
        /// <param name="stream">The stream to use.</param>
        /// <param name="keepAlive">Keep stream alive after closing?</param>
        public Client(Stream stream, bool keepAlive=false)
        {
            this.backing = stream;
            this.KeepAlive = keepAlive;
        }

        private Client()
        {
            SignalHandler = new Dispatcher<Message>();
            In = new Channel<Message>();
            Out = new Channel<Message>();
            up = new ManualResetEvent(false);
        }
        
        /// <summary>
        /// Set up the pipeline for this client. (note: this call blocks until the client closes, run it asyncrounously if you do not want that.)
        /// </summary>
        public void Hook()
        {
            Dispatcher.Go(() =>
            {
                while (Out.IsOpen)
                {
                    var msg = Out.Receive();
                    if (Out.IsOpen)
                    {
                        lock (backing)
                        {
                            try
                            {
                                using (OnSend.Use())
                                {
                                    msg.Write(backing);
                                }
                                _ = SignalHandler.Signal(SignalMessageSent, msg);

                            }
                            catch (Exception e) {if(OnException!=null) OnException(new ClientWriteException(e, "Error writing to stream")); }
                        }
                    }
                }
                if (!KeepAlive) backing.Dispose();
            });
            up.Set();
            while (In.IsOpen)
            {
                Message message = null;
                try
                {
                    using (OnReceive.Use())
                    {
                        message = new Message(backing);
                    }
                    if (In.IsOpen) {
                        In.Send(message);
                        _ = SignalHandler.Signal(SignalMessageReceived, message);
                    }
                }
                catch (Exception e)
                {
                    if (Alive && OnException!=null) OnException(new ClientReadException(e, "Error reading from stream"));
                }
            }
        }

        /// <summary>
        /// Close and release all resources used by this client.
        /// </summary>
        public void Dispose()
        {
            if (!Alive) return;

            up.WaitOne();

            if (OnClose != null)
                OnClose();

            Alive = false;

            up.Dispose();
            SignalHandler = new Dispatcher<Message>();
            Out.Close();
            In.Close();
        }

        /// <summary>
        /// Close and release all resources used by this client.
        /// </summary>
        public void Close()
        {
            Dispose();
        }

        /*void IChannel<Message>.Close()
        {
            Close();
        }*/

        public Message Receive()
        {
            return In.Receive();
        }

        public void Send(Message obj)
        {
            Out.Send(obj);
        }
    }
}
