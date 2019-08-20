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
    public class Message : IDisposable
    {
        /// <summary>
        /// Dispose all values that implement IDisposable.
        /// </summary>
        public void Dispose()
        {
            foreach (var kv in map.Hashtable)
            {
                if (kv.Value is IDisposable)
                    (kv.Value as IDisposable).Dispose();
            }
        }
        /// <summary>
        /// A mechanism for sending buffered data form a Stream in a Message
        /// </summary>
        [Serializable]
        public class LongStream : ISerializable, IDisposable
        {
            /// <summary>
            /// A tag, can be used to communicate information about this Stream to the receiver.
            /// </summary>
            public string Tag { get; set; } = null;
            /// <summary>
            /// The underlying stream.
            /// </summary>
            public Stream Backing { get; set; }
            /// <summary>
            /// The current length of this stream.
            /// </summary>
            public long StreamLength { get; set; }
            /// <summary>
            /// This metadata's ID.
            /// </summary>
            public Guid UUID { get; set; }

            /// <summary>
            /// Do not dispose the underlying stream when disposing?
            /// </summary>
            public bool KeepAlive { get; set; } = false;

            /// <summary>
            /// Recalculate StreamLength.
            /// </summary>
            public void RecalculateLength()
            {
                StreamLength = Backing.Length;
            }

            /// <summary>
            /// Create a null LongStream.
            /// </summary>
            public LongStream()
            {
                Backing = null;
            }

            public override bool Equals(object obj)
            {
                if (obj == null) return false;
                else if (obj is LongStream)
                    (obj as LongStream).UUID.Equals(UUID);
                return base.Equals(obj);
            }

            public override int GetHashCode()
            {
                return UUID.GetHashCode();
            }

            /// <summary>
            /// Generate a new UUID for this Stream.
            /// </summary>
            public void Generate()
            {
                UUID = Guid.NewGuid();
            }

            /// <summary>
            /// Set backing Stream and recalculate length.
            /// </summary>
            /// <param name="copyFrom">The stream to use.</param>
            public void SetStream(Stream copyFrom)
            {
                Backing = copyFrom;
                StreamLength = copyFrom.Length;
            }

            /// <summary>
            /// Release all resources.
            /// </summary>
            public virtual void Dispose()
            {
                if (Backing != null && !KeepAlive)
                {
                    Backing.Dispose();
                    Backing = null;
                }
            }
            public override string ToString()
            {
                return UUID.ToString() + " <" + StreamLength + (Tag == null ? "" : ":" + Tag) + "> (" + ((object)Backing ?? "<disposed>").ToString() + ")";
            }
            /// <summary>
            /// Create a LongStream that referrences another stream.
            /// </summary>
            /// <param name="copyFrom">The stream to use.</param>
            public LongStream(Stream copyFrom)
            {
                Generate();
                SetStream(copyFrom);
                KeepAlive = true;
            }
            /// <summary>
            /// Create a LongStream that referrences another stream.
            /// </summary>
            /// <param name="copyFrom">The stream to use.</param>
            /// <param name="name">The tag to give this instance.</param>
            public LongStream(Stream copyFrom, string name)
            : this(copyFrom)
            {
                Tag = name;
            }
            public LongStream(SerializationInfo info, StreamingContext context)
            {
                Backing = null;
                UUID = Guid.Parse((string)info.GetValue("uuid", typeof(string)));
                StreamLength = (long)info.GetValue("length", typeof(long));
                Tag = (string)info.GetValue("tag", typeof(string));
                BlockSize = (int)info.GetValue("blocksize", typeof(int));
            }
            public void GetObjectData(SerializationInfo info, StreamingContext context)
            {
                info.AddValue("uuid", UUID.ToString());
                info.AddValue("length", StreamLength);
                info.AddValue("tag", Tag);
                info.AddValue("blocksize", BlockSize);
            }

            void writeString(Stream to, string thing)
            {
                byte[] b = Encoding.UTF8.GetBytes(thing);
                to.Write(BitConverter.GetBytes((int)b.Length), 0, sizeof(int));
                if(b.Length>0)
                to.Write(b, 0, b.Length);
            }

            string readString(Stream from)
            {
                byte[] _sz = new byte[sizeof(int)];
                from.BlockingRead(_sz, 0, _sz.Length);
                int sz = BitConverter.ToInt32(_sz, 0);
                if (sz < 0) throw new InvalidDataException("String size less than 0");
                if (sz == 0) return "";
                byte[] _buf = new byte[sz];
                from.BlockingRead(_buf, 0, sz);
                return Encoding.UTF8.GetString(_buf);
            }   

            public void WriteStream(Stream to)
            {
                if (Backing == null) throw new NullReferenceException("Backing stream cannot be null.");
                RecalculateLength();

                to.Write(UUID.ToByteArray(), 0, 16);
                to.Write(BitConverter.GetBytes(StreamLength), 0, sizeof(long));
                to.Write(BitConverter.GetBytes(BlockSize), 0, sizeof(int));
                writeString(to, Tag??"");

                //var pos = to.Position;
                //Backing.Position = 0;

                WriteRest(to);
                //Backing.Position = pos;
            }
            

            protected virtual void WriteRest(Stream to)
            {
                byte[] buffer = new byte[BlockSize];
                int rd;
                while ((rd = Backing.Read(buffer, 0, BlockSize)) > 0)
                {
                    WriteBlock(to, buffer, rd);
                }
            }

            protected void WriteBlock(Stream to, byte[] buffer, int rd)
            {
                to.Write(buffer, 0, rd);
            }

            /// <summary>
            /// Write block size.
            /// </summary>
            public int BlockSize { get; set; } = 4096;

            /// <summary>
            /// Maximum allocated memory for reading.
            /// </summary>
            public int MaxBlockSize { get; set; } = (1024 * 1024 * 1024);

            public Func<LongStream, Stream> StreamReadCreate { get; set; } = (t) => new MemoryStream();

            public void ReadStream(Stream from)
            {
                if (Backing == null) Backing = StreamReadCreate(this);
                var pos = Backing.Position;

                byte[] _uuid = new byte[16];
                byte[] _sz = new byte[sizeof(long)];
                byte[] _bs = new byte[sizeof(int)];

                from.BlockingRead(_uuid, 0, 16);
                from.BlockingRead(_sz, 0, sizeof(long));
                from.BlockingRead(_bs, 0, sizeof(int));
                string _str = readString(from);


                UUID = new Guid(_uuid);
                StreamLength = BitConverter.ToInt64(_sz, 0);
                BlockSize = BitConverter.ToInt32(_bs, 0);
                Tag = _str.Equals("") ? null : _str;

                if (BlockSize < 1 || BlockSize > MaxBlockSize)
                    throw new InvalidDataException("Block size received was invalid (" + BlockSize + "). Datastream corruption likely.");

                ReadRest(from);

                Backing.Position = pos;
            }

            protected int ReadBlock(Stream from, byte[] buffer, ref long read)
            {
                int rd;
                if (StreamLength - read < BlockSize)
                    rd = from.Read(buffer, 0, (int)(StreamLength % BlockSize));
                else rd = from.Read(buffer, 0, BlockSize);
                if (rd > 0)
                {
                    Backing.Write(buffer, 0, rd);
                    read += rd;
                }
                return rd;
            }

            protected virtual void ReadRest(Stream from)
            {
                int rd;
                byte[] buffer = new byte[BlockSize];
                long read = 0;
                while (read < StreamLength)
                {
                    rd = ReadBlock(from, buffer, ref read);
                }
            }
        }

        /// <summary>
        /// The function that a new Stream to write to when a LongStream is to be read. (By default returns a new MemoryStream).
        /// </summary>
        public Func<LongStream, Stream> LongStreamReadCreate { get; set; } = (t) => new MemoryStream();
        /// <summary>
        /// Called when a LongStream has been read.
        /// </summary>
        public event Action<LongStream> OnLongStreamReadComplete;
        /// <summary>
        /// Called after a LongStream has been written.
        /// </summary>
        public event Action<LongStream> OnLongStreamWriteComplete;

       

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
           // File.WriteAllText("fuck2", this.ToString());
            List<LongStream> templates = new List<LongStream>();
            List<string> keys = new List<string>();
            Dictionary<Guid, LongStream> reads = new Dictionary<Guid, LongStream>();
            
            List<ArbitraryDataContainer> c_templates = new List<ArbitraryDataContainer>();
            List<string> c_keys = new List<string>();

            foreach (var vl in map.Hashtable)
            {
                if (vl.Value is LongStream)
                {
                    templates.Add(vl.Value as LongStream);
                    keys.Add(vl.Key);
                }
                if (vl.Value is ArbitraryDataContainer)
                {
                    c_templates.Add(vl.Value as ArbitraryDataContainer);
                    c_keys.Add(vl.Key);
                }
            }

            //ADC
            if (c_templates.Count > 0)
            {
                for (int i = 0; i < c_templates.Count; i++)
                {
                    var template = new ArbitraryDataContainer.Template();
                    template.ReadStream(from);

                    int j = 0;
                    for (; j < c_templates.Count; j++)
                    {
                        var adc = c_templates[j];
                        if (adc.UUID.Equals(template.UUID))
                        {
                            //Match, adc is correct type
                            var ty = template.InvokeFrom(adc.GetType());
                            ty.ReadStream(from);
                            map.Hashtable[c_keys[j]] = ty;
                            break;
                        }
                    }
                    if (j >= c_templates.Count)
                        throw new InvalidDataException("Unexpected ADC UUID received.");
                }
            }

            //LongStream
            if (templates.Count > 0)
            {
                //expected
                for (int i = 0; i < templates.Count; i++)
                {
                    var ls = OnCreateLongStream();
                    ls.StreamReadCreate = LongStreamReadCreate;
                    ls.ReadStream(from);
                    OnLongStreamReadComplete(ls);
                    reads.Add(ls.UUID, ls);
                }
                //match them up
                //foreach (var l in templates)
                for(int i=0;i<templates.Count;i++)
                {
                    var l = templates[i];
                    if (reads.ContainsKey(l.UUID))
                    {
                        var match = reads[l.UUID];
                        if (match.StreamLength != l.StreamLength) throw new InvalidDataException("LongStream metadata incongruence.");
                        //l.Backing = match.Backing;
                        map.Hashtable[keys[i]] = match;
                    }
                    else throw new InvalidDataException("Expected LongStream UUID not received.");
                }
            }
        }

        /// <summary>
        /// When LongStream is requsted to be created for read.
        /// </summary>
        public Func<LongStream> OnCreateLongStream { get; set; }

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
            List<LongStream> l_l = new List<LongStream>();
            List<ArbitraryDataContainer> l_a = new List<ArbitraryDataContainer>();
            foreach (var val in map.Hashtable)
            {
                if (val.Value is LongStream)
                    l_l.Add(val.Value as LongStream);
                else if (val.Value is ArbitraryDataContainer)
                    l_a.Add(val.Value as ArbitraryDataContainer);
            }

            foreach (var adc in l_a)
            {
                var template = new ArbitraryDataContainer.Template(adc);
                template.WriteStream(to);
                adc.WriteStream(to);
            }

            foreach (var ls in l_l)
            {
                ls.WriteStream(to);
                OnLongStreamWriteComplete(ls);
            }
        }

        /// <summary>
        /// The max size allowed for reading messages. (default: int.MaxValue) (you probably shouldn't increase that)
        /// </summary>
        public static long MaxSize { get; set; } = int.MaxValue;
    }

    [Serializable]
    public abstract class ArbitraryDataContainer : ISerializable
    {
        public Guid UUID { get { return guid; } }

        public override bool Equals(object obj)
        {
            if (obj == null) return false;
            else if (obj is ArbitraryDataContainer)
                return (obj as ArbitraryDataContainer).guid.Equals(guid);
            else
                return guid.Equals(obj);
        }

        public override int GetHashCode()
        {
            return guid.GetHashCode();
        }

        public sealed class Template : ArbitraryDataContainer
        {
            public override void ReadStream(Stream from)
            {
                byte[] by = new byte[16];

                from.Read(by, 0, 16);
                guid = new Guid(by);

                var fmt = new System.Runtime.Serialization.Formatters.Binary.BinaryFormatter();
                Tag = fmt.Deserialize(from);
            }

            public override void WriteStream(Stream to)
            {
                byte[] by = guid.ToByteArray();

                to.Write(by, 0, 16);
                var fmt = new System.Runtime.Serialization.Formatters.Binary.BinaryFormatter();
                fmt.Serialize(to, Tag);
            }

            public Template(ArbitraryDataContainer from)
            {
                this.guid = from.guid;
                this.Tag = from.Tag;
            }

            public ArbitraryDataContainer InvokeFrom(Type type)
            {
                if (typeof(ArbitraryDataContainer).IsAssignableFrom(type))
                {
                    ArbitraryDataContainer container = (ArbitraryDataContainer)Activator.CreateInstance(type);
                    container.guid = guid;
                    container.Tag = Tag;
                    return container;
                }
                else throw new InvalidCastException("Type " + type.Name + " is not an ADC");
            }

            public Template()
            {

            }
        }

        private Guid guid;
        public object Tag { get; set; } = null;
        public ArbitraryDataContainer(object tag)
        {
            Tag = tag;
            guid = Guid.NewGuid();
        }

        public ArbitraryDataContainer() : this(null)
        {

        }

        public override string ToString()
        {
            return this.GetType().Name + "@" + guid.ToString() + (Tag == null ? "" : ":" + Tag.ToString());
        }

        public ArbitraryDataContainer(SerializationInfo info, StreamingContext context)
        {
            guid = new Guid(info.GetValue("id", typeof(byte[])) as byte[]);
            Tag = info.GetValue("tag", typeof(object));
        }

        public void GetObjectData(SerializationInfo info, StreamingContext context)
        {
            info.AddValue("tag", Tag);
            info.AddValue("id", guid.ToByteArray());
        }

        public abstract void WriteStream(Stream to);
        public abstract void ReadStream(Stream from);
    }

    public static class Extensions
    {
        /// <summary>
        /// Read data from stream, block until all the data wanted is read.
        /// </summary>
        public static void BlockingRead(this Stream s, byte[] to, int offset, int length)
        {
            int read = 0;
            int rd = 0;
            while ((read += (rd = s.Read(to, offset + read, length - read))) < length) if (rd == 0) throw new ClientReadException(new IOException(), "Blocking read hang.");
        }
    }

    /// <summary>
    /// Exception in Client I/O.
    /// </summary>
    public class ClientException : Exception
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

        public void Disuse()
        {
            useok = false;
        }
        private bool useok = true;

        public Preset Use()
        {
            Before();
            return this;
        }

        public void Dispose()
        {
            if(useok)
            After();
        }
    }

    /// <summary>
    /// Provides abstraction over reading and writing messages to/from a stream atomically.
    /// </summary>
    public class Client : IDisposable, IChannel<Message>
    {
        public Func<Message.LongStream, Stream> LongStreamReadCreate { get; set; } = (t) => new MemoryStream();
        public event Action<Message.LongStream> OnLongStreamReadComplete;
        public Func<Message.LongStream> OnLongStreamCreate { get; set; } = () => new Message.LongStream();

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
        public bool Alive { get { return Out.IsOpen && In.IsOpen; } }

        /// <summary>
        /// Event ran on closing (before resources released).
        /// </summary>
        public event Action OnClose;
        
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
        public Client(Stream stream, bool keepAlive = false) : this()
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

        public bool CloseOnException { get; set; } = false;

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

                        //var sd = OnSend.Use();
                        using (OnSend.Use())
                        {
                            try
                            {
                                msg.Write(backing);
                            }
                            catch (Exception) { OnSend.Disuse(); Close(); }
                        }
                        //sd.Dispose();
                        _ = SignalHandler.Signal(SignalMessageSent, msg);
                    }
                }

                if (!KeepAlive) try { backing.Dispose(); }
                    catch
                    {
                    }
            });
            up.Set();
            while (In.IsOpen)
            {

                Message message = null;
                try
                {
                    //var recv = OnReceive.Use();
                    using (OnReceive.Use())
                    {
                        try
                        {
                            message = new Message();
                            message.OnCreateLongStream = OnLongStreamCreate;
                            message.LongStreamReadCreate = LongStreamReadCreate;
                            message.OnLongStreamReadComplete += OnLongStreamReadComplete;
                            message.Read(backing);
                        }
                        catch (Exception )
                        {
                            OnReceive.Disuse();
                            Close();
                        }
                    }
                    //recv.Dispose();
                    if (In.IsOpen)
                    {
                        In.Send(message);
                        _ = SignalHandler.Signal(SignalMessageReceived, message);
                    }
                }
                catch (Exception)
                {
                    Close();
                    //OnException(new ClientReadException(e, "Error reading from stream"));

                }
            }
        }

        /// <summary>
        /// Close and release all resources used by this client.
        /// </summary>
        public void Dispose()
        {
            if (!Alive)
            {
                Out.Close();
                In.Close();
                return;
            }

            up.WaitOne();

            In.Close();
            Out.Close();
            
            up.Dispose();
            SignalHandler = new Dispatcher<Message>();

            if (OnClose != null)
                OnClose();
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
