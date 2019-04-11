using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Sync.Pipeline
{
    /// <summary>
    /// Provides an interface for sending/receiving objects between threads atomically.
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public interface IChannel<T>
    {
        /// <summary>
        /// Send an object to the channel.
        /// </summary>
        void Send(T obj);
        /// <summary>
        /// Receive an object from the channel. 
        /// </summary>
        T Receive();
        /// <summary>
        /// Is this channel open?
        /// </summary>
        bool IsOpen { get; }
        /// <summary>
        /// Close this channel.
        /// </summary>
        void Close();
    }
    /// <summary>
    /// Provides a method of sending/receiving objects between threads atomically.
    /// </summary>
    /// <typeparam name="T">The type of this channel.</typeparam>
    public class Channel<T> : IDisposable, IChannel<T>
    {
        /// <summary>
        /// Close and release all resources of this channel.
        /// </summary>
        public void Close()
        {
            Dispose();
        }
        private int backlog;
        private Queue<T> queue;
        private System.Threading.AutoResetEvent reset;
        private AutoResetEvent resetR;

        private readonly object qlock = new object();

        /// <summary>
        /// Initialise a new channel.
        /// </summary>
        /// <param name="i">The backlog for this channel, or 0 for infinite.</param>
        public Channel(int i = 0)
        {
            backlog = i;
            queue = new Queue<T>();
            reset = new AutoResetEvent(false);
            resetR = new AutoResetEvent(false);

        }

        /// <summary>
        /// Send an object to the channel. Blocks until there is room to send or the channel is closed.
        /// </summary>
        public void Send(T t)
        {
            start:
            if (closing) { return; }
            lock (qlock)
            {
                int c = queue.Count;

                if (backlog > 0 && c >= backlog)
                {
                    goto wait;
                }

                if (closing) { return; }
                queue.Enqueue(t);
                reset.Set();
                return;
            }
            wait:
            if (ShiftOnFull) Receive();
            resetR.WaitOne();
            goto start;

        }

        /// <summary>
        /// Send an object to this channel asynchronously.
        /// </summary>
        public async System.Threading.Tasks.Task SendAsync(T t)
        {
            var ts = new System.Threading.Tasks.Task(() => Send(t));
            ts.Start();
            await ts;
        }

        /// <summary>
        /// Attempt to send an object to the channel non-blocking. 
        /// </summary>
        /// <returns>True if successful, false if Send(t) would otherwise block.</returns>
        public bool TrySend(T t)
        {
            start:
            if (closing) return false;
            lock (qlock)
            {
                if (backlog > 0 && queue.Count >= backlog)
                {
                    if (ShiftOnFull) goto thing;
                    return false;
                }
                queue.Enqueue(t);
                reset.Set();
            }
            return true;
            thing:
            T tt = default(T);
            if (!TryReceive(ref tt)) return false;
            goto start;
        }

        private bool closing = false;

        /// <summary>
        /// Receive an object from the channel. Blocks until there is an object or the channel is closed.
        /// </summary>
        /// <returns>The object from the channel, or default(T) if the channel was closed.</returns>
        public T Receive()
        {
            while (true)
            {
                if (closing)
                    return default(T);
                lock (qlock)
                {
                    if (poll())
                    {
                        var ret = queue.Dequeue();
                        resetR.Set();
                        return ret;
                    }
                }
                reset.WaitOne();
            }
        }

        /// <summary>
        /// Receive an object from this channel asynchronously.
        /// </summary>
        public async System.Threading.Tasks.Task<T> ReceiveAsync()
        {
            var ts = new System.Threading.Tasks.Task<T>(() => Receive());
            ts.Start();
            return await ts;
        }

        /// <summary>
        /// Attempt to receive an object from the channel non-blocking.
        /// </summary>
        /// <param name="t">The output object if successful.</param>
        /// <returns>True if success, false if Receive() would otherwise block.</returns>
        public bool TryReceive(ref T t)
        {
            if (closing) return false;
            lock (qlock)
            {
                if (poll())
                {
                    t = queue.Dequeue();
                    resetR.Set();
                    return true;
                }
            }
            return false;
        }

        /// <summary>
        /// Is this channel open?
        /// </summary>
        public bool IsOpen { get { return !closing; } }

        public bool poll()
        {
            return queue.Count > 0;
        }

        /// <summary>
        /// Returns true if there are currently any objects in the channel (non-blocking).
        /// </summary>
        public bool Poll()
        {
            lock (qlock)
            {
                return queue.Count > 0;
            }
        }

        /// <summary>
        /// Returns the number of objects currently in this channel.
        /// </summary>
        public int Count
        {
            get
            {
                lock (qlock) return queue.Count;
            }
        }

        /// <summary>
        /// Returns true if there are currently any objects in the channel (non-blocking) and also peeks, the next object into t.
        /// </summary>
        public bool Peek(ref T t)
        {
            if (closing) return false;
            lock (qlock)
            {
                if (poll())
                {
                    t = queue.Peek();
                    return true;
                }
            }
            return false;
        }

        /// <summary>
        /// If true, a Send() or TrySend() call will call Receive() or TryReceive() respectively on the event there is no space in the channel before attemping to send its object.
        /// </summary>
        public bool ShiftOnFull { get; set; } = false;

        /// <summary>
        /// Close and release all resources of this channel.
        /// </summary>
        public void Dispose()
        {
            if (closing) return;
            closing = true;

            reset.Set();
            resetR.Set();

            resetR.Dispose();
            reset.Dispose();
        }

        /// <summary>
        /// Max number of objects allowed in the channel at a time (or 0 for infinite).
        /// </summary>
        public int Backlog { get { return backlog; } }

    }
    /// <summary>
    /// Provides a method of sending/receiving objects of multiple types to threads atomically.
    /// </summary>
    public class MultiplexedChannel : IDisposable, IChannel<object>
    {
        public void Close()
        {
            Dispose();
        }
        private Channel<dynamic> backing;
        private Channel<object> raw;
        private Thread sorter;

        private Dictionary<Type, Channel<dynamic>> outputs;


        public MultiplexedChannel(int i = 0)
        {
            backing = new Channel<dynamic>(i);
            raw = new Channel<object>(i);
            raw.ShiftOnFull = true;
            outputs = new Dictionary<Type, Channel<dynamic>>();

            sorter = Dispatcher.Spawn(recv);
        }

        Channel<dynamic> prepare(Type t)
        {
            Channel<dynamic> op;
            lock (outputs)
            {
                if (outputs.ContainsKey(t))
                {
                    //Send to this channel.
                    op = outputs[t];
                }
                else
                {
                    op = new Channel<dynamic>(backing.Backlog);
                    outputs.Add(t, op);

                }
            }
            return op;
        }

        private async System.Threading.Tasks.Task sort(dynamic d)
        {
            Channel<dynamic> op = prepare(d.GetType());

            var ts = new System.Threading.Tasks.Task(() => op.Send(d));
            ts.Start();
            await ts;
        }

        private void recv()
        {
            async void l()
            {
                System.Threading.Tasks.Task last, lastraw;
                last = lastraw = null;
                while (backing.IsOpen)
                {
                    dynamic d = backing.Receive();
                    if (!backing.IsOpen) break;

                    if (last != null)
                    {
                        await last;
                        last = null;
                    }
                    if (!backing.IsOpen) break;

                    if (lastraw != null)
                    {
                        await lastraw;
                        lastraw = null;
                    }
                    if (!backing.IsOpen) break;

                    lastraw = raw.SendAsync((object)d);
                    last = sort(d);
                }
            }
            l();
        }

        /// <summary>
        /// Receive an object of type `T'. Blocks until one is available or the channel is closed.
        /// </summary>
        /// <returns>The object or default(T) if the channel is closed.</returns>
        public T Receive<T>()
        {
            Channel<dynamic> op = prepare(typeof(T));
            return op.Receive();
        }

        /// <summary>
        /// Send an object of type `T'. Blocks until there is room available or the channel is closed.
        /// </summary>
        public void Send<T>(T t)
        {
            backing.Send(t);
        }

        /// <summary>
        /// Send an object of type `T' asyncronously.
        /// </summary>
        public async System.Threading.Tasks.Task SendAsync<T>(T t)
        {
            var ts = new System.Threading.Tasks.Task(() => Send<T>(t));
            ts.Start();
            await ts;
        }

        /// <summary>
        /// Receive an object of type `T' asyncronously.
        /// </summary>
        public async System.Threading.Tasks.Task<T> ReceiveAsync<T>()
        {
            var ts = new System.Threading.Tasks.Task<T>(() => Receive<T>());
            ts.Start();
            return await ts;
        }

        /// <summary>
        /// Send an arbitary object.
        /// </summary>
        public void Send(object o)
        {
            backing.Send(o);
        }

        /// <summary>
        /// Send an arbitary object asynchrounously.
        /// </summary>
        /// <param name="t"></param>
        /// <returns></returns>
        public async System.Threading.Tasks.Task SendAsync(object t)
        {
            var ts = new System.Threading.Tasks.Task(() => Send(t));
            ts.Start();
            await ts;
        }

        /// <summary>
        /// Receive an arbitary object.
        /// </summary>
        public object Receive()
        {
            return raw.Receive();
        }

        /// <summary>
        /// Receive an arbitary object asynchronously.
        /// </summary>
        public async System.Threading.Tasks.Task<object> ReceiveAsync()
        {
            var ts = new System.Threading.Tasks.Task<object>(() => Receive());
            ts.Start();
            return await ts;
        }

        public void Dispose()
        {
            backing.Dispose();
            raw.Dispose();

            lock (outputs)
            {
                foreach (var t in outputs)
                {
                    t.Value.Dispose();
                }
                outputs = null;
            }
            sorter.Join();
        }

        public bool IsOpen { get { return backing.IsOpen; } }
    }

    public class Dispatcher : Dispatcher<object>, IDispatcher
    { 
        /// <summary>
      /// Start a task for action.
      /// </summary>
        public static System.Threading.Tasks.Task Go(Action a)
        {
            var ts = new System.Threading.Tasks.Task(a);
            ts.Start();
            return ts;
        }

        /// <summary>
        /// Start a thread for action.
        /// </summary>
        public static Thread Spawn(Action a)
        {
            Thread t = new Thread(() => a());
            t.Start();
            return t;
        }
        public new delegate void GlobalCallback(string s);
        public new delegate void Callback();
        public Dispatcher() : base()
        {
            look = new Dictionary<Callback, Dispatcher<object>.Callback>();
            glook = new Dictionary<GlobalCallback, Dispatcher<object>.GlobalCallback>();
        }

        private Dictionary<Callback, Dispatcher<object>.Callback> look;
        private Dictionary<GlobalCallback, Dispatcher<object>.GlobalCallback> glook;

        private new object mutex = new object();
        private new object gmutex = new object();

        public Callback Hook(string s, Callback cb)
        {

            Dispatcher<object>.Callback cc = (object _) =>
            {
                cb();
            };
            lock (mutex)
            {
                look.Add(cb, cc);
                base.Hook(s, cc);
            }
            return cb;
        }

        public override void Clear(string s)
        {
            lock (mutex)
            {
                base.Clear(s);
            }
        }

        public override void ClearGlobal()
        {
            lock (mutex)
            {
                glook.Clear();
                base.ClearGlobal();
            }
        }

        public void Unhook(string s, Callback cb)
        {
            lock (mutex)
            {
                if (look.ContainsKey(cb))
                {
                    var cc = look[cb];
                    base.Unhook(s, cc);
                    look.Remove(cb);
                }
            }
        }

        public void HookGlobal(GlobalCallback cb)
        {
            lock (gmutex)
            {
                Dispatcher<object>.GlobalCallback cc = (string s, object _) =>
                {
                    cb(s);
                };
                lock (gmutex)
                {
                    glook.Add(cb, cc);
                    base.HookGlobal(cc);
                }
            }
        }

        public void UnhookGlobal(GlobalCallback cb)
        {
            lock (gmutex)
            {
                if (glook.ContainsKey(cb))
                {
                    var cc = glook[cb];
                    base.UnhookGlobal(cc);
                    glook.Remove(cb);
                }
            }
        }
    }

    public interface IDispatcher
    {
        Dispatcher.Callback Hook(string name, Dispatcher.Callback callback);
        void Unhook(string name, Dispatcher.Callback callback);
        Task Signal(string name);
    }

    /// <summary>
    /// Provides a method of asynchronously triggering callbacks.
    /// </summary>
    public class Dispatcher<T> : IDispatcher
    {
        public delegate void GlobalCallback(string s, T obj);
        public delegate void Callback(T o);
        protected Dictionary<string, List<Callback>> dict;
        protected List<GlobalCallback> global;
        protected readonly object mutex = new object();
        protected readonly object gmutex = new object();

        public Dispatcher()
        {
            dict = new Dictionary<string, List<Callback>>();
            global = new List<GlobalCallback>();
        }

        public void HookGlobal(GlobalCallback cb)
        {
            lock (gmutex)
            {
                global.Add(cb);
            }
        }

        public void UnhookGlobal(GlobalCallback cb)
        {
            lock (gmutex)
            {
                global.Remove(cb);
            }
        }

        private async Task sigGlobalSer(string s, T o)
        {
            Task t;
            lock (gmutex)
            {
                t = Dispatcher.Go(() =>
                {
                    foreach (var f in global) f(s, o);
                });
            }
            await t;
        }

        private async Task sigGlobalPar(string s, T o)
        {
            Task t = new Task(() =>
            {
                lock (gmutex)
                {
                    foreach (var f in global) f(s, o);

                }
            });
            t.Start();
            await t;
        }


        /// <summary>
        /// All current hooks.
        /// </summary>
        public Dictionary<string, List<Callback>> Hooks { get { lock (mutex) return new Dictionary<string, List<Callback>>(dict); } }

        /// <summary>
        /// Signal callbacks in serial (the order they were added.)
        /// </summary>
        public async Task SignalSerial(string s, T o = default(T))
        {
            Task vt = sigGlobalSer(s, o);
            Task t = new Task(() =>
            {
                lock (mutex)
                {
                    if (dict.ContainsKey(s))
                    {
                        foreach (var k in dict[s]) k(o);
                    }
                }
            });
            t.Start();
            await vt;
            await t;
        }

        /// <summary>
        /// Signal callbacks in serial, block until complete.
        /// </summary>
        public void SignalSerialSync(string s, T o = default(T))
        {
            SignalSerial(s, o).Wait();
        }

        /// <summary>
        /// Signal callbacks in parallel.
        /// </summary>
        public async Task Signal(string s, T o = default(T))
        {
            Task t;
            Task vt = sigGlobalPar(s, o);
            lock (mutex)
            {
                if (dict.ContainsKey(s))
                {
                    t = Task.WhenAll(dict[s].Select((func) =>
                    {
                        return Dispatcher.Go(() =>
                        {
                            func(o);
                        });
                    }));
                }
                else return;
            }
            await vt;
            await t;
        }

        /// <summary>
        /// Signal callbacks in parallel, block until complete.
        /// </summary>
        public void SignalSync(string s, T o = default(T))
        {
            Signal(s, o).Wait();
        }

        /// <summary>
        /// Add hook for designator.
        /// </summary>
        public Callback Hook(string s, Callback cb)
        {
            lock (mutex)
            {
                if (!dict.ContainsKey(s)) dict.Add(s, new List<Callback>());
                dict[s].Add(cb);
            }
            return cb;
        }

        /// <summary>
        /// Remove hook from designator.
        /// </summary>
        public void Unhook(string s, Callback cb)
        {
            lock (mutex)
            {
                if (dict.ContainsKey(s) && dict[s].Contains(cb)) dict[s].Remove(cb);
            }
        }

        /// <summary>
        /// Remove all hooks from designator.
        /// </summary>
        public virtual void Clear(string s)
        {
            lock (mutex)
            {
                if (dict.ContainsKey(s)) dict[s].Clear();
            }
        }

        /// <summary>
        /// Remove all global hooks.
        /// </summary>
        public virtual void ClearGlobal()
        {
            lock (gmutex)
            {
                global.Clear();
            }
        }


        Dispatcher.Callback IDispatcher.Hook(string name, Dispatcher.Callback callback)
        {
            this.Hook(name, (T _) =>
            {
                callback();
            });
            return callback;
        }

        void IDispatcher.Unhook(string name, Dispatcher.Callback callback)
        {
            throw new NotImplementedException();
        }

        async Task IDispatcher.Signal(string name)
        {
            await this.Signal(name);
        }
    }
}
