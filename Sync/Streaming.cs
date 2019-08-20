using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;
using System.Runtime.InteropServices;
using System.Runtime.CompilerServices;
using System.Threading;

namespace Sync.Pipeline
{
    internal interface IDisposeCheckable
    {
        bool IsDisposed { get; }
    }
    /// <summary>
    /// Provides an interface for caching and decaching.
    /// </summary>
    /// <typeparam name="T">The type to cache.</typeparam>
    public interface ICache<T>
    {
        void Cache(T value);
        int Count { get; }
        T Decache();
        bool TryDecache(out T value);
    }
    public unsafe class MemoryCache<T> : IDisposable, ICache<T>, IDisposeCheckable where T : struct
    {
        private IntPtr memory, ptr;
        private int __maxsize;
        public int MaxSize
        {
            get => this.ThrowIfDisposed(__maxsize);
            set => this.ThrowIfDisposed(() => __maxsize = value);
        }
        public int MemoryLeft => this.ThrowIfDisposed(MaxSize - FillPointer);
        public int FillPointer => this.ThrowIfDisposed((int)(ptr.ToInt64() - memory.ToInt64()));
        public MemoryCache(int maxSize)
        {
            ptr = memory = Marshal.AllocHGlobal(maxSize);

            MaxSize = maxSize;
        }

        public IntPtr FullMemory => this.ThrowIfDisposed(memory);
        public byte[] FullBytes
        {
            get
            {
                this.ThrowIfDisposed();
                byte[] ar = new byte[MaxSize];
                Marshal.Copy(memory, ar, 0, MaxSize);
                return ar;
            }
        }
        public T[] FullCache
        {
            get
            {
                this.ThrowIfDisposed();
                T[] outt = new T[Count];
                var handle = GCHandle.Alloc(outt, GCHandleType.Pinned);
                try
                {
                    unsafe
                    {
                        IntPtr sptr = memory;
                        var sz = ObjectSize();
                        for (int i = 0; i < Count; i++)
                        {
                            var tref = __makeref(outt[i]);

                            IntPtr ptr = *((IntPtr*)&tref);
                            Helpers.CopyMemory(ptr, sptr,(uint) sz);
                            sptr += sz;
                        }
                    }
                    return outt;
                }
                finally
                {
                    handle.Free();
                }
            }
        }
        public byte[] FullCacheBytes
        {
            get
            {
                this.ThrowIfDisposed();
                byte[] ar = new byte[FillPointer];
                Marshal.Copy(memory, ar, 0, FillPointer);
                return ar;
            }
        }

        #region Unsafes
        private unsafe static int ObjectSize()
        {
            Type type = typeof(T);

            TypeCode typeCode = Type.GetTypeCode(type);
            switch (typeCode)
            {
                case TypeCode.Boolean:
                    return sizeof(bool);
                case TypeCode.Char:
                    return sizeof(char);
                case TypeCode.SByte:
                    return sizeof(sbyte);
                case TypeCode.Byte:
                    return sizeof(byte);
                case TypeCode.Int16:
                    return sizeof(short);
                case TypeCode.UInt16:
                    return sizeof(ushort);
                case TypeCode.Int32:
                    return sizeof(int);
                case TypeCode.UInt32:
                    return sizeof(uint);
                case TypeCode.Int64:
                    return sizeof(long);
                case TypeCode.UInt64:
                    return sizeof(ulong);
                case TypeCode.Single:
                    return sizeof(float);
                case TypeCode.Double:
                    return sizeof(double);
                case TypeCode.Decimal:
                    return sizeof(decimal);
                case TypeCode.DateTime:
                    return sizeof(DateTime);
                default:
                    T[] tArray = new T[2];
                    GCHandle tArrayPinned = GCHandle.Alloc(tArray, GCHandleType.Pinned);
                    try
                    {
                        TypedReference tRef0 = __makeref(tArray[0]);
                        TypedReference tRef1 = __makeref(tArray[1]);
                        IntPtr ptrToT0 = *((IntPtr*)&tRef0);
                        IntPtr ptrToT1 = *((IntPtr*)&tRef1);

                        return (int)(((byte*)ptrToT1) - ((byte*)ptrToT0));
                    }
                    finally
                    {
                        tArrayPinned.Free();
                    }
            }
        }
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private unsafe static int WriteToPtr(IntPtr dest, T value)
        {
            int sizeOfT = ObjectSize();
            byte* bytePtr = (byte*)dest;

            var handle = GCHandle.Alloc(value, GCHandleType.Pinned);
            try
            {
                TypedReference valueref = __makeref(value);
                byte* valuePtr = (byte*)*((IntPtr*)&valueref);

                for (int i = 0; i < sizeOfT; ++i)
                {
                    bytePtr[i] = valuePtr[i];
                }
            }
            finally
            {
                handle.Free();
            }
            return sizeOfT;
        }
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private unsafe static T ReadFromPtr(IntPtr source, out int read)
        {
            int sizeOfT = ObjectSize();
            byte* bytePtr = (byte*)source;

            T result = default(T);
            var handle = GCHandle.Alloc(result, GCHandleType.Pinned);
            try
            {
                TypedReference resultRef = __makeref(result);
                byte* resultPtr = (byte*)*((IntPtr*)&resultRef);

                for (int i = 0; i < sizeOfT; ++i)
                {
                    resultPtr[i] = bytePtr[i];
                }

                read = sizeOfT;
                return result;
            }
            finally
            {
                handle.Free();
            }
            
        }
        #endregion

        protected T ForceDecache()
        {
            this.ThrowIfDisposed();
            var value = ReadFromPtr(memory, out int read);
            Helpers.MoveMemory(memory, memory + read, MaxSize - read);
            Count -= 1;
            return value;
        }

        public T Decache()
        {
            this.ThrowIfDisposed();
            if (TryDecache(out T value))
                return value;
            else throw new InsufficientMemoryException("No items in cache");
        }

        public bool TryDecache(out T value)
        {
            this.ThrowIfDisposed();
            if (Count > 0) value = ForceDecache();
            else
            {
                value = default(T);
                return false;
            }
            return true;
        }

        public int Count { get; private set; } = 0;

        public void Cache(T value)
        {
            this.ThrowIfDisposed();
            int sz = ObjectSize();
            if (MemoryLeft >= sz)
            {
                ptr += WriteToPtr(ptr, value);
                Count += 1;
            }
            else throw new InsufficientMemoryException("Trying to cache " + sz + " bytes when only " + MemoryLeft + " is left in the buffer");
        }

        #region IDisposable Support
        private bool disposedValue = false;
        bool IDisposeCheckable.IsDisposed => disposedValue;

        protected virtual void Dispose(bool disposing)
        {
            if (!disposedValue)
            {
                if (disposing)
                {

                }

                Marshal.FreeHGlobal(memory);

                disposedValue = true;
            }
        }
        
         ~MemoryCache() {
           Dispose(false);
         }
        
        public void Dispose()
        {
            Dispose(true);
             GC.SuppressFinalize(this);
        }
        #endregion
    }
    internal static class Helpers
    {
        [DllImport("kernel32.dll", EntryPoint = "CopyMemory", SetLastError = false)]
        public static extern void CopyMemory(IntPtr dest, IntPtr src, uint count);
        [DllImport("Kernel32.dll", EntryPoint = "RtlMoveMemory", SetLastError = false)]
        public static extern void MoveMemory(IntPtr dest, IntPtr src, int size);
        public static void ThrowIfDisposed(this IDisposeCheckable d)
        {
            if (d.IsDisposed)
                throw new ObjectDisposedException(nameof(d));
        }
        public static void ThrowIfDisposed(this IDisposeCheckable d, Action or)
        {
            d.ThrowIfDisposed();
            or();
        }
        public static T ThrowIfDisposed<T>(this IDisposeCheckable d, T ret)
        {
            d.ThrowIfDisposed();
            return ret;
        }

        class SemaphoreLock : IDisposable
        {
            public SemaphoreSlim Backing { get; private set; }
            public SemaphoreLock(SemaphoreSlim sem)
            {
                Backing = sem;
            }
            public void Dispose()
            {
                Backing.Release();
            }
        }
        public static IDisposable Aquire(this SemaphoreSlim sem)
        {
            sem.Wait();
            return new SemaphoreLock(sem);
        }
        public static async Task<IDisposable> AquireAsync(this SemaphoreSlim sem)
        {
            await sem.WaitAsync();
            return new SemaphoreLock(sem);
        }
        
    }
    public class AsyncManualResetEvent : IDisposable
    {
        private volatile TaskCompletionSource<bool> tcs = new TaskCompletionSource<bool>();

        public Task WaitAsync() => (disposed ? Task.CompletedTask : this.tcs.Task);
        public void Wait() => WaitAsync().Wait();

        public void Set()
        {
            if (disposed) return;
            // https://stackoverflow.com/questions/12693046/configuring-the-continuation-behaviour-of-a-taskcompletionsources-task
            Task.Run(() =>
            this.tcs.TrySetResult(true));
        }

        public void Reset()
        {
            if (disposed) return;
            while (true)
            {
                var tcs = this.tcs;
                if (!tcs.Task.IsCompleted ||
                    Interlocked.CompareExchange(ref this.tcs, new TaskCompletionSource<bool>(), tcs) == tcs)
                {
                    return;
                }
            }
        }
        private bool disposed = false;
        public void Dispose()
        {
            disposed = true;
	    Task.Run(() => this.tcs.TrySetResult(true));
        }
    }
    public class FixedCache<T> : ICache<T>, IDisposable where T : struct
    {
        public int Count { get; private set; }

        public static int ElementSize => Marshal.SizeOf(typeof(T));

        public int MaxSize { get; private set; }
        public int ItemsLeft => MaxSize - Count;

        private byte[] backing;
        AsyncManualResetEvent decache_reset = new AsyncManualResetEvent();
        AsyncManualResetEvent cache_reset = new AsyncManualResetEvent();

        SemaphoreSlim mutex = new SemaphoreSlim(1, 1);

        public FixedCache(int sz)
        {
            backing = new byte[ElementSize * sz];
            MaxSize = sz;
            Count = 0;
        }

        public void Dispose()
        {
            mutex.Wait();
            try
            {
                decache_reset.Dispose();
                cache_reset.Dispose();
            }
            finally
            {
                mutex.Release();
            }
            mutex.Dispose();
        }

        public void Cache(T value)
        {
            CacheAsync(value).Wait();
        }

        public Task WaitAvailable
        {
            get
            {
                using (mutex.Aquire())
                {
                    if (Count > 0)
                        return Task.CompletedTask;
                    else
                    {
                        TaskCompletionSource<bool> comp = new TaskCompletionSource<bool>();
                        void ev(T _)
                        {
                            OnCache -= ev;
                            comp.SetResult(true);
                        }
                        OnCache += ev;
                        return comp.Task;
                    }
                }
            }
        }

        public event Action<T> OnCache;
        public event Action<T> OnDecache;
        
        protected int LockedCount
        {
            get
            {
                using (mutex.Aquire()) return Count;
            }
        }

        protected async Task<int> GetLockedCount()
        {
            using (await mutex.AquireAsync())
                return Count;
        }

        public async Task CacheAsync(T value)
        {
            st:
            while (await GetLockedCount() >= MaxSize)
                await cache_reset.WaitAsync();
            cache_reset.Reset();
            using (await mutex.AquireAsync())
            {
                if (Count < MaxSize)
                {
                    UnsafeCache(value);
                }
                else goto st;
            }
        }
        protected void UnsafeCache(T value)
        {
            int offs = ElementSize * Count;
            var handle = GCHandle.Alloc(backing, GCHandleType.Pinned);
            try
            {
                var ptr = handle.AddrOfPinnedObject() + offs;
                Marshal.StructureToPtr(value, ptr, false);
                Count += 1;
                OnCache?.Invoke(value);
                decache_reset.Set();
            }
            finally
            {
                handle.Free();
            }
        }
        public async Task<T> DecacheAsync()
        {
            st:
            while (await GetLockedCount() < 1)
                await decache_reset.WaitAsync();
            decache_reset.Reset();
            using (await mutex.AquireAsync())
            {
                if (Count > 0)
                {
                    return UnsafeDecache();
                }
                else goto st;
            }
        }
        protected T UnsafeDecache()
        {
            var handle = GCHandle.Alloc(backing, GCHandleType.Pinned);
            try
            {
                var ptr = handle.AddrOfPinnedObject();
                var r = Marshal.PtrToStructure(ptr, typeof(T));
                Helpers.CopyMemory(ptr, ptr + ElementSize, (uint)(MaxSize - ElementSize));
                Count -= 1;
                OnDecache?.Invoke((T)r);
                cache_reset.Set();
                return (T)r;
            }
            finally
            {
                handle.Free();
            }

        }
        public T Decache()
        {
            var task = DecacheAsync();
            task.Wait();
            if (task.IsFaulted) throw task.Exception;
            else return task.Result;
        }

        public bool TryDecache(out T value)
        {
            using (mutex.Aquire())
            {
                if (Count < 1)
                {
                    value = default(T);
                    return false;
                }
                else
                {
                    value = UnsafeDecache();
                    return true;
                }
            }
        }
        public bool TryCache(T value)
        {
            using (mutex.Aquire())
            {
                if (Count >= MaxSize)
                    return false;
                else
                    UnsafeCache(value);
                return true;
            }
        }
    }

    /// <summary>
    /// A concurrent safe managed cache of items.
    /// </summary>
    public class ManagedCache<T> : ICache<T>
    {
        protected List<T> intern = new List<T>();
        protected readonly object mutex = new object();
        public ManagedCache()
        {

        }

        /// <summary>
        /// Number of items currently cached.
        /// </summary>
        public int Count { get { lock (mutex) return intern.Count; } }

        /// <summary>
        /// Add an item to the end of the cache.
        /// </summary>
        public void Cache(T value)
        {
            lock (mutex)
            {
                UnsafeCache(value);
            }
        }
        /// <summary>
        /// Cache an item with no mutex lock. Lock the mutex manually first.
        /// </summary>
        protected virtual void UnsafeCache(T value)
        {
            intern.Add(value);
        }
        /// <summary>
        /// Remove the first cached item. Will throw InsufficientMemoryException if there are none.
        /// </summary>
        public T Decache()
        {
            lock (mutex)
            {
                return UnsafeDecache();
            }
        }
        /// <summary>
        /// Decache with no mutex lock. Lock the mutex manually first.
        /// </summary>
        protected virtual T UnsafeDecache()
        {
            if (intern.Count > 0)
            {
                var v = intern[0];
                intern.RemoveAt(0);
                return v;
            }
            else throw new InsufficientMemoryException("Empty cache");
        }
        /// <summary>
        /// Attempt to decache an item.
        /// </summary>
        /// <returns>True if the operation succeeded, false if there were no items available.</returns>
        public bool TryDecache(out T value)
        {
            lock (mutex)
            {
                if (intern.Count > 0)
                {
                    value = UnsafeDecache();
                    return true;
                }
                else
                {
                    value = default(T);
                    return false;
                }
            }
        }
    }

    /// <summary>
    /// Provides a stream interface for reading and writing data between tasks.
    /// </summary>
    public class ChanneledStream : Stream, IChannel<byte>
    {
        protected Channel<byte[]> buffer;
        protected ICache<byte> cache;
        protected readonly object mutex = new object();

        /// <summary>
        /// Create a new ChanneledStream with limited buffer size.
        /// </summary>
        /// <param name="max">The max writes able to be stored in the buffer at once.</param>
        public ChanneledStream(int max) : this(max, 0) { }
        /// <summary>
        /// Create a new ChanneledStream with unlimited buffer size.
        /// </summary>
        public ChanneledStream() : this(0, 0) { }
        
        protected ChanneledStream(int max, int cacheMaxSize)
        {
            buffer = new Channel<byte[]>(max);
            if (cacheMaxSize == 0)
            {
                cache = new ManagedCache<byte>();
            }
            else
            {
                cache = new MemoryCache<byte>(cacheMaxSize);
            }
        }
        /// <summary>
        /// Create a new ChanneledStream with unlimited buffer size and custom cache.
        /// </summary>
        /// <param name="cache">The cache to buffer reads.</param>
        public ChanneledStream(ICache<byte> cache) : this(0, cache)
        { }
        /// <summary>
        /// Create a new ChanneledStream with limited buffer size and custom cache.
        /// </summary>
        /// <param name="cache">The cache to buffer reads.</param>
        public ChanneledStream(int max, ICache<byte> cache)
        {
            buffer = new Channel<byte[]>(max);
            if (cache == null)
                cache = new ManagedCache<byte>();
            this.cache = cache;
        }
        protected internal Channel<byte[]> Channel => buffer;
        protected override void Dispose(bool disposing)
        {
            base.Dispose(disposing);
            lock (mutex)
            {
                if (disposing)
                {
                    buffer.Dispose();
                    if (cache is IDisposable d) d.Dispose();
                }
            }
        }
        
        public override bool CanRead => true;

        public override bool CanSeek => false;

        public override bool CanWrite => true;
        /// <summary>
        /// This parameter is not valid and will always through a NotSupportedException. (If you want to track writes/reads, use OnSend/OnReceive events)
        /// </summary>
        public override long Length => throw new NotSupportedException();

        /// <summary>
        /// This parameter is not valid and will always through a NotSupportedException.
        /// </summary>
        public override long Position { get => throw new NotSupportedException(); set => throw new NotSupportedException(); }

        /// <summary>
        /// Is the Channel open?
        /// </summary>
        public bool IsOpen => buffer.IsOpen;

        /// <summary>
        /// Returns an awaitable Task that completes when there is data available.
        /// </summary>
        /// <param name="timeoutMS">Optional timeout. 0 for infinite.</param>
        /// <returns>True if completed, False if timed out.</returns>
        public async Task<bool> AwaitDataAvailable(int timeoutMS = 0)
        {
            if (cacheCount > 0) return true;
            if (buffer.Count > 0) return true;
            TaskCompletionSource<bool> comp = new TaskCompletionSource<bool>();
            void recv(long _)
            {
                OnSend -= recv;
                comp.SetResult(true);
            }
            OnSend += recv;
            if (timeoutMS == 0)
                return await comp.Task;
            else
                return (await Task.WhenAny(comp.Task, Task.Delay(timeoutMS)) == comp.Task);
        }

        /// <summary>
        /// Fires when data is read.
        /// </summary>
        public event Action<long> OnReceive;
        /// <summary>
        /// Fires when data is written.
        /// </summary>
        public event Action<long> OnSend;

        /// <summary>
        /// Flush all buffered data into cache.
        /// </summary>
        public override void Flush()
        {
            byte[] wew = null;
            while (buffer.TryReceive(ref wew))
            {
                lock (mutex)
                {
                    foreach (var b in wew)
                        cache.Cache(b);
                }
            }
        }
        void readCache()
        {
            var buf = buffer.Receive();
            lock (mutex)
            {
                foreach (var by in buf)
                    cache.Cache(by);
            }
        }
        bool tryReadCache()
        {
            byte[] buf = null;
            if (!buffer.TryReceive(ref buf)) return false;

            lock (mutex)
            {
                foreach (var by in buf)
                    cache.Cache(by);
            }
            return true;
        }

        async Task readCacheAsync()
        {
            var buf = await buffer.ReceiveAsync();
            lock (mutex)
            {
                foreach (var by in buf)
                    cache.Cache(by);
            }
        }
        private int cacheCount
        {
            get
            {
                lock (mutex) return cache.Count;
            }
        }

        /// <summary>
        /// Get the current cache size.
        /// </summary>
        public int CacheSize => cacheCount;
        
        /// <summary>
        /// Force blocking until all requested data is available for read.
        /// </summary>
        public bool ForceFullReads { get; set; } = false;
        /// <summary>
        /// Read count bytes into buffer+offset.
        /// </summary>
        public override int Read(byte[] buffer, int offset, int count)
        {
            return Read(buffer, offset, count, ForceFullReads);
        }
        /// <summary>
        /// Read count bytes into buffer+offset, block until all data has been read or the stream is closed.
        /// </summary>
        public int ReadForced(byte[] buffer, int offset, int count) => Read(buffer, offset, count, true);
        protected virtual int Read(byte[] buffer, int offset, int count, bool force)
        {
            if (force)
            {
                st:
                while (cacheCount < count)
                {
                    readCache();
                }
                lock (mutex)
                {
                    if (cache.Count < count) goto st;
                    for (int i = 0; i < count; i++)
                    {
                        buffer[offset + i] = cache.Decache();
                    }
                    OnReceive?.Invoke(count);
                }
                return count;
            }
            else
            {
                while (cacheCount < count)
                {
                    if (!tryReadCache())
                        break;
                }
                lock (mutex)
                {
                    if (cache.Count < count)
                    {
                        count = cache.Count;
                    }

                    for (int i = 0; i < count; i++)
                    {
                        buffer[offset + i] = cache.Decache();
                    }
                    OnReceive?.Invoke(count);
                    return count;
                }
            }
        }

        /// <summary>
        /// Returns an awaitable task that reads count bytes into buffer+offset, waits until all data has been read or the stream is closed.
        /// </summary>
        public Task<int> ReadAsyncForced(byte[] buffer, int offset, int count) => ReadAsync(buffer, offset, count, true);
        protected virtual async Task<int> ReadAsync(byte[] buffer, int offset, int count, bool force)
        {
            if (force)
            {
                st:
                while (cacheCount < count)
                {
                    await readCacheAsync();
                }
                lock (mutex)
                {
                    if (cache.Count < count) goto st;
                    for (int i = 0; i < count; i++)
                    {
                        buffer[offset + i] = cache.Decache();
                    }
                    OnReceive?.Invoke(count);
                }
                return count;
            }
            else
            {
                while (cacheCount < count)
                {
                    if (!tryReadCache())
                        break;
                }
                lock (mutex)
                {
                    if (cache.Count < count)
                    {
                        count = cache.Count;
                    }

                    for (int i = 0; i < count; i++)
                    {
                        buffer[offset + i] = cache.Decache();
                    }
                    OnReceive?.Invoke(count);
                    return count;
                }
            }
        }
        /// <summary>
        /// Returns an awaitable task that reads count bytes into buffer+offset
        /// </summary>
        public new Task<int> ReadAsync(byte[] buffer, int offset, int count)
        {
            return ReadAsync(buffer, offset, count, ForceFullReads);
        }

        /// <summary>
        /// This parameter is not valid and will always through a NotSupportedException.
        /// </summary>
        public override long Seek(long offset, SeekOrigin origin)
        {
            throw new NotSupportedException();
        }

        /// <summary>
        /// This parameter is not valid and will always through a NotSupportedException.
        /// </summary>
        public override void SetLength(long value)
        {
            throw new NotSupportedException();
        }

        public override void Write(byte[] buffer, int offset, int count)
        {
            byte[] ar = new byte[count - offset];
            Array.Copy(buffer, offset, ar, 0, count - offset);
            this.buffer.Send(ar);
            OnSend?.Invoke(ar.Length);
        }

        /// <summary>
        /// Returns an awaitable task that writes count bytes from buffer+offset
        /// </summary>
        public new Task WriteAsync(byte[] buffer, int offset, int count)
        {
            byte[] ar = new byte[count - offset];
            Array.Copy(buffer, offset, ar, 0, count - offset);
            var r =  this.buffer.SendAsync(ar);
            OnSend?.Invoke(ar.Length);
            return r;
        }

        void IChannel<byte>.Send(byte obj)
        {
            Write(new byte[] { obj }, 0, 1);
        }

        byte IChannel<byte>.Receive()
        {
            var r = new byte[1];
            Read(r, 0, 1);
            return r[0];
        }
    }
}
