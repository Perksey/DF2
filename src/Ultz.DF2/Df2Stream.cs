using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Ultz.DF2
{
    /// <summary>
    /// Represents a bidirectional stream capable of transmitting and receiving DF2 data.
    /// </summary>
    public class Df2Stream : IDisposable, IGroup, IGroupInternal
    {
        private readonly bool _leaveOpen;
        private Group? _outboundCurrentGroup;

        public Df2Stream(Stream @base, StreamMode mode, bool leaveOpen = false)
        {
            _leaveOpen = leaveOpen;
            if (@base.CanRead && (mode & StreamMode.Read) != 0)
            {
                BaseReader = new BinaryReader(@base, Encoding.UTF8, leaveOpen);
                if (!Preface.IsValid(BaseReader))
                {
                    throw new InvalidOperationException();
                }
            }

            if (@base.CanWrite && (mode & StreamMode.Write) != 0)
            {
                BaseWriter = new BinaryWriter(@base, Encoding.UTF8, leaveOpen);
                Preface.Write(BaseWriter);
            }

            Receiver = new CommandReceiver(this);
            Sender = new CommandSender(this);
        }
        
        public BinaryReader? BaseReader { get; }
        public BinaryWriter? BaseWriter { get; }
        public bool HasReceivedEnd { get; internal set; }
        public IValueDictionary Values { get; } = new ValueDictionary(x => x.Name);
        public Group? InboundCurrentGroup { get; internal set; }

        public Group? OutboundCurrentGroup
        {
            get => _outboundCurrentGroup;
            internal set
            {
                if (value?.Handle is not null)
                {
                    Sender.SendGroupByHandle(value.Handle.Value);
                }
                else
                {
                    Sender.SendGroup(value?.AbsolutePath ?? string.Empty);
                }
                
                _outboundCurrentGroup = value;
            }
        }

        internal CommandReceiver Receiver { get; }
        internal CommandSender Sender { get; }
        public IReadOnlyDictionary<uint, IValue> Handles { get; } = new Dictionary<uint, IValue>();

        public bool ProcessCommand()
        {
            if (BaseReader is null)
            {
                throw new NotSupportedException("Base stream was read-only at time of creation.");
            }
            
            if (HasReceivedEnd)
            {
                throw new InvalidOperationException("Previously received end command, will not read any further.");
            }

            return Receiver.ProcessCommand();
        }

        public static string GetFullPath(string path, string relativeTo)
        {
            // TODO improve this, yuck
            var baseSplit = relativeTo.Split("/", StringSplitOptions.RemoveEmptyEntries).ToList();
            var pathSplit = path.Split("/", StringSplitOptions.RemoveEmptyEntries);

            if (baseSplit.Contains(".."))
            {
                throw new ArgumentException("Must be an absolute path", nameof(relativeTo));
            }

            foreach (var item in pathSplit)
            {
                switch (item)
                {
                    case "..":
                    {
                        baseSplit.RemoveAt(baseSplit.Count - 1);
                        break;
                    }
                    case ".":
                    {
                        // do nothing
                        break;
                    }
                    default:
                    {
                        baseSplit.Add(item);
                        break;
                    }
                }
            }

            return "/" + string.Join("/", baseSplit);
        }

        public static string GetRelativePath(string path, string groupRelativeTo)
        {
            // TODO improve this, yuck
            var baseSplit = groupRelativeTo.Split('/', StringSplitOptions.RemoveEmptyEntries).Where(x => x != ".").ToArray();
            var pathSplit = path.Split('/', StringSplitOptions.RemoveEmptyEntries).Where(x => x != ".").ToArray();
            if (baseSplit.Contains(".."))
            {
                throw new ArgumentException("Must be an absolute path", nameof(groupRelativeTo));
            }
            
            if (pathSplit.Contains(".."))
            {
                throw new ArgumentException("Must be an absolute path", nameof(pathSplit));
            }
            
            var lastMatchingIndex = 0;
            for (; lastMatchingIndex < baseSplit.Length && lastMatchingIndex < pathSplit.Length; lastMatchingIndex++)
            {
                if (baseSplit[lastMatchingIndex] != pathSplit[lastMatchingIndex])
                {
                    break;
                }
            }

            baseSplit = baseSplit[lastMatchingIndex..];
            pathSplit = pathSplit[lastMatchingIndex..];
            return string.Join("/", baseSplit.Select(_ => "..").Concat(pathSplit).Where(x => !string.IsNullOrEmpty(x)));
        }

        public void ProcessUntilEnd()
        {
            while (ProcessCommand())
            {
                // do nothing, this will loop until we get an End command
            }
        }

        public void RewriteOptimized()
        {
            var s = BaseWriter?.BaseStream ?? throw new InvalidOperationException("Stream not writable");
            if (!s.CanSeek)
            {
                throw new InvalidOperationException("Stream not seekable");
            }
            
            s.Seek(0, SeekOrigin.Begin);
            s.SetLength(0);
            Write(this);

            static void Write(IGroup group)
            {
                if (group is not Df2Stream)
                {
                    ((IGroupInternal)group).GetStream().Sender.SendGroup(group.Name);
                }

                foreach (var (name, value) in group.Values)
                {
                    if (value is IGroup childGroup)
                    {
                        Write(childGroup);
                    }
                    else
                    {
                        ((IGroupInternal)group).GetStream().Sender.SendValue(name, (Value)value, out _);
                    }
                }
                
                if (group is not Df2Stream)
                {
                    ((IGroupInternal)group).GetStream().Sender.SendGroup("..");
                }
            }
        }

        public void Flush()
        {
            if (BaseWriter is null)
            {
                throw new InvalidOperationException();
            }
            
            Sender.SendEnd();
            BaseWriter.Flush();
        }

        public void Close()
        {
            BaseReader?.Close();
            BaseWriter?.Close();
        }

        public Df2Stream GetStream() => this;

        public Group GetOrAddGroup(string name) => new (this, name, true);
        public Value AddOrUpdate(string name, byte val) => new (this, name, ValueKind.Byte, val, true);
        public Value AddOrUpdate(string name, sbyte val) => new (this, name, ValueKind.SByte, val, true);
        public Value AddOrUpdate(string name, short val) => new (this, name, ValueKind.Short, val, true);
        public Value AddOrUpdate(string name, ushort val) => new (this, name, ValueKind.UShort, val, true);
        public Value AddOrUpdate(string name, int val) => new (this, name, ValueKind.Int, val, true);
        public Value AddOrUpdate(string name, uint val) => new (this, name, ValueKind.UInt, val, true);
        public Value AddOrUpdate(string name, long val) => new (this, name, ValueKind.Long, val, true);
        public Value AddOrUpdate(string name, ulong val) => new (this, name, ValueKind.ULong, val, true);
        public Value AddOrUpdate(string name, float val) => new (this, name, ValueKind.Float, val, true);
        public Value AddOrUpdate(string name, double val) => new (this, name, ValueKind.Double, val, true);
        public Value AddOrUpdate(string name, string val) => new (this, name, ValueKind.String, val, true);
        public Value AddOrUpdate(string name, byte[] val) => new (this, name, ValueKind.Array, val, true);
        public Value AddOrUpdate(string name, sbyte[] val) => new (this, name, ValueKind.Array, val, true);
        public Value AddOrUpdate(string name, short[] val) => new (this, name, ValueKind.Array, val, true);
        public Value AddOrUpdate(string name, ushort[] val) => new (this, name, ValueKind.Array, val, true);
        public Value AddOrUpdate(string name, int[] val) => new (this, name, ValueKind.Array, val, true);
        public Value AddOrUpdate(string name, uint[] val) => new (this, name, ValueKind.Array, val, true);
        public Value AddOrUpdate(string name, long[] val) => new (this, name, ValueKind.Array, val, true);
        public Value AddOrUpdate(string name, ulong[] val) => new (this, name, ValueKind.Array, val, true);
        public Value AddOrUpdate(string name, float[] val) => new (this, name, ValueKind.Array, val, true);
        public Value AddOrUpdate(string name, double[] val) => new (this, name, ValueKind.Array, val, true);
        public Value AddOrUpdate(string name, string[] val) => new (this, name, ValueKind.Array, val, true);
        public Value AddOrUpdate(string name, IEnumerable val) => new (this, name, ValueKind.List, val, true);

        public void Dispose()
        {
            BaseReader?.Dispose();
            BaseWriter?.Dispose();
        }

        string IValue.Name => null;
        string IValue.AbsolutePath => null;

        uint? IValue.Handle
        {
            get => null;
            set { }
        }

        ValueKind IValue.Kind => ValueKind.Group;
    }
}