using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
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
            Values = new ValueDictionary(x => x.Name, this);
        }

        public event Action<string> CommandSend;
        public event Action<string> CommandReceive;
        
        public BinaryReader? BaseReader { get; }
        public BinaryWriter? BaseWriter { get; }
        public bool HasReceivedEnd { get; internal set; }
        public IValueDictionary Values { get; }
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
                    Sender.SendGroup(GetRelativePath(value?.AbsolutePath ?? string.Empty, _outboundCurrentGroup?.AbsolutePath ?? "/"));
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
            var baseSplit = relativeTo.Split(new []{"/"}, StringSplitOptions.RemoveEmptyEntries).ToList();
            var pathSplit = path.Split(new[]{"/"}, StringSplitOptions.RemoveEmptyEntries);

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

            return string.Join("/", baseSplit);
        }

        public static string GetRelativePath(string path, string groupRelativeTo)
        {
            // TODO improve this, yuck
            var baseSplit = groupRelativeTo.Split(new[]{'/'}, StringSplitOptions.RemoveEmptyEntries).Where(x => x != ".").ToArray();
            var pathSplit = path.Split(new[]{'/'}, StringSplitOptions.RemoveEmptyEntries).Where(x => x != ".").ToArray();
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

            baseSplit = new ArraySegment<string>(baseSplit, lastMatchingIndex, baseSplit.Length - lastMatchingIndex)
                .ToArray();
            pathSplit = new ArraySegment<string>(pathSplit, lastMatchingIndex, pathSplit.Length - lastMatchingIndex)
                .ToArray();
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
            s.Write(Preface.RawPreface, 0, Preface.RawPreface.Length);
            CopyTo(this);
        }

        public void CopyTo(Df2Stream dest)
        {
            Write(this, dest);
            static void Write(IGroup srcGroup, IGroup destGroup)
            {
                if (srcGroup.Handle is not null)
                {
                    destGroup.Handle = srcGroup.Handle.Value;
                }

                foreach (var kvp in srcGroup.Values)
                {
                    if (kvp.Value is IGroup childGroup)
                    {
                        Write(childGroup, destGroup.GetOrAddGroup(kvp.Key));
                    }
                    else
                    {
                        var val = destGroup.AddOrUpdate(kvp.Key, ((Value)kvp.Value).Data);
                        if (kvp.Value.Handle is not null)
                        {
                            val.Handle = kvp.Value.Handle.Value;
                        }
                    }
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

        public Group GetOrAddGroup(string name)
        {
            if (Values.TryGetValue(name, out var val))
            {
                if (val is not Group group)
                {
                    throw new DataException($"A value with name \"{name}\" already exists and is not a group.");
                }

                return group;
            }
            
            var ret = new Group(this, name, true);
            ((ValueDictionary) Values).Add(ret);
            return ret;
        }

        public Value AddOrUpdate(string name, byte val)
        {
            if (Values.TryGetValue(name, out var existingVal))
            {
                if (existingVal is not Value actualEValue)
                {
                    throw new DataException($"A value with name \"{name}\" already exists and is not a single value.");
                }

                actualEValue.Data = val;
                return actualEValue;
            }

            var ret = new Value(this, name, ValueKind.Byte, val, true);
            ((ValueDictionary) Values).Add(ret);
            return ret;
        }

        public Value AddOrUpdate(string name, sbyte val)
        {
            if (Values.TryGetValue(name, out var existingVal))
            {
                if (existingVal is not Value actualEValue)
                {
                    throw new DataException($"A value with name \"{name}\" already exists and is not a single value.");
                }

                actualEValue.Data = val;
                return actualEValue;
            }

            var ret = new Value(this, name, ValueKind.SByte, val, true);
            ((ValueDictionary) Values).Add(ret);
            return ret;
        }

        public Value AddOrUpdate(string name, short val)
        {
            if (Values.TryGetValue(name, out var existingVal))
            {
                if (existingVal is not Value actualEValue)
                {
                    throw new DataException($"A value with name \"{name}\" already exists and is not a single value.");
                }

                actualEValue.Data = val;
                return actualEValue;
            }

            var ret = new Value(this, name, ValueKind.Short, val, true);
            ((ValueDictionary) Values).Add(ret);
            return ret;
        }

        public Value AddOrUpdate(string name, ushort val)
        {
            if (Values.TryGetValue(name, out var existingVal))
            {
                if (existingVal is not Value actualEValue)
                {
                    throw new DataException($"A value with name \"{name}\" already exists and is not a single value.");
                }

                actualEValue.Data = val;
                return actualEValue;
            }

            var ret = new Value(this, name, ValueKind.UShort, val, true);
            ((ValueDictionary) Values).Add(ret);
            return ret;
        }

        public Value AddOrUpdate(string name, int val)
        {
            if (Values.TryGetValue(name, out var existingVal))
            {
                if (existingVal is not Value actualEValue)
                {
                    throw new DataException($"A value with name \"{name}\" already exists and is not a single value.");
                }

                actualEValue.Data = val;
                return actualEValue;
            }

            var ret = new Value(this, name, ValueKind.Int, val, true);
            ((ValueDictionary) Values).Add(ret);
            return ret;
        }

        public Value AddOrUpdate(string name, uint val)
        {
            if (Values.TryGetValue(name, out var existingVal))
            {
                if (existingVal is not Value actualEValue)
                {
                    throw new DataException($"A value with name \"{name}\" already exists and is not a single value.");
                }

                actualEValue.Data = val;
                return actualEValue;
            }

            var ret = new Value(this, name, ValueKind.UInt, val, true);
            ((ValueDictionary) Values).Add(ret);
            return ret;
        }

        public Value AddOrUpdate(string name, long val)
        {
            if (Values.TryGetValue(name, out var existingVal))
            {
                if (existingVal is not Value actualEValue)
                {
                    throw new DataException($"A value with name \"{name}\" already exists and is not a single value.");
                }

                actualEValue.Data = val;
                return actualEValue;
            }

            var ret = new Value(this, name, ValueKind.Long, val, true);
            ((ValueDictionary) Values).Add(ret);
            return ret;
        }

        public Value AddOrUpdate(string name, ulong val)
        {
            if (Values.TryGetValue(name, out var existingVal))
            {
                if (existingVal is not Value actualEValue)
                {
                    throw new DataException($"A value with name \"{name}\" already exists and is not a single value.");
                }

                actualEValue.Data = val;
                return actualEValue;
            }

            var ret = new Value(this, name, ValueKind.ULong, val, true);
            ((ValueDictionary) Values).Add(ret);
            return ret;
        }

        public Value AddOrUpdate(string name, float val)
        {
            if (Values.TryGetValue(name, out var existingVal))
            {
                if (existingVal is not Value actualEValue)
                {
                    throw new DataException($"A value with name \"{name}\" already exists and is not a single value.");
                }

                actualEValue.Data = val;
                return actualEValue;
            }

            var ret = new Value(this, name, ValueKind.Float, val, true);
            ((ValueDictionary) Values).Add(ret);
            return ret;
        }

        public Value AddOrUpdate(string name, double val)
        {
            if (Values.TryGetValue(name, out var existingVal))
            {
                if (existingVal is not Value actualEValue)
                {
                    throw new DataException($"A value with name \"{name}\" already exists and is not a single value.");
                }

                actualEValue.Data = val;
                return actualEValue;
            }

            var ret = new Value(this, name, ValueKind.Double, val, true);
            ((ValueDictionary) Values).Add(ret);
            return ret;
        }

        public Value AddOrUpdate(string name, string val)
        {
            if (Values.TryGetValue(name, out var existingVal))
            {
                if (existingVal is not Value actualEValue)
                {
                    throw new DataException($"A value with name \"{name}\" already exists and is not a single value.");
                }

                actualEValue.Data = val;
                return actualEValue;
            }

            var ret = new Value(this, name, ValueKind.String, val, true);
            ((ValueDictionary) Values).Add(ret);
            return ret;
        }

        public Value AddOrUpdate(string name, byte[] val)
        {
            if (Values.TryGetValue(name, out var existingVal))
            {
                if (existingVal is not Value actualEValue)
                {
                    throw new DataException($"A value with name \"{name}\" already exists and is not a single value.");
                }

                actualEValue.Data = val;
                return actualEValue;
            }

            var ret = new Value(this, name, ValueKind.Array, val, true);
            ((ValueDictionary) Values).Add(ret);
            return ret;
        }

        public Value AddOrUpdate(string name, sbyte[] val)
        {
            if (Values.TryGetValue(name, out var existingVal))
            {
                if (existingVal is not Value actualEValue)
                {
                    throw new DataException($"A value with name \"{name}\" already exists and is not a single value.");
                }

                actualEValue.Data = val;
                return actualEValue;
            }

            var ret = new Value(this, name, ValueKind.Array, val, true);
            ((ValueDictionary) Values).Add(ret);
            return ret;
        }

        public Value AddOrUpdate(string name, short[] val)
        {
            if (Values.TryGetValue(name, out var existingVal))
            {
                if (existingVal is not Value actualEValue)
                {
                    throw new DataException($"A value with name \"{name}\" already exists and is not a single value.");
                }

                actualEValue.Data = val;
                return actualEValue;
            }

            var ret = new Value(this, name, ValueKind.Array, val, true);
            ((ValueDictionary) Values).Add(ret);
            return ret;
        }

        public Value AddOrUpdate(string name, ushort[] val)
        {
            if (Values.TryGetValue(name, out var existingVal))
            {
                if (existingVal is not Value actualEValue)
                {
                    throw new DataException($"A value with name \"{name}\" already exists and is not a single value.");
                }

                actualEValue.Data = val;
                return actualEValue;
            }

            var ret = new Value(this, name, ValueKind.Array, val, true);
            ((ValueDictionary) Values).Add(ret);
            return ret;
        }

        public Value AddOrUpdate(string name, int[] val)
        {
            if (Values.TryGetValue(name, out var existingVal))
            {
                if (existingVal is not Value actualEValue)
                {
                    throw new DataException($"A value with name \"{name}\" already exists and is not a single value.");
                }

                actualEValue.Data = val;
                return actualEValue;
            }

            var ret = new Value(this, name, ValueKind.Array, val, true);
            ((ValueDictionary) Values).Add(ret);
            return ret;
        }

        public Value AddOrUpdate(string name, uint[] val)
        {
            if (Values.TryGetValue(name, out var existingVal))
            {
                if (existingVal is not Value actualEValue)
                {
                    throw new DataException($"A value with name \"{name}\" already exists and is not a single value.");
                }

                actualEValue.Data = val;
                return actualEValue;
            }

            var ret = new Value(this, name, ValueKind.Array, val, true);
            ((ValueDictionary) Values).Add(ret);
            return ret;
        }

        public Value AddOrUpdate(string name, long[] val)
        {
            if (Values.TryGetValue(name, out var existingVal))
            {
                if (existingVal is not Value actualEValue)
                {
                    throw new DataException($"A value with name \"{name}\" already exists and is not a single value.");
                }

                actualEValue.Data = val;
                return actualEValue;
            }

            var ret = new Value(this, name, ValueKind.Array, val, true);
            ((ValueDictionary) Values).Add(ret);
            return ret;
        }

        public Value AddOrUpdate(string name, ulong[] val)
        {
            if (Values.TryGetValue(name, out var existingVal))
            {
                if (existingVal is not Value actualEValue)
                {
                    throw new DataException($"A value with name \"{name}\" already exists and is not a single value.");
                }

                actualEValue.Data = val;
                return actualEValue;
            }

            var ret = new Value(this, name, ValueKind.Array, val, true);
            ((ValueDictionary) Values).Add(ret);
            return ret;
        }

        public Value AddOrUpdate(string name, float[] val)
        {
            if (Values.TryGetValue(name, out var existingVal))
            {
                if (existingVal is not Value actualEValue)
                {
                    throw new DataException($"A value with name \"{name}\" already exists and is not a single value.");
                }

                actualEValue.Data = val;
                return actualEValue;
            }

            var ret = new Value(this, name, ValueKind.Array, val, true);
            ((ValueDictionary) Values).Add(ret);
            return ret;
        }

        public Value AddOrUpdate(string name, double[] val)
        {
            if (Values.TryGetValue(name, out var existingVal))
            {
                if (existingVal is not Value actualEValue)
                {
                    throw new DataException($"A value with name \"{name}\" already exists and is not a single value.");
                }

                actualEValue.Data = val;
                return actualEValue;
            }

            var ret = new Value(this, name, ValueKind.Array, val, true);
            ((ValueDictionary) Values).Add(ret);
            return ret;
        }

        public Value AddOrUpdate(string name, string[] val)
        {
            if (Values.TryGetValue(name, out var existingVal))
            {
                if (existingVal is not Value actualEValue)
                {
                    throw new DataException($"A value with name \"{name}\" already exists and is not a single value.");
                }

                actualEValue.Data = val;
                return actualEValue;
            }

            var ret = new Value(this, name, ValueKind.Array, val, true);
            ((ValueDictionary) Values).Add(ret);
            return ret;
        }

        public Value AddOrUpdate(string name, IEnumerable val)
        {
            if (Values.TryGetValue(name, out var existingVal))
            {
                if (existingVal is not Value actualEValue)
                {
                    throw new DataException($"A value with name \"{name}\" already exists and is not a single value.");
                }

                actualEValue.Data = val;
                return actualEValue;
            }

            var ret = new Value(this, name, ValueKind.List, val, true);
            ((ValueDictionary) Values).Add(ret);
            return ret;
        }

        public Value AddOrUpdate(string name, object val)
        {
            if (Values.TryGetValue(name, out var existingVal))
            {
                if (existingVal is not Value actualEValue)
                {
                    throw new DataException($"A value with name \"{name}\" already exists and is not a single value.");
                }

                actualEValue.Data = val;
                return actualEValue;
            }

            var ret = new Value(this, name, null, val, true); // if send is true, the null we pass is replaced anyway
            ((ValueDictionary) Values).Add(ret);
            return ret;
        }

        public bool Remove(string name)
        {
            if (!((IDictionary<string, IValue>) Values).Remove(name))
            {
                return false;
            }

            Sender.SendRemove(GetRelativePath(name, OutboundCurrentGroup?.AbsolutePath ?? "/"));
            return true;
        }

        public void Dispose()
        {
            BaseReader?.Dispose();
            BaseWriter?.Dispose();
        }

        public IValue this[string name] => Values[name];

        string IValue.Name => null;
        string IValue.AbsolutePath => null;

        uint? IValue.Handle
        {
            get => null;
            set { }
        }

        ValueKind IValue.Kind => ValueKind.Group;

        internal void CoreSendEvent(string str) => CommandSend?.Invoke(str);
        internal void CoreReceiveEvent(string str) => CommandReceive?.Invoke(str);
    }
}