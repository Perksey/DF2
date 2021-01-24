using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.ComponentModel;
using System.Data;
using System.IO;

namespace Ultz.DF2
{
    public class Group : IValue, IValueInternal, INotifyPropertyChanged, IGroup, IGroupInternal,
        INotifyCollectionChanged
    {
        private object _parent;
        private string _name;
        private uint? _handle;

        internal Group(object parent, string name, bool send = false)
        {
            _parent = parent;
            _name = name;
            Values = new ValueDictionary(x => x.Name, this);
            if (send)
            {
                var s = ((IGroupInternal) this).GetStream();
                s.OutboundCurrentGroup = this;
            }
        }

        public IValueDictionary Values { get; }

        public IValue this[string name] => Values[name];

        public string Name => _name;

        public string AbsolutePath => $"{(_parent as Group)?.AbsolutePath.TrimEnd('/')}/{_name}/";

        public uint? Handle
        {
            get => _handle;
            set
            {
                var s = ((IGroupInternal) this).GetStream();
                if (_handle is not null && value is null)
                {
                    s.Sender.SendHandle(string.Empty, _handle.Value);
                }
                else if (value is not null)
                {
                    var path = Df2Stream.GetRelativePath(AbsolutePath, s.OutboundCurrentGroup?.AbsolutePath ?? "/");
                    if (path == string.Empty)
                    {
                        path = ".";
                    }

                    s.Sender.SendHandle(path, value.Value);
                }

                ((IValueInternal) this).UpdateHandle(value);
            }
        }

        public ValueKind Kind => ValueKind.Group;

        void IValueInternal.UpdateHandle(uint? handle)
        {
            _handle = handle;
            PropertyChanged?.Invoke(this, new PropertyChangedEventArgs(nameof(handle)));
        }

        public event PropertyChangedEventHandler PropertyChanged;

        Df2Stream IGroupInternal.GetStream()
        {
            var parent = _parent;
            while (true)
            {
                switch (parent)
                {
                    case Group group:
                        parent = @group._parent;
                        break;
                    case Df2Stream stream:
                        return stream;
                    default:
                        throw new InvalidOperationException("Parent is not a group or stream");
                }
            }
        }

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

            ((IGroupInternal) this).GetStream().Sender.SendRemove(name);
            return true;
        }

        public event NotifyCollectionChangedEventHandler? CollectionChanged
        {
            add => Values.CollectionChanged += value;
            remove => Values.CollectionChanged -= value;
        }
    }
}