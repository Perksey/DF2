using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.ComponentModel;
using System.Data;
using System.IO;

namespace Ultz.DF2
{
    public class Group : IValue, IValueInternal, INotifyPropertyChanged, IGroup, IGroupInternal
    {
        private object _parent;
        private string _name;
        private uint? _handle;

        internal Group(object parent, string name, bool send = false)
        {
            _parent = parent;
            _name = name;
            Values = new ValueDictionary(x => x.Name);
            if (send)
            {
                var s = ((IGroupInternal)this).GetStream();
                s.Sender.SendGroup(name);
                s.OutboundCurrentGroup = this;
            }
        }
        public IValueDictionary Values { get; }

        public string Name => _name;

        public string AbsolutePath => (_parent as Group)?.AbsolutePath + "/";

        public uint? Handle
        {
            get => _handle;
            set => _handle = value;
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

            return new(this, name, true);
        }
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
    }
}