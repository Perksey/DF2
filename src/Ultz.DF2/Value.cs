using System;
using System.ComponentModel;

namespace Ultz.DF2
{
    public class Value : IValue, IValueInternal, INotifyPropertyChanged
    {
        private IGroupInternal _parent;
        private string _name;
        private uint? _handle;
        private object _data;
        private ValueKind _kind;

        internal Value(IGroupInternal parent, string name, ValueKind? initialKind, object initialValue,
            bool send = false)
        {
            _parent = parent;
            _name = name;
            _kind = initialKind ?? ValueKind.Null;
            _data = initialValue;
            if (send)
            {
                Data = initialValue;
            }
        }

        public object Data
        {
            get => _data;
            set
            {
                var s = _parent.GetStream();
                if (s.OutboundCurrentGroup != _parent as Group)
                {
                    s.OutboundCurrentGroup = _parent switch
                    {
                        _ when _parent is Group group => group,
                        _ when _parent is Df2Stream => null,
                        _ => throw new InvalidOperationException(
                            "This group is neither a child of the stream or a group.")
                    };
                }

                if (_handle is null)
                {
                    s.Sender.SendValue(
                        Df2Stream.GetRelativePath(AbsolutePath, s.OutboundCurrentGroup?.AbsolutePath ?? "/"), value,
                        out _kind);
                }
                else
                {
                    s.Sender.SendEditValueByHandle(_handle.Value, _kind, value, out _kind,
                        Df2Stream.GetRelativePath(AbsolutePath, s.OutboundCurrentGroup?.AbsolutePath ?? "/"));
                }

                UpdateValue(_kind, value);
            }
        }

        public IValue this[string name]
            => throw new InvalidOperationException($"Element at {AbsolutePath} is not a group.");

        public string Name => _name;

        public string AbsolutePath => (_parent as Group)?.AbsolutePath + "/" + Name;

        public uint? Handle
        {
            get => _handle;
            set
            {
                var s = _parent.GetStream();
                if (_handle is not null && value is null)
                {
                    s.Sender.SendHandle(string.Empty, _handle.Value);
                }
                else if (value is not null)
                {
                    s.Sender.SendHandle(
                        Df2Stream.GetRelativePath(AbsolutePath, s.OutboundCurrentGroup?.AbsolutePath ?? "/"),
                        value.Value);
                }

                ((IValueInternal) this).UpdateHandle(value);
            }
        }

        public ValueKind Kind => _kind;

        internal void UpdateValue(ValueKind kind, object value)
        {
            _kind = kind;
            _data = value;
            PropertyChanged?.Invoke(this, new PropertyChangedEventArgs(nameof(Kind)));
            PropertyChanged?.Invoke(this, new PropertyChangedEventArgs(nameof(Data)));
        }

        void IValueInternal.UpdateHandle(uint? handle)
        {
            _handle = handle;
            PropertyChanged?.Invoke(this, new PropertyChangedEventArgs(nameof(handle)));
        }

        public event PropertyChangedEventHandler PropertyChanged;
    }
}