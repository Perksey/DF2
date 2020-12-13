using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.ComponentModel;
using System.IO;

namespace Ultz.DF2
{
    public class Group : IValue, IValueInternal, INotifyPropertyChanged, IGroup, IGroupInternal
    {
        private object _parent;
        private string _name;
        private uint? _handle;

        internal Group(object parent, string name)
        {
            _parent = parent;
            _name = name;
            Values = new ValueDictionary(x => x.Name);
        }
        public IValueDictionary Values { get; }

        public string Name
        {
            get => _name;
            set => _name = value;
        }

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
    }
}