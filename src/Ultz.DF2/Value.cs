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

        internal Value(IGroupInternal parent, string name, ValueKind initialKind, object initialValue)
        {
            _parent = parent;
            _name = name;
            _kind = initialKind;
            _data = initialValue;
        }

        public object Data
        {
            get => _data;
            set => _data = value;
        }

        public string Name
        {
            get => _name;
            set => _name = value;
        }

        public string AbsolutePath => (_parent as Group)?.AbsolutePath + "/" + Name;

        public uint? Handle
        {
            get => _handle;
            set
            { _handle = value; }
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