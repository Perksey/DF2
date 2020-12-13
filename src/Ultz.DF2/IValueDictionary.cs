using System.Collections.Generic;
using System.Collections.Specialized;

namespace Ultz.DF2
{
    public interface IValueDictionary : IReadOnlyDictionary<string, IValue>, INotifyCollectionChanged
    {
    }
}