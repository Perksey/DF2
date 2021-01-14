using System.Collections;

namespace Ultz.DF2
{
    public interface IGroup : IValue
    {
        IValueDictionary Values { get; }
        Group GetOrAddGroup(string name);
        Value AddOrUpdate(string name, byte val);
        Value AddOrUpdate(string name, sbyte val);
        Value AddOrUpdate(string name, short val);
        Value AddOrUpdate(string name, ushort val);
        Value AddOrUpdate(string name, int val);
        Value AddOrUpdate(string name, uint val);
        Value AddOrUpdate(string name, long val);
        Value AddOrUpdate(string name, ulong val);
        Value AddOrUpdate(string name, float val);
        Value AddOrUpdate(string name, double val);
        Value AddOrUpdate(string name, string val);
        Value AddOrUpdate(string name, byte[] val);
        Value AddOrUpdate(string name, sbyte[] val);
        Value AddOrUpdate(string name, short[] val);
        Value AddOrUpdate(string name, ushort[] val);
        Value AddOrUpdate(string name, int[] val);
        Value AddOrUpdate(string name, uint[] val);
        Value AddOrUpdate(string name, long[] val);
        Value AddOrUpdate(string name, ulong[] val);
        Value AddOrUpdate(string name, float[] val);
        Value AddOrUpdate(string name, double[] val);
        Value AddOrUpdate(string name, string[] val);
        Value AddOrUpdate(string name, IEnumerable val);
        bool Remove(string name);
    }
}