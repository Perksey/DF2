namespace Ultz.DF2
{
    public interface IValue
    {
        IValue this[string name] { get; }
        string Name { get; }
        string AbsolutePath { get; }
        uint? Handle { get; set; }
        ValueKind Kind { get; }
    }
}