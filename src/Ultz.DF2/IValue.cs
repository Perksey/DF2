namespace Ultz.DF2
{
    public interface IValue
    {
        string Name { get; }
        string AbsolutePath { get; }
        uint? Handle { get; set; }
        ValueKind Kind { get; }
    }
}