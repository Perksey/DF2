namespace Ultz.DF2
{
    public interface IValue
    {
        string Name { get; set; }
        string AbsolutePath { get; }
        uint? Handle { get; set; }
        ValueKind Kind { get; }
    }
}