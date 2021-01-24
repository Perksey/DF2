using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using Xunit;
using Xunit.Abstractions;

namespace Ultz.DF2.Tests
{
    public class NestedGroupWriteAndRecoverTests
    {
        public readonly ITestOutputHelper Out;

        public NestedGroupWriteAndRecoverTests(ITestOutputHelper output)
        {
            Out = output;
        }

        public static IEnumerable<object[]> GetBigStrings()
        {
            yield return new object[] {new string('A', 1024 * 1024)};
            const string text = "English 日本語 русский язык 官話 🌄 ";
            var bytes = Encoding.UTF8.GetBytes(text);
            var numRepeats = (int) (1024 * 1024 / (double) bytes.Length);
            yield return new object[] {string.Join(string.Empty, Enumerable.Range(0, numRepeats).Select(_ => text))};
        }

        [Theory, InlineData(byte.MinValue), InlineData(byte.MaxValue), InlineData(42)]
        public void CanWriteAndRecoverByte(byte testData)
        {
            using var s = new MemoryStream();
            using var inStream = new Df2Stream(s, StreamMode.Write, true);
            inStream.CommandSend += cmd => Out.WriteLine($"WRITE> {cmd}");
            var group = inStream.GetOrAddGroup("TestGroup");
            group = group.GetOrAddGroup("TestNestedGroup");
            group.AddOrUpdate("TestData", testData);
            inStream.Flush();
            inStream.Close();
            Out.WriteLine($"Data: {Encoding.UTF8.GetString(s.ToArray())} ({BitConverter.ToString(s.ToArray())})");
            s.Seek(0, SeekOrigin.Begin);
            using var outStream = new Df2Stream(s, StreamMode.Read);
            outStream.CommandReceive += msg => Out.WriteLine("READ> " + msg);
            outStream.ProcessUntilEnd();
            Assert.Equal(testData, outStream.Values["TestGroup"]["TestNestedGroup"]["TestData"].AsByte());
        }

        [Theory, InlineData(sbyte.MinValue), InlineData(sbyte.MaxValue), InlineData(42)]
        public void CanWriteAndRecoverSByte(sbyte testData)
        {
            using var s = new MemoryStream();
            using var inStream = new Df2Stream(s, StreamMode.Write, true);
            inStream.CommandSend += cmd => Out.WriteLine($"WRITE> {cmd}");
            var group = inStream.GetOrAddGroup("TestGroup");
            group = group.GetOrAddGroup("TestNestedGroup");
            group.AddOrUpdate("TestData", testData);
            inStream.Flush();
            inStream.Close();
            Out.WriteLine($"Data: {Encoding.UTF8.GetString(s.ToArray())} ({BitConverter.ToString(s.ToArray())})");
            s.Seek(0, SeekOrigin.Begin);
            using var outStream = new Df2Stream(s, StreamMode.Read);
            outStream.CommandReceive += msg => Out.WriteLine("READ> " + msg);
            outStream.ProcessUntilEnd();
            Assert.Equal(testData, outStream.Values["TestGroup"]["TestNestedGroup"]["TestData"].AsSByte());
        }

        [Theory, InlineData(short.MinValue), InlineData(short.MaxValue), InlineData(42), InlineData(0xFE),
         InlineData(0xFF)]
        public void CanWriteAndRecoverShort(short testData)
        {
            using var s = new MemoryStream();
            using var inStream = new Df2Stream(s, StreamMode.Write, true);
            inStream.CommandSend += cmd => Out.WriteLine($"WRITE> {cmd}");
            var group = inStream.GetOrAddGroup("TestGroup");
            group = group.GetOrAddGroup("TestNestedGroup");
            group.AddOrUpdate("TestData", testData);
            inStream.Flush();
            inStream.Close();
            Out.WriteLine($"Data: {Encoding.UTF8.GetString(s.ToArray())} ({BitConverter.ToString(s.ToArray())})");
            s.Seek(0, SeekOrigin.Begin);
            using var outStream = new Df2Stream(s, StreamMode.Read);
            outStream.CommandReceive += msg => Out.WriteLine("READ> " + msg);
            outStream.ProcessUntilEnd();
            Assert.Equal(testData, outStream.Values["TestGroup"]["TestNestedGroup"]["TestData"].AsShort());
        }

        [Theory, InlineData(ushort.MinValue), InlineData(ushort.MaxValue), InlineData(42), InlineData(0xFE),
         InlineData(0xFF)]
        public void CanWriteAndRecoverUShort(ushort testData)
        {
            using var s = new MemoryStream();
            using var inStream = new Df2Stream(s, StreamMode.Write, true);
            inStream.CommandSend += cmd => Out.WriteLine($"WRITE> {cmd}");
            var group = inStream.GetOrAddGroup("TestGroup");
            group = group.GetOrAddGroup("TestNestedGroup");
            group.AddOrUpdate("TestData", testData);
            inStream.Flush();
            inStream.Close();
            Out.WriteLine($"Data: {Encoding.UTF8.GetString(s.ToArray())} ({BitConverter.ToString(s.ToArray())})");
            s.Seek(0, SeekOrigin.Begin);
            using var outStream = new Df2Stream(s, StreamMode.Read);
            outStream.CommandReceive += msg => Out.WriteLine("READ> " + msg);
            outStream.ProcessUntilEnd();
            Assert.Equal(testData, outStream.Values["TestGroup"]["TestNestedGroup"]["TestData"].AsUShort());
        }

        [Theory, InlineData(int.MinValue), InlineData(int.MaxValue), InlineData(42), InlineData(0xFE), InlineData(0xFF),
         InlineData(0xFFFE), InlineData(0xFFFF)]
        public void CanWriteAndRecoverInt(int testData)
        {
            using var s = new MemoryStream();
            using var inStream = new Df2Stream(s, StreamMode.Write, true);
            inStream.CommandSend += cmd => Out.WriteLine($"WRITE> {cmd}");
            var group = inStream.GetOrAddGroup("TestGroup");
            group = group.GetOrAddGroup("TestNestedGroup");
            group.AddOrUpdate("TestData", testData);
            inStream.Flush();
            inStream.Close();
            Out.WriteLine($"Data: {Encoding.UTF8.GetString(s.ToArray())} ({BitConverter.ToString(s.ToArray())})");
            s.Seek(0, SeekOrigin.Begin);
            using var outStream = new Df2Stream(s, StreamMode.Read);
            outStream.CommandReceive += msg => Out.WriteLine("READ> " + msg);
            outStream.ProcessUntilEnd();
            Assert.Equal(testData, outStream.Values["TestGroup"]["TestNestedGroup"]["TestData"].AsInt());
        }

        [Theory, InlineData(uint.MinValue), InlineData(uint.MaxValue), InlineData(42), InlineData(0xFE),
         InlineData(0xFF), InlineData(0xFFFE), InlineData(0xFFFF)]
        public void CanWriteAndRecoverUInt(uint testData)
        {
            using var s = new MemoryStream();
            using var inStream = new Df2Stream(s, StreamMode.Write, true);
            inStream.CommandSend += cmd => Out.WriteLine($"WRITE> {cmd}");
            var group = inStream.GetOrAddGroup("TestGroup");
            group = group.GetOrAddGroup("TestNestedGroup");
            group.AddOrUpdate("TestData", testData);
            inStream.Flush();
            inStream.Close();
            Out.WriteLine($"Data: {Encoding.UTF8.GetString(s.ToArray())} ({BitConverter.ToString(s.ToArray())})");
            s.Seek(0, SeekOrigin.Begin);
            using var outStream = new Df2Stream(s, StreamMode.Read);
            outStream.CommandReceive += msg => Out.WriteLine("READ> " + msg);
            outStream.ProcessUntilEnd();
            Assert.Equal(testData, outStream.Values["TestGroup"]["TestNestedGroup"]["TestData"].AsUInt());
        }

        [Theory, InlineData(long.MinValue), InlineData(long.MaxValue), InlineData(42), InlineData(0xFE),
         InlineData(0xFF), InlineData(0xFFFE), InlineData(0xFFFF), InlineData(0xFFFFFE), InlineData(0xFFFFFF),
         InlineData(0xFFFFFFFE), InlineData(0xFFFFFFFF)]
        public void CanWriteAndRecoverLong(long testData)
        {
            using var s = new MemoryStream();
            using var inStream = new Df2Stream(s, StreamMode.Write, true);
            inStream.CommandSend += cmd => Out.WriteLine($"WRITE> {cmd}");
            var group = inStream.GetOrAddGroup("TestGroup");
            group = group.GetOrAddGroup("TestNestedGroup");
            group.AddOrUpdate("TestData", testData);
            inStream.Flush();
            inStream.Close();
            Out.WriteLine($"Data: {Encoding.UTF8.GetString(s.ToArray())} ({BitConverter.ToString(s.ToArray())})");
            s.Seek(0, SeekOrigin.Begin);
            using var outStream = new Df2Stream(s, StreamMode.Read);
            outStream.CommandReceive += msg => Out.WriteLine("READ> " + msg);
            outStream.ProcessUntilEnd();
            Assert.Equal(testData, outStream.Values["TestGroup"]["TestNestedGroup"]["TestData"].AsLong());
        }

        [Theory, InlineData(ulong.MinValue), InlineData(ulong.MaxValue), InlineData(42), InlineData(0xFE),
         InlineData(0xFF), InlineData(0xFFFE), InlineData(0xFFFF), InlineData(0xFFFFFE), InlineData(0xFFFFFF),
         InlineData(0xFFFFFFFE), InlineData(0xFFFFFFFF)]
        public void CanWriteAndRecoverULong(ulong testData)
        {
            using var s = new MemoryStream();
            using var inStream = new Df2Stream(s, StreamMode.Write, true);
            inStream.CommandSend += cmd => Out.WriteLine($"WRITE> {cmd}");
            var group = inStream.GetOrAddGroup("TestGroup");
            group = group.GetOrAddGroup("TestNestedGroup");
            group.AddOrUpdate("TestData", testData);
            inStream.Flush();
            inStream.Close();
            Out.WriteLine($"Data: {Encoding.UTF8.GetString(s.ToArray())} ({BitConverter.ToString(s.ToArray())})");
            s.Seek(0, SeekOrigin.Begin);
            using var outStream = new Df2Stream(s, StreamMode.Read);
            outStream.CommandReceive += msg => Out.WriteLine("READ> " + msg);
            outStream.ProcessUntilEnd();
            Assert.Equal(testData, outStream.Values["TestGroup"]["TestNestedGroup"]["TestData"].AsULong());
        }

        [Theory, InlineData(float.MinValue), InlineData(float.MaxValue), InlineData(float.Epsilon),
         InlineData(float.PositiveInfinity), InlineData(float.NegativeInfinity), InlineData(float.NaN), InlineData(42)]
        public void CanWriteAndRecoverFloat(float testData)
        {
            using var s = new MemoryStream();
            using var inStream = new Df2Stream(s, StreamMode.Write, true);
            inStream.CommandSend += cmd => Out.WriteLine($"WRITE> {cmd}");
            var group = inStream.GetOrAddGroup("TestGroup");
            group = group.GetOrAddGroup("TestNestedGroup");
            group.AddOrUpdate("TestData", testData);
            inStream.Flush();
            inStream.Close();
            Out.WriteLine($"Data: {Encoding.UTF8.GetString(s.ToArray())} ({BitConverter.ToString(s.ToArray())})");
            s.Seek(0, SeekOrigin.Begin);
            using var outStream = new Df2Stream(s, StreamMode.Read);
            outStream.CommandReceive += msg => Out.WriteLine("READ> " + msg);
            outStream.ProcessUntilEnd();
            Assert.Equal(testData, outStream.Values["TestGroup"]["TestNestedGroup"]["TestData"].AsFloat());
        }

        [Theory, InlineData(double.MinValue), InlineData(double.MaxValue), InlineData(double.Epsilon),
         InlineData(double.PositiveInfinity), InlineData(double.NegativeInfinity), InlineData(double.NaN),
         InlineData(42)]
        public void CanWriteAndRecoverDouble(double testData)
        {
            using var s = new MemoryStream();
            using var inStream = new Df2Stream(s, StreamMode.Write, true);
            inStream.CommandSend += cmd => Out.WriteLine($"WRITE> {cmd}");
            var group = inStream.GetOrAddGroup("TestGroup");
            group = group.GetOrAddGroup("TestNestedGroup");
            group.AddOrUpdate("TestData", testData);
            inStream.Flush();
            inStream.Close();
            Out.WriteLine($"Data: {Encoding.UTF8.GetString(s.ToArray())} ({BitConverter.ToString(s.ToArray())})");
            s.Seek(0, SeekOrigin.Begin);
            using var outStream = new Df2Stream(s, StreamMode.Read);
            outStream.CommandReceive += msg => Out.WriteLine("READ> " + msg);
            outStream.ProcessUntilEnd();
            Assert.Equal(testData, outStream.Values["TestGroup"]["TestNestedGroup"]["TestData"].AsDouble());
        }

        [Theory, InlineData("Forty Two"), InlineData("English 日本語 русский язык 官話"), InlineData("🌄"),
         MemberData(nameof(GetBigStrings))]
        public void CanWriteAndRecoverString(string testData)
        {
            using var s = new MemoryStream();
            using var inStream = new Df2Stream(s, StreamMode.Write, true);
            inStream.CommandSend += msg => Out.WriteLine("SEND> " + msg);
            var group = inStream.GetOrAddGroup("TestGroup");
            group = group.GetOrAddGroup("TestNestedGroup");
            group.AddOrUpdate("TestData", testData);
            inStream.Flush();
            inStream.Close();
            Out.WriteLine($"Data: {Encoding.UTF8.GetString(s.ToArray())} ({BitConverter.ToString(s.ToArray())})");
            s.Seek(0, SeekOrigin.Begin);
            using var outStream = new Df2Stream(s, StreamMode.Read);
            outStream.CommandReceive += msg => Out.WriteLine("READ> " + msg);
            outStream.ProcessUntilEnd();
            Assert.Equal(testData, outStream.Values["TestGroup"]["TestNestedGroup"]["TestData"].AsString());
        }

        [Theory, InlineData(new byte[] {byte.MinValue, byte.MaxValue, 42})]
        public void CanWriteAndRecoverByteArray(byte[] testData)
        {
            using var s = new MemoryStream();
            using var inStream = new Df2Stream(s, StreamMode.Write, true);
            var group = inStream.GetOrAddGroup("TestGroup");
            inStream.CommandSend += msg => Out.WriteLine("WRITE> " + msg);
            group = group.GetOrAddGroup("TestNestedGroup");
            group.AddOrUpdate("TestData", testData);
            inStream.Flush();
            inStream.Close();
            Out.WriteLine($"Data: {Encoding.UTF8.GetString(s.ToArray())} ({BitConverter.ToString(s.ToArray())})");
            s.Seek(0, SeekOrigin.Begin);
            using var outStream = new Df2Stream(s, StreamMode.Read);
            outStream.CommandReceive += msg => Out.WriteLine("READ> " + msg);
            outStream.ProcessUntilEnd();
            Assert.Equal(testData, outStream.Values["TestGroup"]["TestNestedGroup"]["TestData"].AsArray<byte>());
        }

        [Theory, InlineData(new sbyte[] {sbyte.MinValue, sbyte.MaxValue, 42})]
        public void CanWriteAndRecoverSByteArray(sbyte[] testData)
        {
            using var s = new MemoryStream();
            using var inStream = new Df2Stream(s, StreamMode.Write, true);
            var group = inStream.GetOrAddGroup("TestGroup");
            inStream.CommandSend += msg => Out.WriteLine("WRITE> " + msg);
            group = group.GetOrAddGroup("TestNestedGroup");
            group.AddOrUpdate("TestData", testData);
            inStream.Flush();
            inStream.Close();
            Out.WriteLine($"Data: {Encoding.UTF8.GetString(s.ToArray())} ({BitConverter.ToString(s.ToArray())})");
            s.Seek(0, SeekOrigin.Begin);
            using var outStream = new Df2Stream(s, StreamMode.Read);
            outStream.CommandReceive += msg => Out.WriteLine("READ> " + msg);
            outStream.ProcessUntilEnd();
            Assert.Equal(testData, outStream.Values["TestGroup"]["TestNestedGroup"]["TestData"].AsArray<sbyte>());
        }

        [Theory, InlineData(new short[] {short.MinValue, short.MaxValue, 42, 0xFE, 0xFF})]
        public void CanWriteAndRecoverShortArray(short[] testData)
        {
            using var s = new MemoryStream();
            using var inStream = new Df2Stream(s, StreamMode.Write, true);
            var group = inStream.GetOrAddGroup("TestGroup");
            inStream.CommandSend += msg => Out.WriteLine("WRITE> " + msg);
            group = group.GetOrAddGroup("TestNestedGroup");
            group.AddOrUpdate("TestData", testData);
            inStream.Flush();
            inStream.Close();
            Out.WriteLine($"Data: {Encoding.UTF8.GetString(s.ToArray())} ({BitConverter.ToString(s.ToArray())})");
            s.Seek(0, SeekOrigin.Begin);
            using var outStream = new Df2Stream(s, StreamMode.Read);
            outStream.CommandReceive += msg => Out.WriteLine("READ> " + msg);
            outStream.ProcessUntilEnd();
            Assert.Equal(testData, outStream.Values["TestGroup"]["TestNestedGroup"]["TestData"].AsArray<short>());
        }

        [Theory, InlineData(new ushort[] {ushort.MinValue, ushort.MaxValue, 42, 0xFE, 0xFF})]
        public void CanWriteAndRecoverUShortArray(ushort[] testData)
        {
            using var s = new MemoryStream();
            using var inStream = new Df2Stream(s, StreamMode.Write, true);
            var group = inStream.GetOrAddGroup("TestGroup");
            inStream.CommandSend += msg => Out.WriteLine("WRITE> " + msg);
            group = group.GetOrAddGroup("TestNestedGroup");
            group.AddOrUpdate("TestData", testData);
            inStream.Flush();
            inStream.Close();
            Out.WriteLine($"Data: {Encoding.UTF8.GetString(s.ToArray())} ({BitConverter.ToString(s.ToArray())})");
            s.Seek(0, SeekOrigin.Begin);
            using var outStream = new Df2Stream(s, StreamMode.Read);
            outStream.CommandReceive += msg => Out.WriteLine("READ> " + msg);
            outStream.ProcessUntilEnd();
            Assert.Equal(testData, outStream.Values["TestGroup"]["TestNestedGroup"]["TestData"].AsArray<ushort>());
        }

        [Theory, InlineData(new[] {int.MinValue, int.MaxValue, 42, 0xFE, 0xFF, 0xFFFE, 0xFFFF})]
        public void CanWriteAndRecoverIntArray(int[] testData)
        {
            using var s = new MemoryStream();
            using var inStream = new Df2Stream(s, StreamMode.Write, true);
            var group = inStream.GetOrAddGroup("TestGroup");
            inStream.CommandSend += msg => Out.WriteLine("WRITE> " + msg);
            group = group.GetOrAddGroup("TestNestedGroup");
            group.AddOrUpdate("TestData", testData);
            inStream.Flush();
            inStream.Close();
            Out.WriteLine($"Data: {Encoding.UTF8.GetString(s.ToArray())} ({BitConverter.ToString(s.ToArray())})");
            s.Seek(0, SeekOrigin.Begin);
            using var outStream = new Df2Stream(s, StreamMode.Read);
            outStream.CommandReceive += msg => Out.WriteLine("READ> " + msg);
            outStream.ProcessUntilEnd();
            Assert.Equal(testData, outStream.Values["TestGroup"]["TestNestedGroup"]["TestData"].AsArray<int>());
        }

        [Theory, InlineData(new uint[] {uint.MinValue, uint.MaxValue, 42, 0xFE, 0xFF, 0xFFFE, 0xFFFF})]
        public void CanWriteAndRecoverUIntArray(uint[] testData)
        {
            using var s = new MemoryStream();
            using var inStream = new Df2Stream(s, StreamMode.Write, true);
            var group = inStream.GetOrAddGroup("TestGroup");
            inStream.CommandSend += msg => Out.WriteLine("WRITE> " + msg);
            group = group.GetOrAddGroup("TestNestedGroup");
            group.AddOrUpdate("TestData", testData);
            inStream.Flush();
            inStream.Close();
            Out.WriteLine($"Data: {Encoding.UTF8.GetString(s.ToArray())} ({BitConverter.ToString(s.ToArray())})");
            s.Seek(0, SeekOrigin.Begin);
            using var outStream = new Df2Stream(s, StreamMode.Read);
            outStream.CommandReceive += msg => Out.WriteLine("READ> " + msg);
            outStream.ProcessUntilEnd();
            Assert.Equal(testData, outStream.Values["TestGroup"]["TestNestedGroup"]["TestData"].AsArray<uint>());
        }

        [Theory,
         InlineData(new[]
         {
             long.MinValue, long.MaxValue, 42, 0xFE, 0xFF, 0xFFFE, 0xFFFF, 0xFFFFFE, 0xFFFFFF, 0xFFFFFFFE, 0xFFFFFFFF
         })]
        public void CanWriteAndRecoverLongArray(long[] testData)
        {
            using var s = new MemoryStream();
            using var inStream = new Df2Stream(s, StreamMode.Write, true);
            var group = inStream.GetOrAddGroup("TestGroup");
            inStream.CommandSend += msg => Out.WriteLine("WRITE> " + msg);
            group = group.GetOrAddGroup("TestNestedGroup");
            group.AddOrUpdate("TestData", testData);
            inStream.Flush();
            inStream.Close();
            Out.WriteLine($"Data: {Encoding.UTF8.GetString(s.ToArray())} ({BitConverter.ToString(s.ToArray())})");
            s.Seek(0, SeekOrigin.Begin);
            using var outStream = new Df2Stream(s, StreamMode.Read);
            outStream.CommandReceive += msg => Out.WriteLine("READ> " + msg);
            outStream.ProcessUntilEnd();
            Assert.Equal(testData, outStream.Values["TestGroup"]["TestNestedGroup"]["TestData"].AsArray<long>());
        }

        [Theory,
         InlineData(new ulong[]
         {
             ulong.MinValue, ulong.MaxValue, 42, 0xFE, 0xFF, 0xFFFE, 0xFFFF, 0xFFFFFE, 0xFFFFFF, 0xFFFFFFFE, 0xFFFFFFFF
         })]
        public void CanWriteAndRecoverULongArray(ulong[] testData)
        {
            using var s = new MemoryStream();
            using var inStream = new Df2Stream(s, StreamMode.Write, true);
            var group = inStream.GetOrAddGroup("TestGroup");
            inStream.CommandSend += msg => Out.WriteLine("WRITE> " + msg);
            group = group.GetOrAddGroup("TestNestedGroup");
            group.AddOrUpdate("TestData", testData);
            inStream.Flush();
            inStream.Close();
            Out.WriteLine($"Data: {Encoding.UTF8.GetString(s.ToArray())} ({BitConverter.ToString(s.ToArray())})");
            s.Seek(0, SeekOrigin.Begin);
            using var outStream = new Df2Stream(s, StreamMode.Read);
            outStream.CommandReceive += msg => Out.WriteLine("READ> " + msg);
            outStream.ProcessUntilEnd();
            Assert.Equal(testData, outStream.Values["TestGroup"]["TestNestedGroup"]["TestData"].AsArray<ulong>());
        }

        [Theory,
         InlineData(new[]
         {
             float.MinValue, float.MaxValue, float.Epsilon, float.PositiveInfinity, float.NegativeInfinity, float.NaN,
             42
         })]
        public void CanWriteAndRecoverFloatArray(float[] testData)
        {
            using var s = new MemoryStream();
            using var inStream = new Df2Stream(s, StreamMode.Write, true);
            var group = inStream.GetOrAddGroup("TestGroup");
            inStream.CommandSend += msg => Out.WriteLine("WRITE> " + msg);
            group = group.GetOrAddGroup("TestNestedGroup");
            group.AddOrUpdate("TestData", testData);
            inStream.Flush();
            inStream.Close();
            Out.WriteLine($"Data: {Encoding.UTF8.GetString(s.ToArray())} ({BitConverter.ToString(s.ToArray())})");
            s.Seek(0, SeekOrigin.Begin);
            using var outStream = new Df2Stream(s, StreamMode.Read);
            outStream.CommandReceive += msg => Out.WriteLine("READ> " + msg);
            outStream.ProcessUntilEnd();
            Assert.Equal(testData, outStream.Values["TestGroup"]["TestNestedGroup"]["TestData"].AsArray<float>());
        }

        [Theory,
         InlineData(new[]
         {
             double.MinValue, double.MaxValue, double.Epsilon, double.PositiveInfinity, double.NegativeInfinity,
             double.NaN, 42
         })]
        public void CanWriteAndRecoverDoubleArray(double[] testData)
        {
            using var s = new MemoryStream();
            using var inStream = new Df2Stream(s, StreamMode.Write, true);
            var group = inStream.GetOrAddGroup("TestGroup");
            inStream.CommandSend += msg => Out.WriteLine("WRITE> " + msg);
            group = group.GetOrAddGroup("TestNestedGroup");
            group.AddOrUpdate("TestData", testData);
            inStream.Flush();
            inStream.Close();
            Out.WriteLine($"Data: {Encoding.UTF8.GetString(s.ToArray())} ({BitConverter.ToString(s.ToArray())})");
            s.Seek(0, SeekOrigin.Begin);
            using var outStream = new Df2Stream(s, StreamMode.Read);
            outStream.CommandReceive += msg => Out.WriteLine("READ> " + msg);
            outStream.ProcessUntilEnd();
            Assert.Equal(testData, outStream.Values["TestGroup"]["TestNestedGroup"]["TestData"].AsArray<double>());
        }

        [Theory, InlineData(new object[] {new[] {"Forty Two", "English 日本語 русский язык 官話", "🌄"}})]
        public void CanWriteAndRecoverStringArray(string[] testData)
        {
            using var s = new MemoryStream();
            using var inStream = new Df2Stream(s, StreamMode.Write, true);
            var group = inStream.GetOrAddGroup("TestGroup");
            inStream.CommandSend += msg => Out.WriteLine("WRITE> " + msg);
            group = group.GetOrAddGroup("TestNestedGroup");
            group.AddOrUpdate("TestData", testData);
            inStream.Flush();
            inStream.Close();
            Out.WriteLine($"Data: {Encoding.UTF8.GetString(s.ToArray())} ({BitConverter.ToString(s.ToArray())})");
            s.Seek(0, SeekOrigin.Begin);
            using var outStream = new Df2Stream(s, StreamMode.Read);
            outStream.CommandReceive += msg => Out.WriteLine("READ> " + msg);
            outStream.ProcessUntilEnd();
            Assert.Equal(testData, outStream.Values["TestGroup"]["TestNestedGroup"]["TestData"].AsArray<string>());
        }

        [Theory, InlineData(byte.MinValue), InlineData(byte.MaxValue), InlineData(42)]
        public void CanWriteAndRecoverByteOneInRoot(byte testData)
        {
            using var s = new MemoryStream();
            using var inStream = new Df2Stream(s, StreamMode.Write, true);
            inStream.CommandSend += cmd => Out.WriteLine($"WRITE> {cmd}");
            var group = inStream.GetOrAddGroup("TestGroup");
            group = group.GetOrAddGroup("TestNestedGroup");
            group.AddOrUpdate("TestData", testData);
            inStream.AddOrUpdate("TestData", testData);
            inStream.Flush();
            inStream.Close();
            Out.WriteLine($"Data: {Encoding.UTF8.GetString(s.ToArray())} ({BitConverter.ToString(s.ToArray())})");
            s.Seek(0, SeekOrigin.Begin);
            using var outStream = new Df2Stream(s, StreamMode.Read);
            outStream.CommandReceive += msg => Out.WriteLine("READ> " + msg);
            outStream.ProcessUntilEnd();
            Assert.Equal(testData, outStream.Values["TestGroup"]["TestNestedGroup"]["TestData"].AsByte());
            Assert.Equal(testData, outStream["TestData"].AsByte());
        }

        [Theory, InlineData(sbyte.MinValue), InlineData(sbyte.MaxValue), InlineData(42)]
        public void CanWriteAndRecoverSByteOneInRoot(sbyte testData)
        {
            using var s = new MemoryStream();
            using var inStream = new Df2Stream(s, StreamMode.Write, true);
            inStream.CommandSend += cmd => Out.WriteLine($"WRITE> {cmd}");
            var group = inStream.GetOrAddGroup("TestGroup");
            group = group.GetOrAddGroup("TestNestedGroup");
            group.AddOrUpdate("TestData", testData);
            inStream.AddOrUpdate("TestData", testData);
            inStream.Flush();
            inStream.Close();
            Out.WriteLine($"Data: {Encoding.UTF8.GetString(s.ToArray())} ({BitConverter.ToString(s.ToArray())})");
            s.Seek(0, SeekOrigin.Begin);
            using var outStream = new Df2Stream(s, StreamMode.Read);
            outStream.CommandReceive += msg => Out.WriteLine("READ> " + msg);
            outStream.ProcessUntilEnd();
            Assert.Equal(testData, outStream.Values["TestGroup"]["TestNestedGroup"]["TestData"].AsSByte());
            Assert.Equal(testData, outStream["TestData"].AsSByte());
        }

        [Theory, InlineData(short.MinValue), InlineData(short.MaxValue), InlineData(42), InlineData(0xFE),
         InlineData(0xFF)]
        public void CanWriteAndRecoverShortOneInRoot(short testData)
        {
            using var s = new MemoryStream();
            using var inStream = new Df2Stream(s, StreamMode.Write, true);
            inStream.CommandSend += cmd => Out.WriteLine($"WRITE> {cmd}");
            var group = inStream.GetOrAddGroup("TestGroup");
            group = group.GetOrAddGroup("TestNestedGroup");
            group.AddOrUpdate("TestData", testData);
            inStream.AddOrUpdate("TestData", testData);
            inStream.Flush();
            inStream.Close();
            Out.WriteLine($"Data: {Encoding.UTF8.GetString(s.ToArray())} ({BitConverter.ToString(s.ToArray())})");
            s.Seek(0, SeekOrigin.Begin);
            using var outStream = new Df2Stream(s, StreamMode.Read);
            outStream.CommandReceive += msg => Out.WriteLine("READ> " + msg);
            outStream.ProcessUntilEnd();
            Assert.Equal(testData, outStream.Values["TestGroup"]["TestNestedGroup"]["TestData"].AsShort());
            Assert.Equal(testData, outStream["TestData"].AsShort());
        }

        [Theory, InlineData(ushort.MinValue), InlineData(ushort.MaxValue), InlineData(42), InlineData(0xFE),
         InlineData(0xFF)]
        public void CanWriteAndRecoverUShortOneInRoot(ushort testData)
        {
            using var s = new MemoryStream();
            using var inStream = new Df2Stream(s, StreamMode.Write, true);
            inStream.CommandSend += cmd => Out.WriteLine($"WRITE> {cmd}");
            var group = inStream.GetOrAddGroup("TestGroup");
            group = group.GetOrAddGroup("TestNestedGroup");
            group.AddOrUpdate("TestData", testData);
            inStream.AddOrUpdate("TestData", testData);
            inStream.Flush();
            inStream.Close();
            Out.WriteLine($"Data: {Encoding.UTF8.GetString(s.ToArray())} ({BitConverter.ToString(s.ToArray())})");
            s.Seek(0, SeekOrigin.Begin);
            using var outStream = new Df2Stream(s, StreamMode.Read);
            outStream.CommandReceive += msg => Out.WriteLine("READ> " + msg);
            outStream.ProcessUntilEnd();
            Assert.Equal(testData, outStream.Values["TestGroup"]["TestNestedGroup"]["TestData"].AsUShort());
            Assert.Equal(testData, outStream["TestData"].AsUShort());
        }

        [Theory, InlineData(int.MinValue), InlineData(int.MaxValue), InlineData(42), InlineData(0xFE), InlineData(0xFF),
         InlineData(0xFFFE), InlineData(0xFFFF)]
        public void CanWriteAndRecoverIntOneInRoot(int testData)
        {
            using var s = new MemoryStream();
            using var inStream = new Df2Stream(s, StreamMode.Write, true);
            inStream.CommandSend += cmd => Out.WriteLine($"WRITE> {cmd}");
            var group = inStream.GetOrAddGroup("TestGroup");
            group = group.GetOrAddGroup("TestNestedGroup");
            group.AddOrUpdate("TestData", testData);
            inStream.AddOrUpdate("TestData", testData);
            inStream.Flush();
            inStream.Close();
            Out.WriteLine($"Data: {Encoding.UTF8.GetString(s.ToArray())} ({BitConverter.ToString(s.ToArray())})");
            s.Seek(0, SeekOrigin.Begin);
            using var outStream = new Df2Stream(s, StreamMode.Read);
            outStream.CommandReceive += msg => Out.WriteLine("READ> " + msg);
            outStream.ProcessUntilEnd();
            Assert.Equal(testData, outStream.Values["TestGroup"]["TestNestedGroup"]["TestData"].AsInt());
            Assert.Equal(testData, outStream["TestData"].AsInt());
        }

        [Theory, InlineData(uint.MinValue), InlineData(uint.MaxValue), InlineData(42), InlineData(0xFE),
         InlineData(0xFF), InlineData(0xFFFE), InlineData(0xFFFF)]
        public void CanWriteAndRecoverUIntOneInRoot(uint testData)
        {
            using var s = new MemoryStream();
            using var inStream = new Df2Stream(s, StreamMode.Write, true);
            inStream.CommandSend += cmd => Out.WriteLine($"WRITE> {cmd}");
            var group = inStream.GetOrAddGroup("TestGroup");
            group = group.GetOrAddGroup("TestNestedGroup");
            group.AddOrUpdate("TestData", testData);
            inStream.AddOrUpdate("TestData", testData);
            inStream.Flush();
            inStream.Close();
            Out.WriteLine($"Data: {Encoding.UTF8.GetString(s.ToArray())} ({BitConverter.ToString(s.ToArray())})");
            s.Seek(0, SeekOrigin.Begin);
            using var outStream = new Df2Stream(s, StreamMode.Read);
            outStream.CommandReceive += msg => Out.WriteLine("READ> " + msg);
            outStream.ProcessUntilEnd();
            Assert.Equal(testData, outStream.Values["TestGroup"]["TestNestedGroup"]["TestData"].AsUInt());
            Assert.Equal(testData, outStream["TestData"].AsUInt());
        }

        [Theory, InlineData(long.MinValue), InlineData(long.MaxValue), InlineData(42), InlineData(0xFE),
         InlineData(0xFF), InlineData(0xFFFE), InlineData(0xFFFF), InlineData(0xFFFFFE), InlineData(0xFFFFFF),
         InlineData(0xFFFFFFFE), InlineData(0xFFFFFFFF)]
        public void CanWriteAndRecoverLongOneInRoot(long testData)
        {
            using var s = new MemoryStream();
            using var inStream = new Df2Stream(s, StreamMode.Write, true);
            inStream.CommandSend += cmd => Out.WriteLine($"WRITE> {cmd}");
            var group = inStream.GetOrAddGroup("TestGroup");
            group = group.GetOrAddGroup("TestNestedGroup");
            group.AddOrUpdate("TestData", testData);
            inStream.AddOrUpdate("TestData", testData);
            inStream.Flush();
            inStream.Close();
            Out.WriteLine($"Data: {Encoding.UTF8.GetString(s.ToArray())} ({BitConverter.ToString(s.ToArray())})");
            s.Seek(0, SeekOrigin.Begin);
            using var outStream = new Df2Stream(s, StreamMode.Read);
            outStream.CommandReceive += msg => Out.WriteLine("READ> " + msg);
            outStream.ProcessUntilEnd();
            Assert.Equal(testData, outStream.Values["TestGroup"]["TestNestedGroup"]["TestData"].AsLong());
            Assert.Equal(testData, outStream["TestData"].AsLong());
        }

        [Theory, InlineData(ulong.MinValue), InlineData(ulong.MaxValue), InlineData(42), InlineData(0xFE),
         InlineData(0xFF), InlineData(0xFFFE), InlineData(0xFFFF), InlineData(0xFFFFFE), InlineData(0xFFFFFF),
         InlineData(0xFFFFFFFE), InlineData(0xFFFFFFFF)]
        public void CanWriteAndRecoverULongOneInRoot(ulong testData)
        {
            using var s = new MemoryStream();
            using var inStream = new Df2Stream(s, StreamMode.Write, true);
            inStream.CommandSend += cmd => Out.WriteLine($"WRITE> {cmd}");
            var group = inStream.GetOrAddGroup("TestGroup");
            group = group.GetOrAddGroup("TestNestedGroup");
            group.AddOrUpdate("TestData", testData);
            inStream.AddOrUpdate("TestData", testData);
            inStream.Flush();
            inStream.Close();
            Out.WriteLine($"Data: {Encoding.UTF8.GetString(s.ToArray())} ({BitConverter.ToString(s.ToArray())})");
            s.Seek(0, SeekOrigin.Begin);
            using var outStream = new Df2Stream(s, StreamMode.Read);
            outStream.CommandReceive += msg => Out.WriteLine("READ> " + msg);
            outStream.ProcessUntilEnd();
            Assert.Equal(testData, outStream.Values["TestGroup"]["TestNestedGroup"]["TestData"].AsULong());
            Assert.Equal(testData, outStream["TestData"].AsULong());
        }

        [Theory, InlineData(float.MinValue), InlineData(float.MaxValue), InlineData(float.Epsilon),
         InlineData(float.PositiveInfinity), InlineData(float.NegativeInfinity), InlineData(float.NaN), InlineData(42)]
        public void CanWriteAndRecoverFloatOneInRoot(float testData)
        {
            using var s = new MemoryStream();
            using var inStream = new Df2Stream(s, StreamMode.Write, true);
            inStream.CommandSend += cmd => Out.WriteLine($"WRITE> {cmd}");
            var group = inStream.GetOrAddGroup("TestGroup");
            group = group.GetOrAddGroup("TestNestedGroup");
            group.AddOrUpdate("TestData", testData);
            inStream.AddOrUpdate("TestData", testData);
            inStream.Flush();
            inStream.Close();
            Out.WriteLine($"Data: {Encoding.UTF8.GetString(s.ToArray())} ({BitConverter.ToString(s.ToArray())})");
            s.Seek(0, SeekOrigin.Begin);
            using var outStream = new Df2Stream(s, StreamMode.Read);
            outStream.CommandReceive += msg => Out.WriteLine("READ> " + msg);
            outStream.ProcessUntilEnd();
            Assert.Equal(testData, outStream.Values["TestGroup"]["TestNestedGroup"]["TestData"].AsFloat());
            Assert.Equal(testData, outStream["TestData"].AsFloat());
        }

        [Theory, InlineData(double.MinValue), InlineData(double.MaxValue), InlineData(double.Epsilon),
         InlineData(double.PositiveInfinity), InlineData(double.NegativeInfinity), InlineData(double.NaN),
         InlineData(42)]
        public void CanWriteAndRecoverDoubleOneInRoot(double testData)
        {
            using var s = new MemoryStream();
            using var inStream = new Df2Stream(s, StreamMode.Write, true);
            inStream.CommandSend += cmd => Out.WriteLine($"WRITE> {cmd}");
            var group = inStream.GetOrAddGroup("TestGroup");
            group = group.GetOrAddGroup("TestNestedGroup");
            group.AddOrUpdate("TestData", testData);
            inStream.AddOrUpdate("TestData", testData);
            inStream.Flush();
            inStream.Close();
            Out.WriteLine($"Data: {Encoding.UTF8.GetString(s.ToArray())} ({BitConverter.ToString(s.ToArray())})");
            s.Seek(0, SeekOrigin.Begin);
            using var outStream = new Df2Stream(s, StreamMode.Read);
            outStream.CommandReceive += msg => Out.WriteLine("READ> " + msg);
            outStream.ProcessUntilEnd();
            Assert.Equal(testData, outStream.Values["TestGroup"]["TestNestedGroup"]["TestData"].AsDouble());
            Assert.Equal(testData, outStream["TestData"].AsDouble());
        }

        [Theory, InlineData("Forty Two"), InlineData("English 日本語 русский язык 官話"), InlineData("🌄"),
         MemberData(nameof(GetBigStrings))]
        public void CanWriteAndRecoverStringOneInRoot(string testData)
        {
            using var s = new MemoryStream();
            using var inStream = new Df2Stream(s, StreamMode.Write, true);
            inStream.CommandSend += msg => Out.WriteLine("WRITE> " + msg);
            var group = inStream.GetOrAddGroup("TestGroup");
            group = group.GetOrAddGroup("TestNestedGroup");
            group.AddOrUpdate("TestData", testData);
            inStream.AddOrUpdate("TestData", testData);
            inStream.Flush();
            inStream.Close();
            Out.WriteLine($"Data: {Encoding.UTF8.GetString(s.ToArray())} ({BitConverter.ToString(s.ToArray())})");
            s.Seek(0, SeekOrigin.Begin);
            using var outStream = new Df2Stream(s, StreamMode.Read);
            outStream.CommandReceive += msg => Out.WriteLine("READ> " + msg);
            outStream.ProcessUntilEnd();
            Assert.Equal(testData, outStream.Values["TestGroup"]["TestNestedGroup"]["TestData"].AsString());
            Assert.Equal(testData, outStream["TestData"].AsString());
        }

        [Theory, InlineData(new byte[] {byte.MinValue, byte.MaxValue, 42})]
        public void CanWriteAndRecoverTwoByteArraysOneInRoot(byte[] testData)
        {
            using var s = new MemoryStream();
            using var inStream = new Df2Stream(s, StreamMode.Write, true);
            var group = inStream.GetOrAddGroup("TestGroup");
            inStream.CommandSend += msg => Out.WriteLine("WRITE> " + msg);
            group = group.GetOrAddGroup("TestNestedGroup");
            group.AddOrUpdate("TestData", testData);
            inStream.AddOrUpdate("TestData", testData);
            inStream.Flush();
            inStream.Close();
            Out.WriteLine($"Data: {Encoding.UTF8.GetString(s.ToArray())} ({BitConverter.ToString(s.ToArray())})");
            s.Seek(0, SeekOrigin.Begin);
            using var outStream = new Df2Stream(s, StreamMode.Read);
            outStream.CommandReceive += msg => Out.WriteLine("READ> " + msg);
            outStream.ProcessUntilEnd();
            Assert.Equal(testData, outStream.Values["TestGroup"]["TestNestedGroup"]["TestData"].AsArray<byte>());
            Assert.Equal(testData, outStream["TestData"].AsArray<byte>());
        }

        [Theory, InlineData(new sbyte[] {sbyte.MinValue, sbyte.MaxValue, 42})]
        public void CanWriteAndRecoverTwoSByteArraysOneInRoot(sbyte[] testData)
        {
            using var s = new MemoryStream();
            using var inStream = new Df2Stream(s, StreamMode.Write, true);
            var group = inStream.GetOrAddGroup("TestGroup");
            inStream.CommandSend += msg => Out.WriteLine("WRITE> " + msg);
            group = group.GetOrAddGroup("TestNestedGroup");
            group.AddOrUpdate("TestData", testData);
            inStream.AddOrUpdate("TestData", testData);
            inStream.Flush();
            inStream.Close();
            Out.WriteLine($"Data: {Encoding.UTF8.GetString(s.ToArray())} ({BitConverter.ToString(s.ToArray())})");
            s.Seek(0, SeekOrigin.Begin);
            using var outStream = new Df2Stream(s, StreamMode.Read);
            outStream.CommandReceive += msg => Out.WriteLine("READ> " + msg);
            outStream.ProcessUntilEnd();
            Assert.Equal(testData, outStream.Values["TestGroup"]["TestNestedGroup"]["TestData"].AsArray<sbyte>());
            Assert.Equal(testData, outStream["TestData"].AsArray<sbyte>());
        }

        [Theory, InlineData(new short[] {short.MinValue, short.MaxValue, 42, 0xFE, 0xFF})]
        public void CanWriteAndRecoverTwoShortArraysOneInRoot(short[] testData)
        {
            using var s = new MemoryStream();
            using var inStream = new Df2Stream(s, StreamMode.Write, true);
            var group = inStream.GetOrAddGroup("TestGroup");
            inStream.CommandSend += msg => Out.WriteLine("WRITE> " + msg);
            group = group.GetOrAddGroup("TestNestedGroup");
            group.AddOrUpdate("TestData", testData);
            inStream.AddOrUpdate("TestData", testData);
            inStream.Flush();
            inStream.Close();
            Out.WriteLine($"Data: {Encoding.UTF8.GetString(s.ToArray())} ({BitConverter.ToString(s.ToArray())})");
            s.Seek(0, SeekOrigin.Begin);
            using var outStream = new Df2Stream(s, StreamMode.Read);
            outStream.CommandReceive += msg => Out.WriteLine("READ> " + msg);
            outStream.ProcessUntilEnd();
            Assert.Equal(testData, outStream.Values["TestGroup"]["TestNestedGroup"]["TestData"].AsArray<short>());
            Assert.Equal(testData, outStream["TestData"].AsArray<short>());
        }

        [Theory, InlineData(new ushort[] {ushort.MinValue, ushort.MaxValue, 42, 0xFE, 0xFF})]
        public void CanWriteAndRecoverTwoUShortArraysOneInRoot(ushort[] testData)
        {
            using var s = new MemoryStream();
            using var inStream = new Df2Stream(s, StreamMode.Write, true);
            var group = inStream.GetOrAddGroup("TestGroup");
            inStream.CommandSend += msg => Out.WriteLine("WRITE> " + msg);
            group = group.GetOrAddGroup("TestNestedGroup");
            group.AddOrUpdate("TestData", testData);
            inStream.AddOrUpdate("TestData", testData);
            inStream.Flush();
            inStream.Close();
            Out.WriteLine($"Data: {Encoding.UTF8.GetString(s.ToArray())} ({BitConverter.ToString(s.ToArray())})");
            s.Seek(0, SeekOrigin.Begin);
            using var outStream = new Df2Stream(s, StreamMode.Read);
            outStream.CommandReceive += msg => Out.WriteLine("READ> " + msg);
            outStream.ProcessUntilEnd();
            Assert.Equal(testData, outStream.Values["TestGroup"]["TestNestedGroup"]["TestData"].AsArray<ushort>());
            Assert.Equal(testData, outStream["TestData"].AsArray<ushort>());
        }

        [Theory, InlineData(new[] {int.MinValue, int.MaxValue, 42, 0xFE, 0xFF, 0xFFFE, 0xFFFF})]
        public void CanWriteAndRecoverTwoIntArraysOneInRoot(int[] testData)
        {
            using var s = new MemoryStream();
            using var inStream = new Df2Stream(s, StreamMode.Write, true);
            var group = inStream.GetOrAddGroup("TestGroup");
            inStream.CommandSend += msg => Out.WriteLine("WRITE> " + msg);
            group = group.GetOrAddGroup("TestNestedGroup");
            group.AddOrUpdate("TestData", testData);
            inStream.AddOrUpdate("TestData", testData);
            inStream.Flush();
            inStream.Close();
            Out.WriteLine($"Data: {Encoding.UTF8.GetString(s.ToArray())} ({BitConverter.ToString(s.ToArray())})");
            s.Seek(0, SeekOrigin.Begin);
            using var outStream = new Df2Stream(s, StreamMode.Read);
            outStream.CommandReceive += msg => Out.WriteLine("READ> " + msg);
            outStream.ProcessUntilEnd();
            Assert.Equal(testData, outStream.Values["TestGroup"]["TestNestedGroup"]["TestData"].AsArray<int>());
            Assert.Equal(testData, outStream["TestData"].AsArray<int>());
        }

        [Theory, InlineData(new uint[] {uint.MinValue, uint.MaxValue, 42, 0xFE, 0xFF, 0xFFFE, 0xFFFF})]
        public void CanWriteAndRecoverTwoUIntArraysOneInRoot(uint[] testData)
        {
            using var s = new MemoryStream();
            using var inStream = new Df2Stream(s, StreamMode.Write, true);
            var group = inStream.GetOrAddGroup("TestGroup");
            inStream.CommandSend += msg => Out.WriteLine("WRITE> " + msg);
            group = group.GetOrAddGroup("TestNestedGroup");
            group.AddOrUpdate("TestData", testData);
            inStream.AddOrUpdate("TestData", testData);
            inStream.Flush();
            inStream.Close();
            Out.WriteLine($"Data: {Encoding.UTF8.GetString(s.ToArray())} ({BitConverter.ToString(s.ToArray())})");
            s.Seek(0, SeekOrigin.Begin);
            using var outStream = new Df2Stream(s, StreamMode.Read);
            outStream.CommandReceive += msg => Out.WriteLine("READ> " + msg);
            outStream.ProcessUntilEnd();
            Assert.Equal(testData, outStream.Values["TestGroup"]["TestNestedGroup"]["TestData"].AsArray<uint>());
            Assert.Equal(testData, outStream["TestData"].AsArray<uint>());
        }

        [Theory,
         InlineData(new[]
         {
             long.MinValue, long.MaxValue, 42, 0xFE, 0xFF, 0xFFFE, 0xFFFF, 0xFFFFFE, 0xFFFFFF, 0xFFFFFFFE, 0xFFFFFFFF
         })]
        public void CanWriteAndRecoverTwoLongArraysOneInRoot(long[] testData)
        {
            using var s = new MemoryStream();
            using var inStream = new Df2Stream(s, StreamMode.Write, true);
            var group = inStream.GetOrAddGroup("TestGroup");
            inStream.CommandSend += msg => Out.WriteLine("WRITE> " + msg);
            group = group.GetOrAddGroup("TestNestedGroup");
            group.AddOrUpdate("TestData", testData);
            inStream.AddOrUpdate("TestData", testData);
            inStream.Flush();
            inStream.Close();
            Out.WriteLine($"Data: {Encoding.UTF8.GetString(s.ToArray())} ({BitConverter.ToString(s.ToArray())})");
            s.Seek(0, SeekOrigin.Begin);
            using var outStream = new Df2Stream(s, StreamMode.Read);
            outStream.CommandReceive += msg => Out.WriteLine("READ> " + msg);
            outStream.ProcessUntilEnd();
            Assert.Equal(testData, outStream.Values["TestGroup"]["TestNestedGroup"]["TestData"].AsArray<long>());
            Assert.Equal(testData, outStream["TestData"].AsArray<long>());
        }

        [Theory,
         InlineData(new ulong[]
         {
             ulong.MinValue, ulong.MaxValue, 42, 0xFE, 0xFF, 0xFFFE, 0xFFFF, 0xFFFFFE, 0xFFFFFF, 0xFFFFFFFE, 0xFFFFFFFF
         })]
        public void CanWriteAndRecoverTwoULongArraysOneInRoot(ulong[] testData)
        {
            using var s = new MemoryStream();
            using var inStream = new Df2Stream(s, StreamMode.Write, true);
            var group = inStream.GetOrAddGroup("TestGroup");
            inStream.CommandSend += msg => Out.WriteLine("WRITE> " + msg);
            group = group.GetOrAddGroup("TestNestedGroup");
            group.AddOrUpdate("TestData", testData);
            inStream.AddOrUpdate("TestData", testData);
            inStream.Flush();
            inStream.Close();
            Out.WriteLine($"Data: {Encoding.UTF8.GetString(s.ToArray())} ({BitConverter.ToString(s.ToArray())})");
            s.Seek(0, SeekOrigin.Begin);
            using var outStream = new Df2Stream(s, StreamMode.Read);
            outStream.CommandReceive += msg => Out.WriteLine("READ> " + msg);
            outStream.ProcessUntilEnd();
            Assert.Equal(testData, outStream.Values["TestGroup"]["TestNestedGroup"]["TestData"].AsArray<ulong>());
            Assert.Equal(testData, outStream["TestData"].AsArray<ulong>());
        }

        [Theory,
         InlineData(new[]
         {
             float.MinValue, float.MaxValue, float.Epsilon, float.PositiveInfinity, float.NegativeInfinity, float.NaN,
             42
         })]
        public void CanWriteAndRecoverTwoFloatArraysOneInRoot(float[] testData)
        {
            using var s = new MemoryStream();
            using var inStream = new Df2Stream(s, StreamMode.Write, true);
            var group = inStream.GetOrAddGroup("TestGroup");
            inStream.CommandSend += msg => Out.WriteLine("WRITE> " + msg);
            group = group.GetOrAddGroup("TestNestedGroup");
            group.AddOrUpdate("TestData", testData);
            inStream.AddOrUpdate("TestData", testData);
            inStream.Flush();
            inStream.Close();
            Out.WriteLine($"Data: {Encoding.UTF8.GetString(s.ToArray())} ({BitConverter.ToString(s.ToArray())})");
            s.Seek(0, SeekOrigin.Begin);
            using var outStream = new Df2Stream(s, StreamMode.Read);
            outStream.CommandReceive += msg => Out.WriteLine("READ> " + msg);
            outStream.ProcessUntilEnd();
            Assert.Equal(testData, outStream.Values["TestGroup"]["TestNestedGroup"]["TestData"].AsArray<float>());
            Assert.Equal(testData, outStream["TestData"].AsArray<float>());
        }

        [Theory,
         InlineData(new[]
         {
             double.MinValue, double.MaxValue, double.Epsilon, double.PositiveInfinity, double.NegativeInfinity,
             double.NaN, 42
         })]
        public void CanWriteAndRecoverTwoDoubleArraysOneInRoot(double[] testData)
        {
            using var s = new MemoryStream();
            using var inStream = new Df2Stream(s, StreamMode.Write, true);
            var group = inStream.GetOrAddGroup("TestGroup");
            inStream.CommandSend += msg => Out.WriteLine("WRITE> " + msg);
            group = group.GetOrAddGroup("TestNestedGroup");
            group.AddOrUpdate("TestData", testData);
            inStream.AddOrUpdate("TestData", testData);
            inStream.Flush();
            inStream.Close();
            Out.WriteLine($"Data: {Encoding.UTF8.GetString(s.ToArray())} ({BitConverter.ToString(s.ToArray())})");
            s.Seek(0, SeekOrigin.Begin);
            using var outStream = new Df2Stream(s, StreamMode.Read);
            outStream.CommandReceive += msg => Out.WriteLine("READ> " + msg);
            outStream.ProcessUntilEnd();
            Assert.Equal(testData, outStream.Values["TestGroup"]["TestNestedGroup"]["TestData"].AsArray<double>());
            Assert.Equal(testData, outStream["TestData"].AsArray<double>());
        }

        [Theory, InlineData(new object[] {new[] {"Forty Two", "English 日本語 русский язык 官話", "🌄"}})]
        public void CanWriteAndRecoverTwoStringArraysOneInRoot(string[] testData)
        {
            using var s = new MemoryStream();
            using var inStream = new Df2Stream(s, StreamMode.Write, true);
            var group = inStream.GetOrAddGroup("TestGroup");
            inStream.CommandSend += msg => Out.WriteLine("WRITE> " + msg);
            group = group.GetOrAddGroup("TestNestedGroup");
            group.AddOrUpdate("TestData", testData);
            inStream.AddOrUpdate("TestData", testData);
            inStream.Flush();
            inStream.Close();
            Out.WriteLine($"Data: {Encoding.UTF8.GetString(s.ToArray())} ({BitConverter.ToString(s.ToArray())})");
            s.Seek(0, SeekOrigin.Begin);
            using var outStream = new Df2Stream(s, StreamMode.Read);
            outStream.CommandReceive += msg => Out.WriteLine("READ> " + msg);
            outStream.ProcessUntilEnd();
            Assert.Equal(testData, outStream.Values["TestGroup"]["TestNestedGroup"]["TestData"].AsArray<string>());
            Assert.Equal(testData, outStream["TestData"].AsArray<string>());
        }

        [Theory, InlineData(byte.MinValue), InlineData(byte.MaxValue), InlineData(42)]
        public void CanWriteAndRecoverByteOneElsewhereNested(byte testData)
        {
            using var s = new MemoryStream();
            using var inStream = new Df2Stream(s, StreamMode.Write, true);
            inStream.CommandSend += cmd => Out.WriteLine($"WRITE> {cmd}");
            var group = inStream.GetOrAddGroup("TestGroup");
            group = group.GetOrAddGroup("TestNestedGroup");
            group.AddOrUpdate("TestData", testData);
            var group2 = inStream.GetOrAddGroup("TestGroup2");
            group2 = group2.GetOrAddGroup("TestNestedGroup");
            group2.AddOrUpdate("TestData", testData);
            inStream.Flush();
            inStream.Close();
            Out.WriteLine($"Data: {Encoding.UTF8.GetString(s.ToArray())} ({BitConverter.ToString(s.ToArray())})");
            s.Seek(0, SeekOrigin.Begin);
            using var outStream = new Df2Stream(s, StreamMode.Read);
            outStream.CommandReceive += msg => Out.WriteLine("READ> " + msg);
            outStream.ProcessUntilEnd();
            Assert.Equal(testData, outStream.Values["TestGroup"]["TestNestedGroup"]["TestData"].AsByte());
            Assert.Equal(testData, outStream["TestGroup2"]["TestNestedGroup"]["TestData"].AsByte());
        }

        [Theory, InlineData(sbyte.MinValue), InlineData(sbyte.MaxValue), InlineData(42)]
        public void CanWriteAndRecoverSByteOneElsewhereNested(sbyte testData)
        {
            using var s = new MemoryStream();
            using var inStream = new Df2Stream(s, StreamMode.Write, true);
            inStream.CommandSend += cmd => Out.WriteLine($"WRITE> {cmd}");
            var group = inStream.GetOrAddGroup("TestGroup");
            group = group.GetOrAddGroup("TestNestedGroup");
            group.AddOrUpdate("TestData", testData);
            var group2 = inStream.GetOrAddGroup("TestGroup2");
            group2 = group2.GetOrAddGroup("TestNestedGroup");
            group2.AddOrUpdate("TestData", testData);
            inStream.Flush();
            inStream.Close();
            Out.WriteLine($"Data: {Encoding.UTF8.GetString(s.ToArray())} ({BitConverter.ToString(s.ToArray())})");
            s.Seek(0, SeekOrigin.Begin);
            using var outStream = new Df2Stream(s, StreamMode.Read);
            outStream.CommandReceive += msg => Out.WriteLine("READ> " + msg);
            outStream.ProcessUntilEnd();
            Assert.Equal(testData, outStream.Values["TestGroup"]["TestNestedGroup"]["TestData"].AsSByte());
            Assert.Equal(testData, outStream["TestGroup2"]["TestNestedGroup"]["TestData"].AsSByte());
        }

        [Theory, InlineData(short.MinValue), InlineData(short.MaxValue), InlineData(42), InlineData(0xFE),
         InlineData(0xFF)]
        public void CanWriteAndRecoverShortOneElsewhereNested(short testData)
        {
            using var s = new MemoryStream();
            using var inStream = new Df2Stream(s, StreamMode.Write, true);
            inStream.CommandSend += cmd => Out.WriteLine($"WRITE> {cmd}");
            var group = inStream.GetOrAddGroup("TestGroup");
            group = group.GetOrAddGroup("TestNestedGroup");
            group.AddOrUpdate("TestData", testData);
            var group2 = inStream.GetOrAddGroup("TestGroup2");
            group2 = group2.GetOrAddGroup("TestNestedGroup");
            group2.AddOrUpdate("TestData", testData);
            inStream.Flush();
            inStream.Close();
            Out.WriteLine($"Data: {Encoding.UTF8.GetString(s.ToArray())} ({BitConverter.ToString(s.ToArray())})");
            s.Seek(0, SeekOrigin.Begin);
            using var outStream = new Df2Stream(s, StreamMode.Read);
            outStream.CommandReceive += msg => Out.WriteLine("READ> " + msg);
            outStream.ProcessUntilEnd();
            Assert.Equal(testData, outStream.Values["TestGroup"]["TestNestedGroup"]["TestData"].AsShort());
            Assert.Equal(testData, outStream["TestGroup2"]["TestNestedGroup"]["TestData"].AsShort());
        }

        [Theory, InlineData(ushort.MinValue), InlineData(ushort.MaxValue), InlineData(42), InlineData(0xFE),
         InlineData(0xFF)]
        public void CanWriteAndRecoverUShortOneElsewhereNested(ushort testData)
        {
            using var s = new MemoryStream();
            using var inStream = new Df2Stream(s, StreamMode.Write, true);
            inStream.CommandSend += cmd => Out.WriteLine($"WRITE> {cmd}");
            var group = inStream.GetOrAddGroup("TestGroup");
            group = group.GetOrAddGroup("TestNestedGroup");
            group.AddOrUpdate("TestData", testData);
            var group2 = inStream.GetOrAddGroup("TestGroup2");
            group2 = group2.GetOrAddGroup("TestNestedGroup");
            group2.AddOrUpdate("TestData", testData);
            inStream.Flush();
            inStream.Close();
            Out.WriteLine($"Data: {Encoding.UTF8.GetString(s.ToArray())} ({BitConverter.ToString(s.ToArray())})");
            s.Seek(0, SeekOrigin.Begin);
            using var outStream = new Df2Stream(s, StreamMode.Read);
            outStream.CommandReceive += msg => Out.WriteLine("READ> " + msg);
            outStream.ProcessUntilEnd();
            Assert.Equal(testData, outStream.Values["TestGroup"]["TestNestedGroup"]["TestData"].AsUShort());
            Assert.Equal(testData, outStream["TestGroup2"]["TestNestedGroup"]["TestData"].AsUShort());
        }

        [Theory, InlineData(int.MinValue), InlineData(int.MaxValue), InlineData(42), InlineData(0xFE), InlineData(0xFF),
         InlineData(0xFFFE), InlineData(0xFFFF)]
        public void CanWriteAndRecoverIntOneElsewhereNested(int testData)
        {
            using var s = new MemoryStream();
            using var inStream = new Df2Stream(s, StreamMode.Write, true);
            inStream.CommandSend += cmd => Out.WriteLine($"WRITE> {cmd}");
            var group = inStream.GetOrAddGroup("TestGroup");
            group = group.GetOrAddGroup("TestNestedGroup");
            group.AddOrUpdate("TestData", testData);
            var group2 = inStream.GetOrAddGroup("TestGroup2");
            group2 = group2.GetOrAddGroup("TestNestedGroup");
            group2.AddOrUpdate("TestData", testData);
            inStream.Flush();
            inStream.Close();
            Out.WriteLine($"Data: {Encoding.UTF8.GetString(s.ToArray())} ({BitConverter.ToString(s.ToArray())})");
            s.Seek(0, SeekOrigin.Begin);
            using var outStream = new Df2Stream(s, StreamMode.Read);
            outStream.CommandReceive += msg => Out.WriteLine("READ> " + msg);
            outStream.ProcessUntilEnd();
            Assert.Equal(testData, outStream.Values["TestGroup"]["TestNestedGroup"]["TestData"].AsInt());
            Assert.Equal(testData, outStream["TestGroup2"]["TestNestedGroup"]["TestData"].AsInt());
        }

        [Theory, InlineData(uint.MinValue), InlineData(uint.MaxValue), InlineData(42), InlineData(0xFE),
         InlineData(0xFF), InlineData(0xFFFE), InlineData(0xFFFF)]
        public void CanWriteAndRecoverUIntOneElsewhereNested(uint testData)
        {
            using var s = new MemoryStream();
            using var inStream = new Df2Stream(s, StreamMode.Write, true);
            inStream.CommandSend += cmd => Out.WriteLine($"WRITE> {cmd}");
            var group = inStream.GetOrAddGroup("TestGroup");
            group = group.GetOrAddGroup("TestNestedGroup");
            group.AddOrUpdate("TestData", testData);
            var group2 = inStream.GetOrAddGroup("TestGroup2");
            group2 = group2.GetOrAddGroup("TestNestedGroup");
            group2.AddOrUpdate("TestData", testData);
            inStream.Flush();
            inStream.Close();
            Out.WriteLine($"Data: {Encoding.UTF8.GetString(s.ToArray())} ({BitConverter.ToString(s.ToArray())})");
            s.Seek(0, SeekOrigin.Begin);
            using var outStream = new Df2Stream(s, StreamMode.Read);
            outStream.CommandReceive += msg => Out.WriteLine("READ> " + msg);
            outStream.ProcessUntilEnd();
            Assert.Equal(testData, outStream.Values["TestGroup"]["TestNestedGroup"]["TestData"].AsUInt());
            Assert.Equal(testData, outStream["TestGroup2"]["TestNestedGroup"]["TestData"].AsUInt());
        }

        [Theory, InlineData(long.MinValue), InlineData(long.MaxValue), InlineData(42), InlineData(0xFE),
         InlineData(0xFF), InlineData(0xFFFE), InlineData(0xFFFF), InlineData(0xFFFFFE), InlineData(0xFFFFFF),
         InlineData(0xFFFFFFFE), InlineData(0xFFFFFFFF)]
        public void CanWriteAndRecoverLongOneElsewhereNested(long testData)
        {
            using var s = new MemoryStream();
            using var inStream = new Df2Stream(s, StreamMode.Write, true);
            inStream.CommandSend += cmd => Out.WriteLine($"WRITE> {cmd}");
            var group = inStream.GetOrAddGroup("TestGroup");
            group = group.GetOrAddGroup("TestNestedGroup");
            group.AddOrUpdate("TestData", testData);
            var group2 = inStream.GetOrAddGroup("TestGroup2");
            group2 = group2.GetOrAddGroup("TestNestedGroup");
            group2.AddOrUpdate("TestData", testData);
            inStream.Flush();
            inStream.Close();
            Out.WriteLine($"Data: {Encoding.UTF8.GetString(s.ToArray())} ({BitConverter.ToString(s.ToArray())})");
            s.Seek(0, SeekOrigin.Begin);
            using var outStream = new Df2Stream(s, StreamMode.Read);
            outStream.CommandReceive += msg => Out.WriteLine("READ> " + msg);
            outStream.ProcessUntilEnd();
            Assert.Equal(testData, outStream.Values["TestGroup"]["TestNestedGroup"]["TestData"].AsLong());
            Assert.Equal(testData, outStream["TestGroup2"]["TestNestedGroup"]["TestData"].AsLong());
        }

        [Theory, InlineData(ulong.MinValue), InlineData(ulong.MaxValue), InlineData(42), InlineData(0xFE),
         InlineData(0xFF), InlineData(0xFFFE), InlineData(0xFFFF), InlineData(0xFFFFFE), InlineData(0xFFFFFF),
         InlineData(0xFFFFFFFE), InlineData(0xFFFFFFFF)]
        public void CanWriteAndRecoverULongOneElsewhereNested(ulong testData)
        {
            using var s = new MemoryStream();
            using var inStream = new Df2Stream(s, StreamMode.Write, true);
            inStream.CommandSend += cmd => Out.WriteLine($"WRITE> {cmd}");
            var group = inStream.GetOrAddGroup("TestGroup");
            group = group.GetOrAddGroup("TestNestedGroup");
            group.AddOrUpdate("TestData", testData);
            var group2 = inStream.GetOrAddGroup("TestGroup2");
            group2 = group2.GetOrAddGroup("TestNestedGroup");
            group2.AddOrUpdate("TestData", testData);
            inStream.Flush();
            inStream.Close();
            Out.WriteLine($"Data: {Encoding.UTF8.GetString(s.ToArray())} ({BitConverter.ToString(s.ToArray())})");
            s.Seek(0, SeekOrigin.Begin);
            using var outStream = new Df2Stream(s, StreamMode.Read);
            outStream.CommandReceive += msg => Out.WriteLine("READ> " + msg);
            outStream.ProcessUntilEnd();
            Assert.Equal(testData, outStream.Values["TestGroup"]["TestNestedGroup"]["TestData"].AsULong());
            Assert.Equal(testData, outStream["TestGroup2"]["TestNestedGroup"]["TestData"].AsULong());
        }

        [Theory, InlineData(float.MinValue), InlineData(float.MaxValue), InlineData(float.Epsilon),
         InlineData(float.PositiveInfinity), InlineData(float.NegativeInfinity), InlineData(float.NaN), InlineData(42)]
        public void CanWriteAndRecoverFloatOneElsewhereNested(float testData)
        {
            using var s = new MemoryStream();
            using var inStream = new Df2Stream(s, StreamMode.Write, true);
            inStream.CommandSend += cmd => Out.WriteLine($"WRITE> {cmd}");
            var group = inStream.GetOrAddGroup("TestGroup");
            group = group.GetOrAddGroup("TestNestedGroup");
            group.AddOrUpdate("TestData", testData);
            var group2 = inStream.GetOrAddGroup("TestGroup2");
            group2 = group2.GetOrAddGroup("TestNestedGroup");
            group2.AddOrUpdate("TestData", testData);
            inStream.Flush();
            inStream.Close();
            Out.WriteLine($"Data: {Encoding.UTF8.GetString(s.ToArray())} ({BitConverter.ToString(s.ToArray())})");
            s.Seek(0, SeekOrigin.Begin);
            using var outStream = new Df2Stream(s, StreamMode.Read);
            outStream.CommandReceive += msg => Out.WriteLine("READ> " + msg);
            outStream.ProcessUntilEnd();
            Assert.Equal(testData, outStream.Values["TestGroup"]["TestNestedGroup"]["TestData"].AsFloat());
            Assert.Equal(testData, outStream["TestGroup2"]["TestNestedGroup"]["TestData"].AsFloat());
        }

        [Theory, InlineData(double.MinValue), InlineData(double.MaxValue), InlineData(double.Epsilon),
         InlineData(double.PositiveInfinity), InlineData(double.NegativeInfinity), InlineData(double.NaN),
         InlineData(42)]
        public void CanWriteAndRecoverDoubleOneElsewhereNested(double testData)
        {
            using var s = new MemoryStream();
            using var inStream = new Df2Stream(s, StreamMode.Write, true);
            inStream.CommandSend += cmd => Out.WriteLine($"WRITE> {cmd}");
            var group = inStream.GetOrAddGroup("TestGroup");
            group = group.GetOrAddGroup("TestNestedGroup");
            group.AddOrUpdate("TestData", testData);
            var group2 = inStream.GetOrAddGroup("TestGroup2");
            group2 = group2.GetOrAddGroup("TestNestedGroup");
            group2.AddOrUpdate("TestData", testData);
            inStream.Flush();
            inStream.Close();
            Out.WriteLine($"Data: {Encoding.UTF8.GetString(s.ToArray())} ({BitConverter.ToString(s.ToArray())})");
            s.Seek(0, SeekOrigin.Begin);
            using var outStream = new Df2Stream(s, StreamMode.Read);
            outStream.CommandReceive += msg => Out.WriteLine("READ> " + msg);
            outStream.ProcessUntilEnd();
            Assert.Equal(testData, outStream.Values["TestGroup"]["TestNestedGroup"]["TestData"].AsDouble());
            Assert.Equal(testData, outStream["TestGroup2"]["TestNestedGroup"]["TestData"].AsDouble());
        }

        [Theory, InlineData("Forty Two"), InlineData("English 日本語 русский язык 官話"), InlineData("🌄"),
         MemberData(nameof(GetBigStrings))]
        public void CanWriteAndRecoverStringOneElsewhereNested(string testData)
        {
            using var s = new MemoryStream();
            using var inStream = new Df2Stream(s, StreamMode.Write, true);
            inStream.CommandSend += msg => Out.WriteLine("WRITE> " + msg);
            var group = inStream.GetOrAddGroup("TestGroup");
            group = group.GetOrAddGroup("TestNestedGroup");
            group.AddOrUpdate("TestData", testData);
            var group2 = inStream.GetOrAddGroup("TestGroup2");
            group2 = group2.GetOrAddGroup("TestNestedGroup");
            group2.AddOrUpdate("TestData", testData);
            inStream.Flush();
            inStream.Close();
            Out.WriteLine($"Data: {Encoding.UTF8.GetString(s.ToArray())} ({BitConverter.ToString(s.ToArray())})");
            s.Seek(0, SeekOrigin.Begin);
            using var outStream = new Df2Stream(s, StreamMode.Read);
            outStream.CommandReceive += msg => Out.WriteLine("READ> " + msg);
            outStream.ProcessUntilEnd();
            Assert.Equal(testData, outStream.Values["TestGroup"]["TestNestedGroup"]["TestData"].AsString());
            Assert.Equal(testData, outStream["TestGroup2"]["TestNestedGroup"]["TestData"].AsString());
        }

        [Theory, InlineData(new byte[] {byte.MinValue, byte.MaxValue, 42})]
        public void CanWriteAndRecoverTwoByteArraysOneNestedElsewhere(byte[] testData)
        {
            using var s = new MemoryStream();
            using var inStream = new Df2Stream(s, StreamMode.Write, true);
            var group = inStream.GetOrAddGroup("TestGroup");
            inStream.CommandSend += msg => Out.WriteLine("WRITE> " + msg);
            group = group.GetOrAddGroup("TestNestedGroup");
            group.AddOrUpdate("TestData", testData);
            var group2 = inStream.GetOrAddGroup("TestGroup2");
            group2 = group2.GetOrAddGroup("TestNestedGroup");
            group2.AddOrUpdate("TestData", testData);
            inStream.Flush();
            inStream.Close();
            Out.WriteLine($"Data: {Encoding.UTF8.GetString(s.ToArray())} ({BitConverter.ToString(s.ToArray())})");
            s.Seek(0, SeekOrigin.Begin);
            using var outStream = new Df2Stream(s, StreamMode.Read);
            outStream.CommandReceive += msg => Out.WriteLine("READ> " + msg);
            outStream.ProcessUntilEnd();
            Assert.Equal(testData, outStream.Values["TestGroup"]["TestNestedGroup"]["TestData"].AsArray<byte>());
            Assert.Equal(testData, outStream["TestGroup2"]["TestNestedGroup"]["TestData"].AsArray<byte>());
        }

        [Theory, InlineData(new sbyte[] {sbyte.MinValue, sbyte.MaxValue, 42})]
        public void CanWriteAndRecoverTwoSByteArraysOneNestedElsewhere(sbyte[] testData)
        {
            using var s = new MemoryStream();
            using var inStream = new Df2Stream(s, StreamMode.Write, true);
            var group = inStream.GetOrAddGroup("TestGroup");
            inStream.CommandSend += msg => Out.WriteLine("WRITE> " + msg);
            group = group.GetOrAddGroup("TestNestedGroup");
            group.AddOrUpdate("TestData", testData);
            var group2 = inStream.GetOrAddGroup("TestGroup2");
            group2 = group2.GetOrAddGroup("TestNestedGroup");
            group2.AddOrUpdate("TestData", testData);
            inStream.Flush();
            inStream.Close();
            Out.WriteLine($"Data: {Encoding.UTF8.GetString(s.ToArray())} ({BitConverter.ToString(s.ToArray())})");
            s.Seek(0, SeekOrigin.Begin);
            using var outStream = new Df2Stream(s, StreamMode.Read);
            outStream.CommandReceive += msg => Out.WriteLine("READ> " + msg);
            outStream.ProcessUntilEnd();
            Assert.Equal(testData, outStream.Values["TestGroup"]["TestNestedGroup"]["TestData"].AsArray<sbyte>());
            Assert.Equal(testData, outStream["TestGroup2"]["TestNestedGroup"]["TestData"].AsArray<sbyte>());
        }

        [Theory, InlineData(new short[] {short.MinValue, short.MaxValue, 42, 0xFE, 0xFF})]
        public void CanWriteAndRecoverTwoShortArraysOneNestedElsewhere(short[] testData)
        {
            using var s = new MemoryStream();
            using var inStream = new Df2Stream(s, StreamMode.Write, true);
            var group = inStream.GetOrAddGroup("TestGroup");
            inStream.CommandSend += msg => Out.WriteLine("WRITE> " + msg);
            group = group.GetOrAddGroup("TestNestedGroup");
            group.AddOrUpdate("TestData", testData);
            var group2 = inStream.GetOrAddGroup("TestGroup2");
            group2 = group2.GetOrAddGroup("TestNestedGroup");
            group2.AddOrUpdate("TestData", testData);
            inStream.Flush();
            inStream.Close();
            Out.WriteLine($"Data: {Encoding.UTF8.GetString(s.ToArray())} ({BitConverter.ToString(s.ToArray())})");
            s.Seek(0, SeekOrigin.Begin);
            using var outStream = new Df2Stream(s, StreamMode.Read);
            outStream.CommandReceive += msg => Out.WriteLine("READ> " + msg);
            outStream.ProcessUntilEnd();
            Assert.Equal(testData, outStream.Values["TestGroup"]["TestNestedGroup"]["TestData"].AsArray<short>());
            Assert.Equal(testData, outStream["TestGroup2"]["TestNestedGroup"]["TestData"].AsArray<short>());
        }

        [Theory, InlineData(new ushort[] {ushort.MinValue, ushort.MaxValue, 42, 0xFE, 0xFF})]
        public void CanWriteAndRecoverTwoUShortArraysOneNestedElsewhere(ushort[] testData)
        {
            using var s = new MemoryStream();
            using var inStream = new Df2Stream(s, StreamMode.Write, true);
            var group = inStream.GetOrAddGroup("TestGroup");
            inStream.CommandSend += msg => Out.WriteLine("WRITE> " + msg);
            group = group.GetOrAddGroup("TestNestedGroup");
            group.AddOrUpdate("TestData", testData);
            var group2 = inStream.GetOrAddGroup("TestGroup2");
            group2 = group2.GetOrAddGroup("TestNestedGroup");
            group2.AddOrUpdate("TestData", testData);
            inStream.Flush();
            inStream.Close();
            Out.WriteLine($"Data: {Encoding.UTF8.GetString(s.ToArray())} ({BitConverter.ToString(s.ToArray())})");
            s.Seek(0, SeekOrigin.Begin);
            using var outStream = new Df2Stream(s, StreamMode.Read);
            outStream.CommandReceive += msg => Out.WriteLine("READ> " + msg);
            outStream.ProcessUntilEnd();
            Assert.Equal(testData, outStream.Values["TestGroup"]["TestNestedGroup"]["TestData"].AsArray<ushort>());
            Assert.Equal(testData, outStream["TestGroup2"]["TestNestedGroup"]["TestData"].AsArray<ushort>());
        }

        [Theory, InlineData(new[] {int.MinValue, int.MaxValue, 42, 0xFE, 0xFF, 0xFFFE, 0xFFFF})]
        public void CanWriteAndRecoverTwoIntArraysOneNestedElsewhere(int[] testData)
        {
            using var s = new MemoryStream();
            using var inStream = new Df2Stream(s, StreamMode.Write, true);
            var group = inStream.GetOrAddGroup("TestGroup");
            inStream.CommandSend += msg => Out.WriteLine("WRITE> " + msg);
            group = group.GetOrAddGroup("TestNestedGroup");
            group.AddOrUpdate("TestData", testData);
            var group2 = inStream.GetOrAddGroup("TestGroup2");
            group2 = group2.GetOrAddGroup("TestNestedGroup");
            group2.AddOrUpdate("TestData", testData);
            inStream.Flush();
            inStream.Close();
            Out.WriteLine($"Data: {Encoding.UTF8.GetString(s.ToArray())} ({BitConverter.ToString(s.ToArray())})");
            s.Seek(0, SeekOrigin.Begin);
            using var outStream = new Df2Stream(s, StreamMode.Read);
            outStream.CommandReceive += msg => Out.WriteLine("READ> " + msg);
            outStream.ProcessUntilEnd();
            Assert.Equal(testData, outStream.Values["TestGroup"]["TestNestedGroup"]["TestData"].AsArray<int>());
            Assert.Equal(testData, outStream["TestGroup2"]["TestNestedGroup"]["TestData"].AsArray<int>());
        }

        [Theory, InlineData(new uint[] {uint.MinValue, uint.MaxValue, 42, 0xFE, 0xFF, 0xFFFE, 0xFFFF})]
        public void CanWriteAndRecoverTwoUIntArraysOneNestedElsewhere(uint[] testData)
        {
            using var s = new MemoryStream();
            using var inStream = new Df2Stream(s, StreamMode.Write, true);
            var group = inStream.GetOrAddGroup("TestGroup");
            inStream.CommandSend += msg => Out.WriteLine("WRITE> " + msg);
            group = group.GetOrAddGroup("TestNestedGroup");
            group.AddOrUpdate("TestData", testData);
            var group2 = inStream.GetOrAddGroup("TestGroup2");
            group2 = group2.GetOrAddGroup("TestNestedGroup");
            group2.AddOrUpdate("TestData", testData);
            inStream.Flush();
            inStream.Close();
            Out.WriteLine($"Data: {Encoding.UTF8.GetString(s.ToArray())} ({BitConverter.ToString(s.ToArray())})");
            s.Seek(0, SeekOrigin.Begin);
            using var outStream = new Df2Stream(s, StreamMode.Read);
            outStream.CommandReceive += msg => Out.WriteLine("READ> " + msg);
            outStream.ProcessUntilEnd();
            Assert.Equal(testData, outStream.Values["TestGroup"]["TestNestedGroup"]["TestData"].AsArray<uint>());
            Assert.Equal(testData, outStream["TestGroup2"]["TestNestedGroup"]["TestData"].AsArray<uint>());
        }

        [Theory,
         InlineData(new[]
         {
             long.MinValue, long.MaxValue, 42, 0xFE, 0xFF, 0xFFFE, 0xFFFF, 0xFFFFFE, 0xFFFFFF, 0xFFFFFFFE, 0xFFFFFFFF
         })]
        public void CanWriteAndRecoverTwoLongArraysOneNestedElsewhere(long[] testData)
        {
            using var s = new MemoryStream();
            using var inStream = new Df2Stream(s, StreamMode.Write, true);
            var group = inStream.GetOrAddGroup("TestGroup");
            inStream.CommandSend += msg => Out.WriteLine("WRITE> " + msg);
            group = group.GetOrAddGroup("TestNestedGroup");
            group.AddOrUpdate("TestData", testData);
            var group2 = inStream.GetOrAddGroup("TestGroup2");
            group2 = group2.GetOrAddGroup("TestNestedGroup");
            group2.AddOrUpdate("TestData", testData);
            inStream.Flush();
            inStream.Close();
            Out.WriteLine($"Data: {Encoding.UTF8.GetString(s.ToArray())} ({BitConverter.ToString(s.ToArray())})");
            s.Seek(0, SeekOrigin.Begin);
            using var outStream = new Df2Stream(s, StreamMode.Read);
            outStream.CommandReceive += msg => Out.WriteLine("READ> " + msg);
            outStream.ProcessUntilEnd();
            Assert.Equal(testData, outStream.Values["TestGroup"]["TestNestedGroup"]["TestData"].AsArray<long>());
            Assert.Equal(testData, outStream["TestGroup2"]["TestNestedGroup"]["TestData"].AsArray<long>());
        }

        [Theory,
         InlineData(new ulong[]
         {
             ulong.MinValue, ulong.MaxValue, 42, 0xFE, 0xFF, 0xFFFE, 0xFFFF, 0xFFFFFE, 0xFFFFFF, 0xFFFFFFFE, 0xFFFFFFFF
         })]
        public void CanWriteAndRecoverTwoULongArraysOneNestedElsewhere(ulong[] testData)
        {
            using var s = new MemoryStream();
            using var inStream = new Df2Stream(s, StreamMode.Write, true);
            var group = inStream.GetOrAddGroup("TestGroup");
            inStream.CommandSend += msg => Out.WriteLine("WRITE> " + msg);
            group = group.GetOrAddGroup("TestNestedGroup");
            group.AddOrUpdate("TestData", testData);
            var group2 = inStream.GetOrAddGroup("TestGroup2");
            group2 = group2.GetOrAddGroup("TestNestedGroup");
            group2.AddOrUpdate("TestData", testData);
            inStream.Flush();
            inStream.Close();
            Out.WriteLine($"Data: {Encoding.UTF8.GetString(s.ToArray())} ({BitConverter.ToString(s.ToArray())})");
            s.Seek(0, SeekOrigin.Begin);
            using var outStream = new Df2Stream(s, StreamMode.Read);
            outStream.CommandReceive += msg => Out.WriteLine("READ> " + msg);
            outStream.ProcessUntilEnd();
            Assert.Equal(testData, outStream.Values["TestGroup"]["TestNestedGroup"]["TestData"].AsArray<ulong>());
            Assert.Equal(testData, outStream["TestGroup2"]["TestNestedGroup"]["TestData"].AsArray<ulong>());
        }

        [Theory,
         InlineData(new[]
         {
             float.MinValue, float.MaxValue, float.Epsilon, float.PositiveInfinity, float.NegativeInfinity, float.NaN,
             42
         })]
        public void CanWriteAndRecoverTwoFloatArraysOneNestedElsewhere(float[] testData)
        {
            using var s = new MemoryStream();
            using var inStream = new Df2Stream(s, StreamMode.Write, true);
            var group = inStream.GetOrAddGroup("TestGroup");
            inStream.CommandSend += msg => Out.WriteLine("WRITE> " + msg);
            group = group.GetOrAddGroup("TestNestedGroup");
            group.AddOrUpdate("TestData", testData);
            var group2 = inStream.GetOrAddGroup("TestGroup2");
            group2 = group2.GetOrAddGroup("TestNestedGroup");
            group2.AddOrUpdate("TestData", testData);
            inStream.Flush();
            inStream.Close();
            Out.WriteLine($"Data: {Encoding.UTF8.GetString(s.ToArray())} ({BitConverter.ToString(s.ToArray())})");
            s.Seek(0, SeekOrigin.Begin);
            using var outStream = new Df2Stream(s, StreamMode.Read);
            outStream.CommandReceive += msg => Out.WriteLine("READ> " + msg);
            outStream.ProcessUntilEnd();
            Assert.Equal(testData, outStream.Values["TestGroup"]["TestNestedGroup"]["TestData"].AsArray<float>());
            Assert.Equal(testData, outStream["TestGroup2"]["TestNestedGroup"]["TestData"].AsArray<float>());
        }

        [Theory,
         InlineData(new[]
         {
             double.MinValue, double.MaxValue, double.Epsilon, double.PositiveInfinity, double.NegativeInfinity,
             double.NaN, 42
         })]
        public void CanWriteAndRecoverTwoDoubleArraysOneNestedElsewhere(double[] testData)
        {
            using var s = new MemoryStream();
            using var inStream = new Df2Stream(s, StreamMode.Write, true);
            var group = inStream.GetOrAddGroup("TestGroup");
            inStream.CommandSend += msg => Out.WriteLine("WRITE> " + msg);
            group = group.GetOrAddGroup("TestNestedGroup");
            group.AddOrUpdate("TestData", testData);
            var group2 = inStream.GetOrAddGroup("TestGroup2");
            group2 = group2.GetOrAddGroup("TestNestedGroup");
            group2.AddOrUpdate("TestData", testData);
            inStream.Flush();
            inStream.Close();
            Out.WriteLine($"Data: {Encoding.UTF8.GetString(s.ToArray())} ({BitConverter.ToString(s.ToArray())})");
            s.Seek(0, SeekOrigin.Begin);
            using var outStream = new Df2Stream(s, StreamMode.Read);
            outStream.CommandReceive += msg => Out.WriteLine("READ> " + msg);
            outStream.ProcessUntilEnd();
            Assert.Equal(testData, outStream.Values["TestGroup"]["TestNestedGroup"]["TestData"].AsArray<double>());
            Assert.Equal(testData, outStream["TestGroup2"]["TestNestedGroup"]["TestData"].AsArray<double>());
        }

        [Theory, InlineData(new object[] {new[] {"Forty Two", "English 日本語 русский язык 官話", "🌄"}})]
        public void CanWriteAndRecoverTwoStringArraysOneNestedElsewhere(string[] testData)
        {
            using var s = new MemoryStream();
            using var inStream = new Df2Stream(s, StreamMode.Write, true);
            var group = inStream.GetOrAddGroup("TestGroup");
            inStream.CommandSend += msg => Out.WriteLine("WRITE> " + msg);
            group = group.GetOrAddGroup("TestNestedGroup");
            group.AddOrUpdate("TestData", testData);
            var group2 = inStream.GetOrAddGroup("TestGroup2");
            group2 = group2.GetOrAddGroup("TestNestedGroup");
            group2.AddOrUpdate("TestData", testData);
            inStream.Flush();
            inStream.Close();
            Out.WriteLine($"Data: {Encoding.UTF8.GetString(s.ToArray())} ({BitConverter.ToString(s.ToArray())})");
            s.Seek(0, SeekOrigin.Begin);
            using var outStream = new Df2Stream(s, StreamMode.Read);
            outStream.CommandReceive += msg => Out.WriteLine("READ> " + msg);
            outStream.ProcessUntilEnd();
            Assert.Equal(testData, outStream.Values["TestGroup"]["TestNestedGroup"]["TestData"].AsArray<string>());
            Assert.Equal(testData, outStream["TestGroup2"]["TestNestedGroup"]["TestData"].AsArray<string>());
        }
    }
}