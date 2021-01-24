using System.Diagnostics;
using Xunit;
using Xunit.Sdk;

namespace Ultz.DF2.Tests
{
    public class RunInDebugModeNotReleaseMode
    {
#if !DEBUG
        public const string Message =
            "DF2 tests depend on some debug-only APIs in the DF2 library. Please run in Debug mode.";
        [Fact(DisplayName = Message)]
        public static void MustRunInDebugModeNotReleaseMode() => throw new XunitException(Message);
#endif
    }
}