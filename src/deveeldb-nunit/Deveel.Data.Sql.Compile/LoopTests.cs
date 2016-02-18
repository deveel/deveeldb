using System;

using NUnit.Framework;

namespace Deveel.Data.Sql.Compile {
	[TestFixture]
	public sealed class LoopTests : SqlCompileTestBase {
		[Test]
		public void EmptyLoop() {
			const string sql = "LOOP";
		}
	}
}
