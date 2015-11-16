using System;

using NUnit.Framework;

namespace Deveel.Data.Store {
	[TestFixture]
	public class SingleFileStoreTests : ContextBasedTest {
		protected override IDatabaseContext CreateDatabaseContext(ISystemContext context) {
			return base.CreateDatabaseContext(context);
		}
	}
}
