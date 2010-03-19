using System;

using NUnit.Framework;

namespace Deveel.Data {
	[TestFixture]
	[StorageBased(StorageType.Memory)]
	[GenerateDatabase(false)]
	public sealed class CreateDatabaseMemoryTest : CreateDatabaseTest {
		
	}
}