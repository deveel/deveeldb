using System;

using Deveel.Data.Configurations;
using Deveel.Data.Storage;

using Xunit;

namespace Deveel.Data.Sql.Tables {
	public class IndexSetStoreTests : IDisposable {
		private InMemoryStoreSystem storeSystem;
		private InMemoryStore store;

		public IndexSetStoreTests() {
			storeSystem = new InMemoryStoreSystem();
			store = storeSystem.CreateStore("test", new Configuration());
		}


		[Fact]
		public void CreateAndOpenStore() {
			var indexSetStore = new IndexSetStore(store);

			var pointer = indexSetStore.Create();

			Assert.True(pointer >= 0);

			indexSetStore.Open(pointer);
			
			indexSetStore.Dispose();
		}


		[Fact]
		public void GetSnapshotIndex() {
			var indexSetStore = new IndexSetStore(store);

			var pointer = indexSetStore.Create();
			indexSetStore.Open(pointer);

			var indexSet = indexSetStore.GetSnapshotIndex();

			Assert.NotNull(indexSet);
			Assert.Empty(indexSet);

			indexSet.Dispose();

			indexSetStore.Dispose();
		}

		public void Dispose() {
			storeSystem?.Dispose();
			store?.Dispose();
		}
	}
}