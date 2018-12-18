using System;
using System.IO;

using Deveel.Data.Configurations;

using Xunit;

namespace Deveel.Data.Storage {
	public class LargeObjectStoreTests : IDisposable {
		private InMemoryStore store;
		private InMemoryStoreSystem system;

		public LargeObjectStoreTests() {
			system = new InMemoryStoreSystem();
			store = system.CreateStore("lob_store", new Configuration());
		}

		[Fact]
		public void CreateNewStore() {
			var lobStore = new LargeObjectStore(1, store);
			var pointer = lobStore.Create();

			Assert.True(pointer >= 0);
			Assert.Equal(1, lobStore.Id);

			lobStore.Dispose();
		}

		[Fact]
		public void CreateNewUncompressedObject() {
			var lobStore = new LargeObjectStore(1, store);
			lobStore.Create();

			var obj = lobStore.CreateObject(1024, false);

			Assert.NotNull(obj);
			Assert.Equal(1024, obj.RawSize);
			Assert.False(obj.IsCompressed);
			Assert.False(obj.IsComplete);

			obj.Dispose();
			lobStore.Dispose();
		}

		[Fact]
		public void CreateNewCompressedObject() {
			var lobStore = new LargeObjectStore(1, store);
			lobStore.Create();

			var obj = lobStore.CreateObject(1024, true);

			Assert.NotNull(obj);
			Assert.Equal(1024, obj.RawSize);
			Assert.True(obj.IsCompressed);
			Assert.False(obj.IsComplete);

			obj.Dispose();
			lobStore.Dispose();
		}

		[Fact]
		public void CreateAndEstablishObject() {
			var lobStore = new LargeObjectStore(1, store);
			lobStore.Create();

			var obj = lobStore.CreateObject(1024, true);

			Assert.NotNull(obj);
			Assert.Equal(1024, obj.RawSize);
			Assert.True(obj.IsCompressed);
			Assert.False(obj.IsComplete);

			obj.Complete();
			obj.Establish();

			obj.Dispose();
			lobStore.Dispose();
		}

		[Fact]
		public void CreateAnWriteObject() {
			var lobStore = new LargeObjectStore(1, store);
			lobStore.Create();

			var obj = lobStore.CreateObject(1024, true);

			var stream = new ObjectStream(obj);
			var writer = new BinaryWriter(stream);

			writer.Write("test1");
			writer.Flush();
			stream.Dispose();

			obj.Complete();

			obj = lobStore.GetObject(obj.Id);

			Assert.NotNull(obj);
			Assert.True(obj.IsComplete);

			stream = new ObjectStream(obj);
			var reader = new BinaryReader(stream);

			var s = reader.ReadString();

			Assert.Equal("test1", s);

			stream.Dispose();
			
			obj.Dispose();
			lobStore.Dispose();
		}

		[Fact]
		public void CreateAndReleaseObject() {
			var lobStore = new LargeObjectStore(1, store);
			lobStore.Create();

			var obj = lobStore.CreateObject(1024, true);

			Assert.NotNull(obj);
			Assert.Equal(1024, obj.RawSize);
			Assert.True(obj.IsCompressed);
			Assert.False(obj.IsComplete);

			obj.Complete();
			obj.Establish();
			Assert.True(obj.Release());

			obj.Dispose();
			lobStore.Dispose();
		}

		public void Dispose() {
			store?.Dispose();
			system?.Dispose();
		}
	}
}