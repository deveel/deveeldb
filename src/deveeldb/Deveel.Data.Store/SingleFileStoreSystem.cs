using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;

using Deveel.Data.Configuration;

namespace Deveel.Data.Store {
	public sealed class SingleFileStoreSystem : IStoreSystem, IConfigurable {
		private IDatabaseContext context;

		private IFile lockFile;
		private IFile dataFile;

		private bool disposed;
		private bool configured;

		private readonly object checkPointLock = new object();

		private IDictionary<int, SingleFileStore> stores;
		private IDictionary<string, int> nameIdMap;
		private IDictionary<int, StoreInfo> storeInfo;

		private int storeId;

		public const string DefaultFileExtension = "db";

		private const int Magic = 0xf09a671;

		public SingleFileStoreSystem(IDatabaseContext context) {
			if (context == null)
				throw new ArgumentNullException("context");

			this.context = context;
		}

		~SingleFileStoreSystem() {
			Dispose(false);
		}

		void IConfigurable.Configure(IConfiguration config) {
			var basePath = config.GetString("basePath");
			var fileName = config.GetString("fileName");
			var fullPath = config.GetString("fullPath");

			if (String.IsNullOrEmpty(basePath)) {
				if (String.IsNullOrEmpty(fullPath)) {
					FileName = fileName;
				} else {
					FileName = fullPath;
				}
			} else if (String.IsNullOrEmpty(fileName)) {
				
			} else if (String.IsNullOrEmpty(fullPath)) {
				FileName = FileSystem.CombinePath(basePath, fileName);
			}

			// TODO: configure
			configured = true;
		}

		bool IConfigurable.IsConfigured {
			get { return configured; }
		}

		public string FileName { get; set; }

		private string LockFileName {
			get { return String.Format("{0}.lock", FileName); }
		}

		public bool ReadOnly { get; set; }

		public void Dispose() {
			Dispose(true);
			GC.SuppressFinalize(this);
		}

		private void Dispose(bool disposing) {
			if (disposing) {
				if (!disposed) {
					if (lockFile != null) {
						lockFile.Delete();
						lockFile.Dispose();
					}

					if (dataFile != null) {
						dataFile.Close();
						dataFile.Dispose();
					}
				}

				if (stores != null) {
					foreach (var store in stores.Values) {
						if (store != null)
							store.Dispose();
					}

					stores.Clear();
				}
			}

			disposed = true;
			dataFile = null;
			lockFile = null;

			storeInfo = null;
			stores = null;
		}

		public StorageType StorageType {
			get { return StorageType.File; }
		}

		public bool IsReadOnly { get; set; }

		private IFileSystem FileSystem {
			get { return context.ResolveService<IFileSystem>(); }
		}

		public object CheckPointLock {
			get { return checkPointLock; }
		}

		private void OpenOrCreateFile() {
			bool created = false;

			if (FileSystem.FileExists(FileName)) {
				dataFile = FileSystem.OpenFile(FileName, ReadOnly);
			} else if (ReadOnly) {
				throw new InvalidOperationException(
					string.Format("The file '{0}' does not exist and the store is configured to be read-only.", FileName));
			} else {
				dataFile = FileSystem.CreateFile(FileName);
				created = true;
			}

			if (!created) {
				LoadStores();
			}
		}

		private void LoadStores() {
			using (var fileStream = new FileStream(dataFile)) {
				using (var reader = new BinaryReader(fileStream)) {
					LoadHeaders(reader);
				}
			}
		}

		private void LoadHeaders(BinaryReader reader) {
			var magic = reader.ReadInt32();

			if (magic != Magic)
				throw new IOException("The magic number in the header is invalid.");

			var version = reader.ReadInt32();
			var lastModified = reader.ReadInt64();
			var storeCount = reader.ReadInt32();

			// the maximum number of the stores
			storeId = reader.ReadInt32();

			storeInfo = new Dictionary<int, StoreInfo>(storeCount);

			for (int i = 0; i < storeCount; i++) {
				var strLength = reader.ReadInt32();
				var nameChars = reader.ReadChars(strLength);

				var name = new string(nameChars);
				var id = reader.ReadInt32();
				var offset = reader.ReadInt64();
				var size = reader.ReadInt64();

				storeInfo[id] = new StoreInfo(name, id, offset, size);

				if (nameIdMap == null)
					nameIdMap = new Dictionary<string, int>();

				nameIdMap[name] = id;
			}
		}

		internal Stream LoadStoreData(int id) {
			StoreInfo info;
			if (storeInfo == null ||
				!storeInfo.TryGetValue(id, out info))
				return null;

			var size = info.Size;
			var offset = info.Offset;

			var stream = new FileStream(dataFile);
			stream.Seek(offset, SeekOrigin.Begin);

			var byteCount = 0;
			const int bufferSize = 1024;
			var buffer = new byte[bufferSize];

			var outputStream = new MemoryStream();
			while (byteCount < size) {
				var readCount = stream.Read(buffer, 0, bufferSize);
				if (readCount == 0)
					break;

				outputStream.Write(buffer, 0, readCount);
				byteCount += readCount;
			}

			if (outputStream.Length != size)
				throw new IOException("Corruption when reading the store.");

			return outputStream;
		}

		public bool StoreExists(string name) {
			SingleFileStore store;
			return TryFindStore(name, out store);
		}

		IStore IStoreSystem.CreateStore(string name) {
			return CreateStore(name);
		}

		public SingleFileStore CreateStore(string name) {
			lock (checkPointLock) {
				SingleFileStore store;
				if (TryFindStore(name, out store))
					throw new IOException(string.Format("The store '{0}' already exists in this database.", name));

				if (nameIdMap == null)
					nameIdMap = new Dictionary<string, int>();
				
				if (stores == null)
					stores = new Dictionary<int, SingleFileStore>();	

				var id = ++storeId;
				store = new SingleFileStore(this, name, id);
				store.Open();
				stores[id] = store;
				nameIdMap[name] = id;
				return store;
			}
		}

		IStore IStoreSystem.OpenStore(string name) {
			return OpenStore(name);
		}

		public SingleFileStore OpenStore(string name) {
			lock (checkPointLock) {
				SingleFileStore store;
				if (!TryFindStore(name, out store))
					throw new IOException(string.Format("The store '{0}' does not exist in this database.", name));

				store.Open();
				return store;
			}
		}

		bool IStoreSystem.CloseStore(IStore store) {
			return CloseStore((SingleFileStore) store);
		}

		public bool CloseStore(SingleFileStore store) {
			try {
				SingleFileStore fileStore;
				if (!TryFindStore(store.Name, out fileStore))
					throw new IOException("The store was not found in this database.");

				fileStore.Close();
				return true;
			} catch (IOException) {
				throw;
			} catch (Exception ex) {
				throw new IOException("Unable to close the store.", ex);
			}
		}

		bool IStoreSystem.DeleteStore(IStore store) {
			return DeleteStore((SingleFileStore) store);
		}

		public bool DeleteStore(SingleFileStore store) {
			try {
				if (stores == null || !stores.ContainsKey(store.Id))
					return false;

				return stores.Remove(store.Id);
			} catch (IOException) {
				throw;
			} catch (Exception ex) {
				throw new IOException("Unable to delete the store.", ex);
			} finally {
				if (nameIdMap != null)
					nameIdMap.Remove(store.Name);
			}
		}

		public void SetCheckPoint() {
			lock (checkPointLock) {
				try {
					if (lockFile != null && lockFile.Exists)
						throw new IOException("A check-point operation is already happening.");

					lockFile = FileSystem.CreateFile(LockFileName);

					if (dataFile != null &&
						dataFile.Exists)
						dataFile.Delete();

					dataFile = FileSystem.CreateFile(FileName);

					using (var stream = new FileStream(dataFile)) {
						WriteHeaders(stream);
						WriteStores(stream);

						stream.Flush();
					}
				} catch (IOException) {
					throw;
				} catch (Exception ex) {
					throw new IOException("An error occurred while saving data to database file.", ex);
				} finally {
					if (lockFile != null &&
						lockFile.Exists)
						lockFile.Delete();

					lockFile = null;
				}
			}
		}

		private void WriteStores(Stream stream) {
			if (stores != null) {
				foreach (var store in stores.Values) {
					store.WriteTo(stream);
				}
			}
		}

		private long WriteStoreInfo(BinaryWriter writer, long offset, SingleFileStore store) {
			var nameLength = store.Name.Length;
			var name = store.Name;
			var id = store.Id;
			var size = store.DataLength;

			writer.Write(nameLength);
			for (int i = 0; i < nameLength; i++) {
				writer.Write(name[i]);
			}

			writer.Write(id);
			writer.Write(offset);
			writer.Write(size);

			storeInfo[store.Id] = new StoreInfo(name, id, offset, size);

			offset += store.DataLength;

			return offset;
		}

		private void WriteHeaders(Stream stream) {
			using (var writer = new BinaryWriter(stream, Encoding.Unicode)) {
				writer.Write(Magic);
				writer.Write(1);
				writer.Write(DateTime.UtcNow.Ticks);

				var storeCount = stores == null ? 0 : stores.Count;
				var topId = FindMaxStoreId();

				writer.Write(storeCount);
				writer.Write(topId);

				long offset = 24;

				if (stores != null) {
					storeInfo = new Dictionary<int, StoreInfo>(stores.Count);

					foreach (var store in stores.Values) {
						offset = WriteStoreInfo(writer, offset, store);
					}
				}
			}
		}

		private int FindMaxStoreId() {
			return stores == null ? 0 : stores.Max(x => x.Value.Id);
		}

		private bool TryFindStore(string storeName, out SingleFileStore store) {
			int id;
			if (nameIdMap == null || !nameIdMap.TryGetValue(storeName, out id)) {
				store = null;
				return false;
			}

			if (stores == null || !stores.TryGetValue(id, out store)) {
				store = null;
				return false;
			}

			return true;
		}

		public void Lock(string lockName) {
			SingleFileStore store;
			if (!TryFindStore(lockName, out store))
				return;

			store.Lock();
		}

		public void Unlock(string lockName) {
			SingleFileStore store;
			if (!TryFindStore(lockName, out store))
				return;

			store.Unlock();
		}

		#region StoreInfo

		class StoreInfo {
			public StoreInfo(string name, int id, long offset, long size) {
				Name = name;
				Id = id;
				Offset = offset;
				Size = size;
			}

			public string Name { get; private set; }

			public int Id { get; private set; }

			public long Offset { get; private set; }

			public long Size { get; private set; }
		}

		#endregion
	}
}
