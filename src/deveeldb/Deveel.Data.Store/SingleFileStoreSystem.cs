// 
//  Copyright 2010-2015 Deveel
// 
//    Licensed under the Apache License, Version 2.0 (the "License");
//    you may not use this file except in compliance with the License.
//    You may obtain a copy of the License at
// 
//        http://www.apache.org/licenses/LICENSE-2.0
// 
//    Unless required by applicable law or agreed to in writing, software
//    distributed under the License is distributed on an "AS IS" BASIS,
//    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//    See the License for the specific language governing permissions and
//    limitations under the License.
//


using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;

using Deveel.Data.Configuration;
using Deveel.Data.Services;

namespace Deveel.Data.Store {
	public sealed class SingleFileStoreSystem : IStoreSystem {
		private IDatabaseContext context;

		private bool disposed;

		private readonly object checkPointLock = new object();

		private IDictionary<int, SingleFileStore> stores;
		private IDictionary<string, int> nameIdMap;
		private IDictionary<int, StoreInfo> storeInfo;

		private int storeId;

		public const string DefaultFileExtension = "db";

		private const int Magic = 0xf09a671;

		public SingleFileStoreSystem(IDatabaseContext context, IConfiguration configuration) {
			if (context == null)
				throw new ArgumentNullException("context");

			this.context = context;

			Configure(configuration);

			OpenOrCreateFile();
		}

		~SingleFileStoreSystem() {
			Dispose(false);
		}

		private void Configure(IConfiguration config) {
			var basePath = config.GetString("database.basePath");
			var fileName = config.GetString("database.fileName");
			var fullPath = config.GetString("database.fullPath");

			if (String.IsNullOrEmpty(basePath)) {
				if (String.IsNullOrEmpty(fullPath)) {
					FileName = fileName;
				} else {
					FileName = fullPath;
				}
			} else if (String.IsNullOrEmpty(fileName)) {
				if (!String.IsNullOrEmpty(basePath)) {
					fileName = String.Format("{0}.{1}", context.DatabaseName(), DefaultFileExtension);
					FileName = FileSystem.CombinePath(basePath, fileName);
				} else if (!String.IsNullOrEmpty(fullPath)) {
					FileName = fullPath;
				}
			} else if (String.IsNullOrEmpty(fullPath)) {
				FileName = FileSystem.CombinePath(basePath, fileName);
			}

			if (String.IsNullOrEmpty(FileName))
				throw new DatabaseConfigurationException("Could not configure the file name of the database.");
		}

		public string FileName { get; set; }

		private string TempFileName {
			get { return String.Format("{0}.tmp", FileName); }
		}

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
				if (stores != null) {
					foreach (var store in stores.Values) {
						if (store != null)
							store.Dispose();
					}

					stores.Clear();
				}
			}

			disposed = true;
			storeInfo = null;
			stores = null;
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

			IFile dataFile;

			if (FileSystem.FileExists(FileName)) {
				dataFile = FileSystem.OpenFile(FileName, ReadOnly);
			} else if (ReadOnly) {
				throw new InvalidOperationException(
					string.Format("The file '{0}' does not exist and the store is configured to be read-only.", FileName));
			} else {
				dataFile = FileSystem.CreateFile(FileName);
				created = true;
			}

			try {
				if (!created) {
					LoadStores(dataFile);
				} else {
					using (var stream = new FileStream(dataFile)) {
						WriteHeaders(stream, 24);

						stream.Flush();
					}
				}
			} finally {
				if (dataFile != null)
					dataFile.Dispose();
			}
		}

		private void LoadStores(IFile dataFile) {
			using (var fileStream = new FileStream(dataFile)) {
				using (var reader = new BinaryReader(fileStream, Encoding.Unicode)) {
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

			using (var dataFile = FileSystem.OpenFile(FileName, true)) {
				using (var stream = new FileStream(dataFile)) {
					stream.Seek(offset, SeekOrigin.Begin);

					var buffer = new byte[size];

					var outputStream = new MemoryStream();

					// TODO: support larger portions...
					stream.Read(buffer, 0, (int) size);

					outputStream.Write(buffer, 0, buffer.Length);

					if (outputStream.Length != size)
						throw new IOException("Corruption when reading the store.");

					return outputStream;
				}
			}
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
					if (FileSystem.FileExists(TempFileName))
						FileSystem.DeleteFile(TempFileName);

					using (var dataFile = FileSystem.CreateFile(TempFileName)) {
						using (var stream = new FileStream(dataFile)) {
							var dataStartOffset = GetDataStartOffset();
							WriteHeaders(stream, dataStartOffset);
							WriteStores(stream);

							stream.Flush();
						}
					}

					if (FileSystem.FileExists(FileName))
						FileSystem.DeleteFile(FileName);

					FileSystem.RenameFile(TempFileName, FileName);
				} catch (IOException) {
					throw;
				} catch (Exception ex) {
					throw new IOException("An error occurred while saving data to database file.", ex);
				}
			}
		}

		private long GetDataStartOffset() {
			long offset = 24;

			foreach (var store in stores.Values) {
				var nameLength = Encoding.Unicode.GetByteCount(store.Name);

				offset += 4 + nameLength + 4 + 8 + 8;
			}

			return offset;
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
			writer.Write(name.ToCharArray());

			writer.Write(id);
			writer.Write(offset);
			writer.Write(size);

			storeInfo[store.Id] = new StoreInfo(name, id, offset, size);

			offset += store.DataLength;

			return offset;
		}

		private void WriteHeaders(Stream stream, long dataStartOffset) {
			var writer = new BinaryWriter(stream, Encoding.Unicode);
			writer.Write(Magic);
			writer.Write(1);
			writer.Write(DateTime.UtcNow.Ticks);

			var storeCount = stores == null ? 0 : stores.Count;
			var topId = FindMaxStoreId();

			writer.Write(storeCount);
			writer.Write(topId);

			long offset = dataStartOffset;

			if (stores != null) {
				storeInfo = new Dictionary<int, StoreInfo>(stores.Count);

				foreach (var store in stores.Values) {
					offset = WriteStoreInfo(writer, offset, store);
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

		[DebuggerDisplay("[{Id}]{Name} ({Offset} - {Size})")]
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
