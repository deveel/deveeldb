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

using Deveel.Data.Configuration;

namespace Deveel.Data.Store.Journaled {
	public sealed class JournaledStoreSystem : IStoreSystem {
		private const string LockFileExtension = ".lock";

		private Dictionary<string, IFile> lockFiles; 

		public JournaledStoreSystem(IDatabaseContext context, IConfiguration configuration) {
			this.context = context;

			FileSystem = context.ResolveService<IFileSystem>();

			Configure(configuration);
		}

		private void Configure(IConfiguration configuration) {
			BasePath = configuration.GetString("database.basePath");
			ReadOnly = configuration.GetBoolean("system.readOnly");

			if (!FileSystem.DirectoryExists(BasePath))
				FileSystem.CreateDirectory(BasePath);

			int pageSize = configuration.GetInt32("store.buffered.io.pageSize", 8192);
			int maxPages = configuration.GetInt32("store.buffered.io.maxPages", 256);

			var fileExt = configuration.GetString("store.journaled.fileExtension", "db");

			// Max slice size is 1 GB for file scattering class
			const int maxSliceSize = 16384*65536;

			BufferManager = new BufferManager(context) {
				MaxPages = maxPages,
				PageSize = pageSize,
				JournalPath = BasePath,
				EnableLogging = true,
				DataFactory = new ScatteringFileStoreDataFactory(FileSystem, BasePath, maxSliceSize, fileExt)
			};

			BufferManager.Start();
		}

		~JournaledStoreSystem() {
			Dispose(false);
		}

		private IDatabaseContext context;

		private IFileSystem FileSystem { get; set; }

		private BufferManager BufferManager { get; set; }

		public bool ReadOnly { get; private set; }

		public string BasePath { get; private set; }

		public void Dispose() {
			Dispose(true);
			GC.SuppressFinalize(this);
		}

		private void Dispose(bool disposing) {
			if (disposing) {
				if (lockFiles != null) {
					foreach (var lockFile in lockFiles.Values) {
						try {
							if (lockFile != null)
								lockFile.Dispose();
						} catch (Exception) {
						}
					}
				}

				if (BufferManager != null)
					BufferManager.Dispose();
			}

			lockFiles = null;
			context = null;
		}

		public bool StoreExists(string name) {
			var store = new JournaledStore(BufferManager, name, ReadOnly);
			return store.Exists;
		}

		public JournaledStore CreateStore(string name) {
			BufferManager.Lock();

			try {
				var store = new JournaledStore(BufferManager, name, ReadOnly);
				if (store.Exists)
					throw new InvalidOperationException(String.Format("The store '{0}' already exists.", name));

				store.Open();
				return store;
			} finally {
				BufferManager.Release();
			}
		}

		IStore IStoreSystem.CreateStore(string name) {
			return CreateStore(name);
		}

		IStore IStoreSystem.OpenStore(string name) {
			return OpenStore(name);
		}

		public JournaledStore OpenStore(string name) {
			BufferManager.Lock();

			try {
				var store = new JournaledStore(BufferManager, name, ReadOnly);
				if (!store.Exists)
					throw new InvalidOperationException(String.Format("The store '{0}' does not exist.", name));

				store.Open();
				return store;
			} finally {
				BufferManager.Release();
			}
		}

		bool IStoreSystem.CloseStore(IStore store) {
			var journaledStore = store as JournaledStore;
			if (journaledStore == null)
				throw new ArgumentException("The store is not valid for this system.");

			return CloseStore(journaledStore);
		}

		public bool CloseStore(JournaledStore store) {
			if (store == null)
				throw new ArgumentNullException("store");

			BufferManager.Lock();

			try {
				store.Close();

				return true;
			} finally {
				BufferManager.Release();
			}
		}

		bool IStoreSystem.DeleteStore(IStore store) {
			var journaledStore = store as JournaledStore;
			if (journaledStore == null)
				throw new ArgumentException("The store is not valid for this system.");

			return DeleteStore(journaledStore);
		}

		public bool DeleteStore(JournaledStore store) {
			if (store == null)
				throw new ArgumentNullException("store");

			BufferManager.Lock();

			try {
				store.Delete();
				return true;
			} finally {
				BufferManager.Release();
			}
		}

		public void SetCheckPoint() {
			BufferManager.Checkpoint();
		}

		public void Lock(string lockName) {
			var lockFileName = String.Format("{0}.{1}", lockName, LockFileExtension);
			var lockPath = FileSystem.CombinePath(BasePath, lockFileName);

			// TODO: maybe apply a different logic here: wait for the file not to exist
			if (FileSystem.FileExists(lockPath)) {
				if (!FileSystem.DeleteFile(lockPath)) {
					// TODO: log this condition...
				}

				if (lockFiles != null &&
				    lockFiles.ContainsKey(lockPath))
					lockFiles.Remove(lockPath);
			}

			var lockFile = FileSystem.CreateFile(lockPath);

			if (lockFiles == null)
				lockFiles = new Dictionary<string, IFile>();

			lockFiles[lockPath] = lockFile;
		}

		public void Unlock(string lockName) {
			if (lockFiles == null)
				return;

			var lockFileName = String.Format("{0}.{1}", lockName, LockFileExtension);
			var lockPath = FileSystem.CombinePath(BasePath, lockFileName);

			IFile lockFile;
			if (lockFiles.TryGetValue(lockPath, out lockFile)) {
				if (lockFile.Exists)
					lockFile.Delete();
			}

			if (FileSystem.FileExists(lockPath))
				FileSystem.DeleteFile(lockPath);
		}
	}
}
