using System;
using System.IO;

using Deveel.Data.Configuration;
using Deveel.Data.Diagnostics;

namespace Deveel.Data.Store.Journaled {
	public sealed class JournaledStoreSystem : IStoreSystem {
		private IContext context;
		private LoggingBufferManager bufferManager;
		private IFile lockFile;

		private const int DefaultPageSize = 2048;
		private const int DefaultMaxPages = 128;

		private const string FileLockExtension = ".lock";

		public JournaledStoreSystem(IDatabaseContext context) {
			this.context = context;
			Configure();
		}

		~JournaledStoreSystem() {
			Dispose(false);
		}

		private IFileSystem FileSystem { get; set; }

		private bool ReadOnly { get; set; }

		private static string GetFileLockName(string resourceName) {
			return String.Format("{0}{1}", resourceName, FileLockExtension);
		}

		private void Configure() {
			var configuration = context.ResolveService<IConfiguration>();

			if (configuration == null)
				throw new InvalidOperationException("Could not find any configuration in context");

			var dataFactoryName = configuration.GetString("store.journaled.dataFactory", "scattering");

			var dataFactory = context.ResolveService<IStoreDataFactory>(dataFactoryName);
			if (dataFactory == null)
				throw new ArgumentException(String.Format("The data factory '{0}' could not be resolved in this context.", dataFactoryName));

			var maxPages = configuration.GetInt32("store.journaled.maxPages", DefaultMaxPages);
			var pageSize = configuration.GetInt32("store.journaled.pageSize", DefaultPageSize);

			var fsName = configuration.GetString("store.journaled.fileSystem");
			if (String.IsNullOrEmpty(fsName))
				fsName = configuration.GetString("store.fileSystem", "local");

			FileSystem = context.ResolveService<IFileSystem>(fsName);

			if (FileSystem == null)
				throw new DatabaseConfigurationException(String.Format("The file-system '{0}' is not found in this context.", fsName));

			var journalPath = configuration.GetString("store.journaled.journalPath");
			if (String.IsNullOrEmpty(journalPath)) {
				var dbName = configuration.GetString("database.name");
				var dbPath = configuration.GetString("database.path");

				if (String.IsNullOrEmpty(dbPath))
					throw new DatabaseConfigurationException("Unable to determine the journal path");

				journalPath = FileSystem.CombinePath(dbPath, "journals");
			}

			if (!FileSystem.DirectoryExists(journalPath))
				FileSystem.CreateDirectory(journalPath);

			ReadOnly = configuration.GetBoolean("database.readOnly", false);

			bufferManager = new LoggingBufferManager(context, FileSystem, journalPath, dataFactory, pageSize, maxPages, ReadOnly);
			bufferManager.Start();
		}

		private JournaledFileStore CreateFileStore(string resourceName) {
			return new JournaledFileStore(resourceName, bufferManager, ReadOnly);
		}

		public void Dispose() {
			Dispose(true);
			GC.SuppressFinalize(this);
		}

		private void Dispose(bool disposing) {
			if (disposing) {
				if (bufferManager != null) {
					try {
						// Set a check point
						SetCheckPoint();

						// Stop the buffer manager
						bufferManager.Stop();
						bufferManager.Dispose();
					} catch (IOException e) {
						// TODO: log the issue
					}
				}

				if (lockFile != null)
					lockFile.Close();
			}

			bufferManager = null;
			lockFile = null;
			context = null;
		}

		public bool StoreExists(string name) {
			var store = CreateFileStore(name);
			return store.Exists();
		}

		public IStore CreateStore(string name) {
			if (ReadOnly)
				throw new IOException("The system is read-only and cannot create storage.");

			try {
				bufferManager.Lock();

				var store = CreateFileStore(name);
				if (store.Exists())
					throw new IOException(String.Format("The store '{0}' already exists.", name));

				store.Open();
				return store;
			} finally {
				bufferManager.Unlock();
			}
		}

		public IStore OpenStore(string name) {
			try {
				bufferManager.Lock();

				var store = CreateFileStore(name);
				if (!store.Exists())
					throw new IOException(String.Format("Store '{0}' does not exist.", name));

				store.Open();
				return store;
			} finally {
				bufferManager.Unlock();
			}
		}

		public bool CloseStore(IStore store) {
			try {
				bufferManager.Lock();

				((JournaledFileStore)store).Close();
				return true;
			} finally {
				bufferManager.Unlock();
			}
		}

		public bool DeleteStore(IStore store) {
			try {
				bufferManager.Lock();

				((JournaledFileStore)store).Delete();
				return true;
			} finally {
				bufferManager.Unlock();
			}
		}

		public void SetCheckPoint() {
			bufferManager.SetCheckPoint(false);
		}

		public void Lock(string lockName) {
			var lockFileName = GetFileLockName(lockName);

			if (FileSystem.FileExists(lockFileName)) {
				context.OnWarning(String.Format("The lock file '{0}' already exists.", lockFileName));

				if (!FileSystem.DeleteFile(lockFileName)) {
					throw new IOException(String.Format("Could not delete the lock file '{0}' from the file-system.", lockFileName));
				}
			}

			// TODO: Specify more options in the IFileSystem?
			lockFile = FileSystem.CreateFile(lockFileName);
		}

		public void Unlock(string lockName) {
			if (lockFile != null)
				lockFile.Close();

			var lockFileName = GetFileLockName(lockName);
			if (!FileSystem.DeleteFile(lockFileName))
				throw new IOException(String.Format("Could not delete the lock file '{0}' from the file-system.", lockFileName));
		}
	}
}
