// 
//  Copyright 2010  Deveel
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

using System;
using System.IO;
using System.Threading;

namespace Deveel.Data.Store {
	/// <summary>
	/// An implementation of <see cref="IStoreSystem"/> that manages persistant 
	/// data through the native file system.
	/// </summary>
	/// <remarks>
	/// Each store is represented by a <see cref="JournalledFileStore"/> object 
	/// against the current path.
	/// </remarks>
	class V1FileStoreSystem : IStoreSystem {
		/// <summary>
		/// The name of the file extention of the file Lock on this conglomerate.
		/// </summary>
		private const String FLOCK_EXT = ".lock";

		/// <summary>
		/// The TransactionSystem that contains the various configuration options 
		/// for the database.
		/// </summary>
		private TransactionSystem system;

		/// <summary>
		/// The path in the filesystem where the data files are located.
		/// </summary>
		private string path;

		/// <summary>
		/// True if the stores are read-only.
		/// </summary>
		private bool read_only;

		/// <summary>
		/// The lock file.
		/// </summary>
		private FileStream lock_file;

		/// <summary>
		/// A LoggingBufferManager object used to manage pages of ScatteringFileStore
		/// objects in the file system.  We can configure the maximum pages and page
		/// size via this object, so we have control over how much memory from the
		/// heap is used for buffering.
		/// </summary>
		private LoggingBufferManager buffer_manager;

		~V1FileStoreSystem() {
			Dispose(false);
		}

		void IDisposable.Dispose() {
			GC.SuppressFinalize(this);
			Dispose(true);
		}

		public StorageType StorageType {
			get { return StorageType.File; }
		}

		///<summary>
		/// Returns the LoggingBufferManager object enabling us to create no file
		/// stores in the file system.
		///</summary>
		/// <remarks>
		/// This provides access to the buffer scheme that has been configured.
		/// </remarks>
		public LoggingBufferManager BufferManager {
			get { return buffer_manager; }
		}

		private void Dispose(bool disposing) {
			if (disposing) {
				if (buffer_manager != null) {
					try {
						// Set a check point
						SetCheckPoint();
						// Stop the buffer manager
						buffer_manager.Stop();
					} catch (IOException e) {
						Console.Out.WriteLine("Error stopping buffer manager.");
						Console.Out.Write(e.StackTrace);
					}
				}
				buffer_manager = null;

				if (lock_file != null)
					lock_file.Close();
				lock_file = null;
			}
		}

		/// <summary>
		/// Creates the JournalledFileStore object for this table.
		/// </summary>
		/// <param name="file_name"></param>
		/// <returns></returns>
		private JournalledFileStore CreateFileStore(String file_name) {
			return new JournalledFileStore(file_name, buffer_manager, read_only);
		}

		// ---------- Implemented from IStoreSystem ----------

		/// <inheritdoc/>
		public void Init(TransactionSystem transactionSystem) {
			system = transactionSystem;

			// The path where the database data files are stored.
			string databasePath = transactionSystem.Config.GetStringValue("database_path", "./data");
			// The root path variable
			string rootPathVar = transactionSystem.Config.GetStringValue("root_path", null);

			// Set the absolute database path
			path = transactionSystem.Config.ParseFileString(rootPathVar, databasePath);

			read_only = transactionSystem.ReadOnlyAccess;

			// If the database path doesn't exist, create it now,
			if (!read_only && !Directory.Exists(path)) {
				Directory.CreateDirectory(path);
			}

			// ---- Store system setup ----

			// Get the safety level of the file system where 10 is the most safe
			// and 1 is the least safe.
			int ioSafetyLevel = transactionSystem.Config.GetIntegerValue("io_safety_level", 10);
			if (ioSafetyLevel < 1 || ioSafetyLevel > 10) {
				system.Logger.Message(this, "Invalid io_safety_level value.  Setting to the most safe level.");
				ioSafetyLevel = 10;
			}

			system.Logger.Message(this, "io_safety_level = " + ioSafetyLevel);

			// Logging is disabled when safety level is less or equal to 2
			bool enable_logging = true;
			if (ioSafetyLevel <= 2) {
				system.Logger.Message(this, "Disabling journaling and file sync.");
				enable_logging = false;
			}

			system.Logger.Message(this, "Using stardard IO API for heap buffered file access.");
			int page_size = transactionSystem.Config.GetIntegerValue("buffered_io_page_size", 8192);
			int max_pages = transactionSystem.Config.GetIntegerValue("buffered_io_max_pages", 256);

			// Output this information to the log
			system.Logger.Message(this, "[Buffer Manager] Page Size: " + page_size);
			system.Logger.Message(this, "[Buffer Manager] Max pages: " + max_pages);

			// Journal path is currently always the same as database path.
			string journal_path = path;
			// Max slice size is 1 GB for file scattering class
			const long max_slice_size = 16384*65536;
			// First file extention is 'db'
			const String first_file_ext = "db";

			// Set up the BufferManager
			buffer_manager = new LoggingBufferManager(
				path, journal_path, read_only, max_pages, page_size,
				first_file_ext, max_slice_size, system.Logger, enable_logging);
			// ^ This is a big constructor.  It sets up the logging manager and
			//   sets a resource store data accessor converter to a scattering
			//   implementation with a max slice size of 1 GB

			// Start the buffer manager.
			try {
				buffer_manager.Start();
			} catch(IOException e) {
				system.Logger.Error(this, "Error starting buffer manager");
				system.Logger.Error(this, e);
				throw new ApplicationException("IO Error: " + e.Message, e);
			}
		}

		/// <inheritdoc/>
		public bool StoreExists(String name) {
			try {
				JournalledFileStore store = CreateFileStore(name);
				return store.Exists;
			} catch (IOException e) {
				system.Logger.Error(this, e);
				throw new Exception("IO Error: " + e.Message, e);
			}
		}

		/// <inheritdoc/>
		public IStore CreateStore(String name) {
			if (read_only)
				throw new Exception("Can not create store because system is Read-only.");

			try {
				buffer_manager.LockForWrite();

				JournalledFileStore store = CreateFileStore(name);
				if (!store.Exists) {
					store.Open();
					return store;
				} else {
					throw new Exception("Can not create - store with name " + name +
					                    " already exists.");
				}
			} catch (IOException e) {
				system.Logger.Error(this, e);
				throw new Exception("IO Error: " + e.Message, e);
			} catch (ThreadInterruptedException e) {
				throw new ApplicationException("Interrupted: " + e.Message, e);
			} finally {
				buffer_manager.UnlockForWrite();
			}

		}

		/// <inheritdoc/>
		public IStore OpenStore(String name) {
			try {
				buffer_manager.LockForWrite();

				JournalledFileStore store = CreateFileStore(name);
				if (store.Exists) {
					store.Open();
					return store;
				} else {
					throw new Exception("Can not open - store with name " + name +
					                    " does not exist.");
				}
			} catch (IOException e) {
				system.Logger.Error(this, e);
				throw new Exception("IO Error: " + e.Message, e);
			} catch (ThreadInterruptedException e) {
				throw new ApplicationException("Interrupted: " + e.Message, e);
			} finally {
				buffer_manager.UnlockForWrite();
			}

		}

		/// <inheritdoc/>
		public bool CloseStore(IStore store) {
			try {
				buffer_manager.LockForWrite();

				((JournalledFileStore)store).Close();
				return true;
			} catch (IOException e) {
				system.Logger.Error(this, e);
				throw new Exception("IO Error: " + e.Message, e);
			} catch (ThreadInterruptedException e) {
				throw new ApplicationException("Interrupted: " + e.Message, e);
			} finally {
				buffer_manager.UnlockForWrite();
			}

		}

		/// <inheritdoc/>
		public bool DeleteStore(IStore store) {
			try {
				buffer_manager.LockForWrite();

				return ((JournalledFileStore)store).Delete();
			} catch (IOException e) {
				system.Logger.Error(this, e);
				throw new Exception("IO Error: " + e.Message, e);
			} catch (ThreadInterruptedException e) {
				throw new ApplicationException("Interrupted: " + e.Message, e);
			} finally {
				buffer_manager.UnlockForWrite();
			}

		}

		/// <inheritdoc/>
		public void SetCheckPoint() {
			try {
				buffer_manager.SetCheckPoint(false);
			} catch (IOException e) {
				system.Logger.Error(this, e);
				throw new Exception("IO Error: " + e.Message, e);
			} catch (ThreadInterruptedException e) {
				system.Logger.Error(this, e);
				throw new Exception("Interrupted Error: " + e.Message, e);
			}
		}

		/// <inheritdoc/>
		public void Lock(String name) {
			string flock_fn = Path.Combine(path, name + FLOCK_EXT);
			if (File.Exists(flock_fn)) {
				// Okay, the file Lock exists.  This means either an extremely bad
				// crash or there is another database locked on the files.  If we can
				// delete the Lock then we can go on.
				system.Logger.Warning(this, "File Lock file exists: " + flock_fn);
				File.Delete(flock_fn);
				if (File.Exists(flock_fn)) {
					// If we couldn't delete, then most likely database being used.
					Console.Error.WriteLine("\n" +
					                        "I couldn't delete the file Lock for Database '" + name + "'.\n" +
					                        "This most likely means the database is open and being used by\n" +
					                        "another process.\n" +
					                        "The Lock file is: " + flock_fn + "\n\n");
					throw new IOException("Couldn't delete conglomerate file Lock.");
				}
			}
			//#IFDEF(NO_1.1)
			// Atomically create the file,
			// Set it to delete on normal exit of the JVM.
			//TODO: check this... flock_fn.deleteOnExit();
			//#ENDIF
			// Open up a stream and Lock it in the OS
			lock_file = new FileStream(flock_fn, FileMode.CreateNew, FileAccess.Write, FileShare.None);
		}

		/// <inheritdoc/>
		public void Unlock(String name) {
			// Close and delete the Lock file.
			if (lock_file != null) {
				lock_file.Close();
			}
			// Try and delete it
			string flock_fn = Path.Combine(path, name + FLOCK_EXT);
			File.Delete(flock_fn);
		}
	}
}