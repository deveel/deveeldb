// 
//  V1FileStoreSystem.cs
//  
//  Author:
//       Antonello Provenzano <antonello@deveel.com>
//       Tobias Downer <toby@mckoi.com>
//  
//  Copyright (c) 2009 Deveel
// 
//  This program is free software: you can redistribute it and/or modify
//  it under the terms of the GNU Lesser General Public License as published by
//  the Free Software Foundation, either version 3 of the License, or
//  (at your option) any later version.
// 
//  This program is distributed in the hope that it will be useful,
//  but WITHOUT ANY WARRANTY; without even the implied warranty of
//  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
//  GNU Lesser General Public License for more details.
// 
//  You should have received a copy of the GNU Lesser General Public License
//  along with this program.  If not, see <http://www.gnu.org/licenses/>.

using System;
using System.IO;
using System.Threading;

using Deveel.Data.Store;
using Deveel.Diagnostics;

namespace Deveel.Data {
	/// <summary>
	/// An implementation of <see cref="IStoreSystem"/> that manages persistant 
	/// data through the native file system.
	/// </summary>
	/// <remarks>
	/// Each store is represented by a <see cref="JournalledFileStore"/> object 
	/// against the current path.
	/// </remarks>
	class V1FileStoreSystem : IStoreSystem, IDisposable {
		/// <summary>
		/// The name of the file extention of the file Lock on this conglomerate.
		/// </summary>
		private const String FLOCK_EXT = ".lock";

		/// <summary>
		/// The TransactionSystem that contains the various configuration options 
		/// for the database.
		/// </summary>
		private readonly TransactionSystem system;

		/// <summary>
		/// The path in the filesystem where the data files are located.
		/// </summary>
		private readonly string path;

		/// <summary>
		/// True if the stores are read-only.
		/// </summary>
		private readonly bool read_only;

		/// <summary>
		/// The lock file.
		/// </summary>
		private FileStream lock_file;

		public V1FileStoreSystem(TransactionSystem system, string path, bool read_only) {
			this.system = system;
			this.path = path;
			this.read_only = read_only;
			// If the database path doesn't exist, create it now,
			if (!read_only && !Directory.Exists(path)) {
				Directory.CreateDirectory(path);
			}
		}

		~V1FileStoreSystem() {
			Dispose(false);
		}

		void IDisposable.Dispose() {
			GC.SuppressFinalize(this);
			Dispose(true);
		}

		private void Dispose(bool disposing) {
			if (disposing) {
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
		private JournalledFileStore createFileStore(String file_name) {
			LoggingBufferManager buffer_manager = system.BufferManager;
			return new JournalledFileStore(file_name, buffer_manager, read_only);
		}

		// ---------- Implemented from IStoreSystem ----------

		/// <inheritdoc/>
		public bool StoreExists(String name) {
			try {
				JournalledFileStore store = createFileStore(name);
				return store.Exists;
			} catch (IOException e) {
				Debug.WriteException(e);
				throw new Exception("IO Error: " + e.Message);
			}
		}

		/// <inheritdoc/>
		public IStore CreateStore(String name) {
			LoggingBufferManager buffer_manager = system.BufferManager;
			if (read_only) {
				throw new Exception(
								  "Can not create store because system is Read-only.");
			}
			try {
				buffer_manager.LockForWrite();

				JournalledFileStore store = createFileStore(name);
				if (!store.Exists) {
					store.Open();
					return store;
				} else {
					throw new Exception("Can not create - store with name " + name +
											   " already exists.");
				}
			} catch (IOException e) {
				Debug.WriteException(e);
				throw new Exception("IO Error: " + e.Message);
			} catch (ThreadInterruptedException e) {
				throw new ApplicationException("Interrupted: " + e.Message);
			} finally {
				buffer_manager.UnlockForWrite();
			}

		}

		/// <inheritdoc/>
		public IStore OpenStore(String name) {
			LoggingBufferManager buffer_manager = system.BufferManager;
			try {
				buffer_manager.LockForWrite();

				JournalledFileStore store = createFileStore(name);
				if (store.Exists) {
					store.Open();
					return store;
				} else {
					throw new Exception("Can not open - store with name " + name +
											   " does not exist.");
				}
			} catch (IOException e) {
				Debug.WriteException(e);
				throw new Exception("IO Error: " + e.Message);
			} catch (ThreadInterruptedException e) {
				throw new ApplicationException("Interrupted: " + e.Message);
			} finally {
				buffer_manager.UnlockForWrite();
			}

		}

		/// <inheritdoc/>
		public bool CloseStore(IStore store) {
			LoggingBufferManager buffer_manager = system.BufferManager;
			try {
				buffer_manager.LockForWrite();

				((JournalledFileStore)store).Close();
				return true;
			} catch (IOException e) {
				Debug.WriteException(e);
				throw new Exception("IO Error: " + e.Message);
			} catch (ThreadInterruptedException e) {
				throw new ApplicationException("Interrupted: " + e.Message);
			} finally {
				buffer_manager.UnlockForWrite();
			}

		}

		/// <inheritdoc/>
		public bool DeleteStore(IStore store) {
			LoggingBufferManager buffer_manager = system.BufferManager;
			try {
				buffer_manager.LockForWrite();

				return ((JournalledFileStore)store).Delete();
			} catch (IOException e) {
				Debug.WriteException(e);
				throw new Exception("IO Error: " + e.Message);
			} catch (ThreadInterruptedException e) {
				throw new ApplicationException("Interrupted: " + e.Message);
			} finally {
				buffer_manager.UnlockForWrite();
			}

		}

		/// <inheritdoc/>
		public void SetCheckPoint() {
			try {
				LoggingBufferManager buffer_manager = system.BufferManager;
				buffer_manager.SetCheckPoint(false);
			} catch (IOException e) {
				Debug.WriteException(e);
				throw new Exception("IO Error: " + e.Message);
			} catch (ThreadInterruptedException e) {
				Debug.WriteException(e);
				throw new Exception("Interrupted Error: " + e.Message);
			}
		}

		/// <inheritdoc/>
		public void Lock(String name) {
			string flock_fn = Path.Combine(path, name + FLOCK_EXT);
			if (File.Exists(flock_fn)) {
				// Okay, the file Lock exists.  This means either an extremely bad
				// crash or there is another database locked on the files.  If we can
				// delete the Lock then we can go on.
				Debug.Write(DebugLevel.Warning, this, "File Lock file exists: " + flock_fn);
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