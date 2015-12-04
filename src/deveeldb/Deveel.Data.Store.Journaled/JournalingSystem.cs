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
using System.IO;
using System.Threading;

namespace Deveel.Data.Store.Journaled {
	public sealed class JournalingSystem : IDisposable {
		private readonly object initLock = new object();

		private List<JournalRegistry> registries;
		private JournalRegistry topRegistry;
		private readonly object topRegistryLock = new object();
		private int registryId = 0;

		private Dictionary<string, ResourceBase> resources;
		private long currentId = -1;

		private JournalingThread journalingThread;

		public JournalingSystem(IFileSystem fileSystem, IStoreDataFactory dataFactory) {
			FileSystem = fileSystem;
			DataFactory = dataFactory;
			registries = new List<JournalRegistry>();
			EnableLogging = true;

			resources = new Dictionary<string, ResourceBase>();
		}

		~JournalingSystem() {
			Dispose(false);
		}

		public IFileSystem FileSystem { get; private set; }

		public IStoreDataFactory DataFactory { get; private set; }

		public bool IsStarted {
			get {
				lock (initLock) {
					return journalingThread != null;
				}
			}
		}

		public string JournalPath { get; set; }

		public int PageSize { get; set; }

		public bool EnableLogging { get; set; }

		public bool ReadOnly { get; set; }

		private JournalRegistry TopRegistry {
			get {
				lock (topRegistryLock) {
					return topRegistry;
				}
			}
		}

		public void Start() {
			if (!EnableLogging)
				return;

			lock (initLock) {
				if (journalingThread != null)
					throw new InvalidOperationException("The system was already started.");

				journalingThread = new JournalingThread(this);

				Recover();

				if (!ReadOnly)
					NewRegistry();
			}
		}

		private void Recover() {
			var registryInfoList = new List<JournalRegistryInfo>();

			for (int i = 10; i < 74; i++) {
				string journalFn = GetRegistryFileName(i);
				string f = Path.Combine(JournalPath, journalFn);

				if (FileSystem.FileExists(f)) {
					if (ReadOnly) {
						throw new IOException(
							"Journal file " + f + " exists for a Read-only session.  " +
							"There may not be any pending journals for a Read-only session.");
					}

					var registry = new JournalRegistry(this, f, ReadOnly);

					// Open the journal registry, getting various
					// information about the registry such as the last 
					// check point and the id of the journal file.
					var info = registry.Open();

					if (info.Recoverable) {
						registryInfoList.Add(info);
					} else {
						registry.Close(true);
					}
				}

				registryInfoList.Sort(new RegistryInfoComparer());

				long lastNumber = -1;

				foreach (var info in registryInfoList) {
					foreach (var resource in info.Resources) {
						CreateResource(resource);
					}

					var registry = info.Registry;
					if (lastNumber > registry.JournalNumber)
						throw new IOException("Sort of the registries failed.");

					lastNumber = registry.JournalNumber;

					registry.Persist(8, info.LastCheckPoint);
					registry.Close(true);

					foreach (var resourceName in info.Resources) {
						var resource =  (ISystemJournaledResource) CreateResource(resourceName);
						resource.PersistClose();
						resource.OnRecovered();
					}
				}
			}
		}

		class RegistryInfoComparer : IComparer<JournalRegistryInfo> {
			public int Compare(JournalRegistryInfo x, JournalRegistryInfo y) {
				return x.Registry.JournalNumber.CompareTo(y.Registry.JournalNumber);
			}
		}

		private string GetRegistryFileName(int offset) {
			if (offset < 10 || offset > 73)
				throw new ArgumentOutOfRangeException();

			return string.Format("jnl{0}", offset);
		}

		public void Stop() {
			if (!EnableLogging)
				return;

			lock (initLock) {
				if (journalingThread == null)
					throw new InvalidOperationException("The system was already stopped.");

				journalingThread.WaitRegistryCount(0);
				journalingThread.Finish();
				journalingThread.Wait();
				journalingThread = null;
			}

			if (ReadOnly)
				return;

			lock (topRegistryLock) {
				foreach (var registry in registries) {
					registry.Close(false);
				}

				TopRegistry.Close(false);
				Recover();
			}
		}

		private void NewRegistry() {
			var fileName = GetRegistryFileName((registryId & 63) + 10);
			registryId++;

			var fullPath = FileSystem.CombinePath(JournalPath, fileName);
			if (FileSystem.FileExists(fullPath))
				throw new InvalidOperationException(string.Format("The registry file '{0}' already exists.", fullPath));

			topRegistry = new JournalRegistry(this, fullPath, ReadOnly);
			topRegistry.Create(registryId - 1);
		}

		public IJournaledResource CreateResource(string resourceName) {
			ResourceBase resource;
			if (!resources.TryGetValue(resourceName, out resource)) {
				long id = currentId++;

				// Create the IStoreDataAccessor for this resource.
				var accessor = DataFactory.CreateData(resourceName);
				if (EnableLogging) {
					resource = new LoggingResource(this, resourceName, id, accessor);
				} else {
					resource = new NonLoggingResource(this, resourceName, id, accessor);
				}

				// Put this input the map.
				resources[resourceName] = resource;
			}

			return resource;
		}

		internal ISystemJournaledResource GetResource(string resourceName) {
			ResourceBase resource;
			if (!resources.TryGetValue(resourceName, out resource))
				return null;

			return resource;
		}

		public void Checkpoint(bool flushRegistries) {
			if (!EnableLogging || ReadOnly)
				return;

			bool persist;

			lock (topRegistryLock) {
				var top = TopRegistry;

				if (flushRegistries || top.File.Length > (256*1024)) {
					NewRegistry();
					registries.Add(top);
				}

				persist = registries.Count > 0;
				top.Checkpoint();
			}

			if (persist)
				journalingThread.WaitRegistryCount(10);
		}

		public void Dispose() {
			Dispose(true);
			GC.SuppressFinalize(this);
		}

		private void Dispose(bool disposing) {
			if (disposing) {
				if (IsStarted)
					Stop();
			}
		}

		#region ResourceBase

		abstract class ResourceBase : JournaledResource, ISystemJournaledResource {
			protected ResourceBase(JournalingSystem system, string name, long id, IStoreData storeData) 
				: base(name, id, storeData) {
				System = system;
			}

			protected JournalingSystem System { get; private set; }

			public override int PageSize {
				get { return System.PageSize; }
			}

			public abstract void PersistClose();

			public abstract void PersistDelete();

			public abstract void PersistSetSize(long newSize);

			public abstract void PersistPageChange(long page, int offset, int length, BinaryReader data);

			public abstract void Synch();

			public abstract void OnRecovered();
		}

		#endregion

		#region NonLoggingResource

		class NonLoggingResource : ResourceBase {
			public NonLoggingResource(JournalingSystem system, string name, long id, IStoreData storeData) 
				: base(system, name, id, storeData) {
			}

			public override bool Exists {
				get { return StoreData.Exists; }
			}

			public override void Read(long pageNumber, byte[] buffer, int offset) {
				long pageOffset = pageNumber * PageSize;
				StoreData.Read(pageOffset + offset, buffer, offset, PageSize);
			}

			public override void Write(long pageNumber, byte[] buffer, int offset, int length) {
				var pageOffset = pageNumber * PageSize;
				StoreData.Write(pageOffset + offset, buffer, offset, length);
			}

			protected override void OpenResource() {
				StoreData.Open(IsReadOnly);
			}

			public override void Close() {
				StoreData.Close();
			}

			public override void Delete() {
				StoreData.Delete();
			}

			public override void PersistClose() {
			}

			public override void PersistDelete() {
			}

			public override void PersistSetSize(long newSize) {
			}

			public override void PersistPageChange(long page, int offset, int length, BinaryReader data) {
			}

			public override void Synch() {
			}

			public override void OnRecovered() {
			}
		}

		#endregion

		#region LoggingResource

		class LoggingResource : ResourceBase {
			private long size;

			private bool backingData;
			private bool isOpen;

			private bool dataExists;
			private bool dataOpen;
			private bool dataDeleted;

			private readonly JournalEntry[] journalEntries;


			public LoggingResource(JournalingSystem system, string name, long id, IStoreData storeData) 
				: base(system, name, id, storeData) {
				journalEntries = new JournalEntry[257];
				dataOpen = false;
				dataExists = storeData.Exists;
				dataDeleted = false;

				if (dataExists) {
					try {
						size = storeData.Length;
					} catch (IOException e) {
						throw new Exception("Error getting size of resource: " + e.Message, e);
					}
				}

				isOpen = false;
			}

			public override bool Exists {
				get { return dataExists; }
			}

			public override long Size {
				get {
					lock (journalEntries) {
						return size;
					}
				}
			}

			public override void Read(long pageNumber, byte[] buffer, int offset) {
				lock (journalEntries) {
					if (!dataOpen) {
						throw new IOException("Data file is not open.");
					}
				}

				// The list of all journal entries on this page number
				var entries = new List<JournalEntry>(4);
				try {
					// The map index.
					lock (journalEntries) {
						int i = ((int)(pageNumber & 0x0FFFFFFF) % journalEntries.Length);
						var entry = journalEntries[i];
						JournalEntry prev = null;

						while (entry != null) {
							bool deletedHash = false;

							var registry = entry.Registry;

							// Note that once we have a reference the journal can not be deleted.
							registry.Reference();

							// If the file is closed (or deleted)
							if (registry.IsDeleted) {
								deletedHash = true;

								registry.Unreference();
		
								if (prev == null) {
									journalEntries[i] = entry.Next;
								} else {
									prev.Next = entry.Next;
								}
							}
							else if (entry.PageNumber == pageNumber) {
								// Else if not closed then is this entry the page number?
								entries.Add(entry);
							} else {
								// Not the page we are looking for so remove the reference to the
								// file.
								registry.Unreference();
							}

							// Only move prev is we have NOT deleted a hash entry
							if (!deletedHash) {
								prev = entry;
							}

							entry = entry.Next;
						}
					}

					// Read any data from the underlying file
					if (backingData) {
						long pageOffset = pageNumber * PageSize;

						// First Read the page from the underlying store.
						StoreData.Read(pageOffset, buffer, offset, PageSize);
					} else {
						// Clear the buffer
						for (int i = offset; i < (PageSize + offset); ++i) {
							buffer[i] = 0;
						}
					}

					// Rebuild from the journal file(s)
					int sz = entries.Count;
					for (int i = 0; i < sz; ++i) {
						var entry = entries[i];
						var registry = entry.Registry;
						long position = entry.Offset;
						lock (registry) {
							registry.BuildPage(pageNumber, position, buffer, offset);
						}
					}
				} finally {
					// Make sure we remove the reference for all the journal files.
					int sz = entries.Count;
					for (int i = 0; i < sz; ++i) {
						var entry = entries[i];
						var registry = entry.Registry;
						registry.Unreference();
					}
				}
			}

			public override void Write(long pageNumber, byte[] buffer, int offset, int length) {
				lock (journalEntries) {
					if (!dataOpen) {
						throw new IOException("Data file is not open.");
					}

					// Make this modification input the log
					JournalEntry journal;
					lock (System.topRegistryLock) {
						journal = System.TopRegistry.ModifyPage(Name, pageNumber, buffer, offset, length);
					}

					// This adds the modification to the END of the hash list.  This means
					// when we reconstruct the page the journals will always be input the
					// correct order - from oldest to newest.

					// The map index.
					int i = ((int)(pageNumber & 0x0FFFFFFF) % journalEntries.Length);
					JournalEntry entry = (JournalEntry)journalEntries[i];
					// Make sure this entry is added to the END
					if (entry == null) {
						// Add at the head if no first entry
						journalEntries[i] = journal;
						journal.Next = null;
					} else {
						// Otherwise search to the end
						// The number of journal entries input the linked list
						int journalEntryCount = 0;
						while (entry.Next != null) {
							entry = entry.Next;
							++journalEntryCount;
						}

						// and add to the end
						entry.Next = journal;
						journal.Next = null;

						// If there are over 35 journal entries, scan and remove all entries
						// on journals that have persisted
						if (journalEntryCount > 35) {
							int entriesCleaned = 0;
							entry = journalEntries[i];
							JournalEntry prev = null;

							while (entry != null) {
								bool deletedHash = false;

								var file = entry.Registry;
								// Note that once we have a reference the journal file can not be
								// deleted.
								file.Reference();

								// If the file is closed (or deleted)
								if (file.IsDeleted) {
									deletedHash = true;
									// Deleted so remove the reference to the journal
									file.Unreference();
									// Remove the journal entry from the chain.
									if (prev == null) {
										journalEntries[i] = entry.Next;
									} else {
										prev.Next = entry.Next;
									}
									++entriesCleaned;
								}

								// Remove the reference
								file.Unreference();

								// Only move prev is we have NOT deleted a hash entry
								if (!deletedHash) {
									prev = entry;
								}
								entry = entry.Next;
							}

						}
					}
				}
			}

			private void PersistOpen() {
				PersistOpen(IsReadOnly);
			}

			private void PersistOpen(bool readOnly) {
				if (!isOpen) {
					StoreData.Open(readOnly);
					backingData = true;
					isOpen = true;
				}
			}

			protected override void OpenResource() {
				if (!dataDeleted && StoreData.Exists) {
					// It does exist so open it.
					PersistOpen();
				} else {
					backingData = false;
					dataDeleted = false;
				}

				dataOpen = true;
				dataExists = true;
			}

			public override void Close() {
				lock (journalEntries) {
					dataOpen = false;
				}
			}

			public override void Delete() {
				lock (System.topRegistryLock) {
					System.TopRegistry.DeleteResource(Name);
				}
				lock (journalEntries) {
					dataExists = false;
					dataDeleted = true;
					size = 0;
				}
			}

			public override void PersistClose() {
				if (isOpen) {
					// When we close we reset the size attribute.  We do this because of
					// the roll forward recovery.
					size = StoreData.Length;
					StoreData.Flush();
					StoreData.Close();
					isOpen = false;
				}
			}

			public override void PersistDelete() {
				if (isOpen) {
					PersistClose();
				}
				StoreData.Delete();
				backingData = false;
			}

			public override void PersistSetSize(long newSize) {
				if (!isOpen) {
					PersistOpen(false);
				}

				// Don't let us set a size that's smaller than the current size.
				if (newSize > StoreData.Length)
					StoreData.SetLength(newSize);
			}

			public override void PersistPageChange(long page, int offset, int length, BinaryReader data) {
				if (!isOpen)
					PersistOpen(false);

				// Buffer to Read the page content into
				byte[] buf;
				if (length <= PageSize) {
					// If length is smaller or equal to the size of a page then use the
					// local page buffer.
					buf = new byte[PageSize];
				} else {
					// Otherwise create a new buffer of the required size (this may happen
					// if the page size changes between sessions).
					buf = new byte[length];
				}

				data.Read(buf, 0, length);

				// TODO: should we use the PageSize here?
				long pos = page * 8192; // PageSize;
				StoreData.Write(pos + offset, buf, 0, length);

			}

			public override void Synch() {
				if (isOpen)
					StoreData.Flush();
			}

			public override void SetSize(long value) {
				lock (journalEntries) {
					this.size = value;
				}
				lock (System.topRegistryLock) {
					System.TopRegistry.ChangeResourceSize(Name, size);
				}
			}

			public override void OnRecovered() {
				dataExists = StoreData.Exists;
			}
		}

		#endregion

		#region JournalingThread

		class JournalingThread : IDisposable {
			private Thread thread;
			private JournalingSystem system;
			private bool finished;
			private bool actuallyFinished;

			public JournalingThread(JournalingSystem system) {
				this.system = system;

				thread = new Thread(Process);
				thread.Name = "Journaling Thread";
				thread.IsBackground = true;
				thread.Start();
			}

			~JournalingThread() {
				Dispose(false);
			}

			private void Process() {
				var localFinished = false;

				while (!localFinished) {
					IEnumerable<JournalRegistry> toProcess = null;
					lock (system.topRegistryLock) {
						if (system.registries.Count > 0) {
							toProcess = new List<JournalRegistry>(system.registries);
						}
					}

					if (toProcess == null) {
						lock (this) {
							if (!finished) {
								try {
									Monitor.Wait(this);
								} catch (ThreadInterruptedException) {
								}
							}
						}
					} else {
						foreach (var registry in toProcess) {
							try {
								registry.Persist(8, registry.File.Length);
								registry.Close(true);
							} catch (IOException) {
								lock (this) {
									finished = true;
								}
							}
						}
					}

					lock (this) {
						localFinished = finished;

						if (toProcess != null) {
							lock (system.topRegistryLock) {
								system.registries.Clear();
							}
						}

						Monitor.PulseAll(this);
					}
				}

				lock (this) {
					actuallyFinished = true;
					Monitor.PulseAll(this);
				}
			}

			public void Finish() {
				lock (this) {
					finished = true;
					Monitor.PulseAll(this);
				}
			}

			public void Wait() {
				lock (this) {
					try {
						while (!actuallyFinished) {
							Monitor.Wait(this);
						}
					} catch (ThreadInterruptedException) {
					}

					Monitor.PulseAll(this);
				}
			}

			public void WaitRegistryCount(int size) {
				lock (this) {
					int sz;
					lock (system.topRegistryLock) {
						sz = system.registries.Count;
					}

					// Wait until the sz is smaller than 'until_size'
					while (sz > size) {
						try {
							Monitor.Wait(this);
						} catch (ThreadInterruptedException e) {
							/* ignore */
						}
						lock (system.topRegistryLock) {
							sz = system.registries.Count;
						}
					}
				}
			}

			private void Dispose(bool disposing) {
				lock (this) {
					if (disposing) {
						if (thread != null) {
							try {
								thread.Join(500);
								thread.Interrupt();
							} catch (ThreadInterruptedException) {
							} catch (Exception) {
								// TODO: log the error...
							} finally {
								finished = true;
							}
						}
					}

					thread = null;
					system = null;
				}
			}

			public void Dispose() {
				Dispose(true);
				GC.SuppressFinalize(this);
			}
		}

		#endregion
	}
}
