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

namespace Deveel.Data.Store.Journaled {
	public sealed class JournalingSystem {
		public JournalingSystem(IFileHandleFactory fileHandleFactory) {
			if (fileHandleFactory == null)
				throw new ArgumentNullException("fileHandleFactory");

			FileHandleFactory = fileHandleFactory;
		}

		public IFileHandleFactory FileHandleFactory { get; private set; }

		public int PageSize { get; set; }

		public IJournaledResource CreateResource(string resourceName) {
			throw new NotImplementedException();
		}

		#region ResourceBase

		abstract class ResourceBase : JournaledResource {
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
				get { throw new NotImplementedException(); }
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

							var file = entry.Registry;

							// Note that once we have a reference the journal can not be deleted.
							file.Reference();

							// If the file is closed (or deleted)
							if (file.IsDeleted) {
								deletedHash = true;

								file.Unreference();
		
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
								file.Unreference();
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
				throw new NotImplementedException();
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
				throw new NotImplementedException();
			}

			public override void Delete() {
				throw new NotImplementedException();
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
		}

		#endregion
	}
}
