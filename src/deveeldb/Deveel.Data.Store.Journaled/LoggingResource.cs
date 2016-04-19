// 
//  Copyright 2010-2016 Deveel
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
using System.Net;
using System.Text;

namespace Deveel.Data.Store.Journaled {
	class LoggingResource : ResourceBase {
		private long size;
		private bool thereIsBackingData;

		private bool reallyOpen;
		private bool dataExists;
		private bool dataOpen;

		private bool dataDeleted;

		private readonly JournalEntry[] journalMap;

		private readonly byte[] pageBuffer;

		public LoggingResource(JournaledSystem journaledSystem, long id, string name, IStoreData data) 
			: base(journaledSystem, id, name, data) {
			journalMap = new JournalEntry[257];
			dataOpen = false;
			dataExists = data.Exists;
			dataDeleted = false;

			if (dataExists) {
				try {
					size = data.Length;
				} catch (IOException e) {
					throw new Exception("Error getting size of resource: " + e.Message);
				}
			}

			reallyOpen = false;
			pageBuffer = new byte[journaledSystem.PageSize];
		}

		public override long Size {
			get {
				lock (journalMap) {
					return size;
				}
			}
		}

		public override bool Exists {
			get { return dataExists; }
		}

		public override void Read(long pageNumber, byte[] buffer, int offset) {
			lock (journalMap) {
				if (!dataOpen) {
					throw new IOException("Assertion failed: Data file is not open.");
				}
			}

			// The list of all journal entries on this page number
			var allJournalEntries = new List<JournalEntry>(4);

			try {
				// The map index.
				lock (journalMap) {
					int i = ((int)(pageNumber & 0x0FFFFFFF) % journalMap.Length);
					var entry = journalMap[i];
					JournalEntry prev = null;

					while (entry != null) {
						bool deletedHash = false;

						JournalFile file = entry.File;
						// Note that once we have a reference the journal file can not be
						// deleted.
						file.Reference();

						// If the file is closed (or deleted)
						if (file.IsDeleted) {
							deletedHash = true;
							// Deleted so remove the reference to the journal
							file.Dereference();
							// Remove the journal entry from the chain.
							if (prev == null) {
								journalMap[i] = entry.Next;
							} else {
								prev.Next = entry.Next;
							}
						}

						// Else if not closed then is this entry the page number?
						else if (entry.PageNumber == pageNumber) {
							allJournalEntries.Add(entry);
						} else {
							// Not the page we are looking for so remove the reference to the
							// file.
							file.Dereference();
						}

						// Only move prev is we have NOT deleted a hash entry
						if (!deletedHash) {
							prev = entry;
						}

						entry = entry.Next;
					}
				}

				// Read any data from the underlying file
				if (thereIsBackingData) {
					long pagePosition = pageNumber * JournaledSystem.PageSize;
					// First Read the page from the underlying store.
					Data.Read(pagePosition, buffer, offset, JournaledSystem.PageSize);
				} else {
					// Clear the buffer
					for (int i = offset; i < (JournaledSystem.PageSize + offset); ++i) {
						buffer[i] = 0;
					}
				}

				// Rebuild from the journal file(s)
				int sz = allJournalEntries.Count;
				for (int i = 0; i < sz; ++i) {
					var entry = allJournalEntries[i];
					var file = entry.File;
					long position = entry.Position;
					lock (file) {
						file.BuildPage(pageNumber, position, buffer, offset);
					}
				}
			} finally {
				// Make sure we remove the reference for all the journal files.
				int sz = allJournalEntries.Count;
				for (int i = 0; i < sz; ++i) {
					var entry = allJournalEntries[i];
					JournalFile file = entry.File;
					file.Dereference();
				}

			}
		}

		public override void Write(long pageNumber, byte[] buffer, int offset, int count) {
			lock (journalMap) {
				if (!dataOpen) {
					throw new IOException("Assertion failed: Data file is not open.");
				}

				// Make this modification input the log
				var journal = JournaledSystem.LogPageModification(Name, pageNumber, buffer, offset, count);

				// This adds the modification to the END of the hash list.  This means
				// when we reconstruct the page the journals will always be input the
				// correct order - from oldest to newest.

				// The map index.
				int i = ((int)(pageNumber & 0x0FFFFFFF) % journalMap.Length);
				var entry = journalMap[i];

				// Make sure this entry is added to the END
				if (entry == null) {
					// Add at the head if no first entry
					journalMap[i] = journal;
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
						entry = journalMap[i];
						JournalEntry prev = null;

						while (entry != null) {
							bool deletedHash = false;

							JournalFile file = entry.File;
							// Note that once we have a reference the journal file can not be
							// deleted.
							file.Reference();

							// If the file is closed (or deleted)
							if (file.IsDeleted) {
								deletedHash = true;

								// Deleted so remove the reference to the journal
								file.Dereference();

								// Remove the journal entry from the chain.
								if (prev == null) {
									journalMap[i] = entry.Next;
								} else {
									prev.Next = entry.Next;
								}
							}

							// Remove the reference
							file.Dereference();

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

		public override void SetSize(long value) {
			lock (journalMap) {
				size = value;
			}

			JournaledSystem.LogResourceSizeChange(Name, size);
		}

		public override void Open(bool readOnly) {
			SetReadOnly(readOnly);

			if (!dataDeleted && Data.Exists) {
				// It does exist so open it.
				PersistOpen(readOnly);
			} else {
				thereIsBackingData = false;
				dataDeleted = false;
			}

			dataOpen = true;
			dataExists = true;
		}

		public override void Close() {
			lock (journalMap) {
				dataOpen = false;
			}
		}

		public override void Delete() {
			// Log that this resource was deleted.
			JournaledSystem.LogResourceDelete(Name);

			lock (journalMap) {
				dataExists = false;
				dataDeleted = true;
				size = 0;
			}
		}

		protected override void Persist(PersistCommand command) {
			if (command is PersistOpenCommand) {
				OpenCommand((PersistOpenCommand) command);
			} else if (command is PersistSetSizeCommand) {
				SetSizeCommand((PersistSetSizeCommand) command);
			} else if (command is PersistPageChangeCommand) {
				PageChangeCommand((PersistPageChangeCommand) command);
			} else if (command.CommandType == PersistCommandType.Close) {
				CloseCommand();
			} else if (command.CommandType == PersistCommandType.Delete) {
				DeleteCommand();
			} else if (command.CommandType == PersistCommandType.Synch) {
				SynchCommand();
			} else if (command.CommandType == PersistCommandType.PostRecover) {
			}
		}

		private void OpenCommand(PersistOpenCommand command) {
			if (!reallyOpen) {
				Data.Open(command.ReadOnly);
				thereIsBackingData = true;
				reallyOpen = true;
			}
		}

		private void CloseCommand() {
			if (reallyOpen) {
				// When we close we reset the size attribute.  We do this because of
				// the roll forward recovery.
				size = Data.Length;
				Data.Flush();
				Data.Close();
				reallyOpen = false;
			}
		}

		private void DeleteCommand() {
			if (reallyOpen) {
				PersistClose();
			}
			Data.Delete();
			thereIsBackingData = false;
		}

		private void SetSizeCommand(PersistSetSizeCommand command) {
			if (!reallyOpen) {
				PersistOpen(false);
			}

			if (command.NewSize > Data.Length) {
				Data.SetLength(command.NewSize);
			}
		}

		private void PageChangeCommand(PersistPageChangeCommand command) {
			if (!reallyOpen) {
				PersistOpen(false);
			}

			// Buffer to Read the page content into
			byte[] buf;
			if (command.Count <= pageBuffer.Length) {
				// If length is smaller or equal to the size of a page then use the
				// local page buffer.
				buf = pageBuffer;
			} else {
				// Otherwise create a new buffer of the required size (this may happen
				// if the page size changes between sessions).
				buf = new byte[command.Count];
			}

			// Read the change from the input stream
			var reader = new BinaryReader(command.Source, Encoding.Unicode);
			reader.Read(buf, 0, command.Count);

			// Write the change output to the underlying resource container
			long pos = command.PageNumber * 8192; //pageSize;
			Data.Write(pos + command.Offset, buf, 0, command.Count);
		}

		private void SynchCommand() {
			if (reallyOpen) {
				Data.Flush();
			}
		}

		private void PostRecoverCommand() {
			dataExists = Data.Exists;
		}
	}
}
