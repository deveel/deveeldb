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
using System.Text;

using Deveel.Data.Diagnostics;
using Deveel.Data.Util;

namespace Deveel.Data.Store.Journaled {
	class JournalFile {
		private BinaryWriter dataOut;
		private byte[] buffer;

		private Dictionary<string, long> resourceIdMap;

		private long cur_seq_id;

		private int reference_count;

		public JournalFile(JournaledSystem journaledSystem, IFileSystem fileSystem, string path, bool readOnly) {
			JournaledSystem = journaledSystem;
			FileSystem = fileSystem;
			FilePath = path;
			ReadOnly = readOnly;

			buffer = new byte[36];
			resourceIdMap = new Dictionary<string, long>();
			cur_seq_id = 0;
			reference_count = 1;
		}

		public JournaledSystem JournaledSystem { get; private set; }

		public StreamFile File { get; private set; }

		public string FilePath { get; private set; }

		public IFileSystem FileSystem { get; private set; }

		public bool ReadOnly { get; private set; }

		private bool IsOpen { get; set; }

		public long JournalNumber { get; private set; }

		public long Length {
			get { return File.Length; }
		}

		public bool IsDeleted {
			get {
				lock (this) {
					return File == null;
				}
			}
		}

		public void Reference() {
			lock (this) {
				if (reference_count != 0) {
					++reference_count;
				}
			}
		}

		public void Dereference() {
			CloseAndDelete();
		}

		public void Open(long journalNumber) {
			if (IsOpen)
				throw new IOException(String.Format("Journal file '{0}' is already open.", FilePath));

			if (FileSystem.FileExists(FilePath))
				throw new IOException(String.Format("Journal file '{0}' already exists.", FilePath));

			JournalNumber = journalNumber;
			File = new StreamFile(FileSystem, FilePath, ReadOnly);
#if PCL
			dataOut = new BinaryWriter(File.FileStream, Encoding.Unicode);
#else
			dataOut = new BinaryWriter(new BufferedStream(File.FileStream), Encoding.Unicode);
#endif
			dataOut.Write(journalNumber);
			IsOpen = true;
		}

		internal JournalSummary OpenForRecovery() {
			if (IsOpen)
				throw new IOException(String.Format("Journal file '{0}' is already open.", FilePath));

			if (!FileSystem.FileExists(FilePath))
				throw new IOException(String.Format("Journal file '{0}' does not exists.", FilePath));

			// Open the random access file to this journal
			File = new StreamFile(FileSystem, FilePath, ReadOnly);
			IsOpen = true;

			// Create the summary object (by default, not recoverable).
			var summary = new JournalSummary(this);

			long endPointer = File.Length;

			// If end_pointer < 8 then can't recover this journal
			if (endPointer < 8) {
				return summary;
			}

			// The input stream.
			using (var reader = new BinaryReader(File.FileStream, Encoding.Unicode)) {

				// Set the journal number for this
				JournalNumber = reader.ReadInt64();
				long position = 8;

				var checkpointResList = new List<string>();

				// Start scan
				while (true) {
					// If we can't Read 12 bytes ahead, return the summary
					if (position + 12 > endPointer) {
						return summary;
					}

					long type = reader.ReadInt64();
					int size = reader.ReadInt32();

					position = position + size + 12;

					bool skipBody = true;

					// If checkpoint reached then we are recoverable
					if (type == 100) {
						summary.LastCheckPoint = position;
						summary.CanBeRecovered = true;

						// Add the resources input this check point
						foreach (var checkpoint in checkpointResList) {
							summary.Resources.Add(checkpoint);
						}

						// And clear the temporary list.
						checkpointResList.Clear();
					}

					// If end reached, or type is not understood then return
					else if (position >= endPointer ||
					         type < 1 || type > 7) {
						return summary;
					}

					// If we are resource type, then load the resource
					if (type == 2) {

						// We don't skip body for this type, we Read the content
						skipBody = false;
						long id = reader.ReadInt64();
						int strLen = reader.ReadInt32();
						StringBuilder str = new StringBuilder(strLen);
						for (int i = 0; i < strLen; ++i) {
							str.Append(reader.ReadChar());
						}

						var resourceName = str.ToString();
						checkpointResList.Add(resourceName);

					}

					if (skipBody)
						reader.BaseStream.Seek(size, SeekOrigin.Current);
				}

			}
		}

		private long WriteResourceName(string resourceName, BinaryWriter output) {
			long v;
			lock (resourceIdMap) {
				if (!resourceIdMap.TryGetValue(resourceName, out v)) {
					++cur_seq_id;

					int len = resourceName.Length;
					// byte[] buf = Encoding.Unicode.GetBytes(resource_name);

					// Write the header for this resource
					output.Write(2L);
					output.Write(8 + 4 + (len * 2));
					output.Write(cur_seq_id);
					output.Write(len);
					for (int i = 0; i < len; i++) {
						output.Write(resourceName[i]);
					}

					// Put this id input the cache
					v = cur_seq_id;
					resourceIdMap[resourceName] = v;
				}
			}

			return v;
		}

		public void LogResourceDelete(string resourceName) {
			lock (this) {
				// Build the header,
				long v = WriteResourceName(resourceName, dataOut);

				// Write the header
				long resourceId = v;
				dataOut.Write(6L);
				dataOut.Write(8);
				dataOut.Write(resourceId);
			}
		}

		public void LogResourceSizeChange(string resourceName, long newSize) {
			lock (this) {
				// Build the header,
				long v = WriteResourceName(resourceName, dataOut);

				// Write the header
				long resourceId = v;
				dataOut.Write(3L);
				dataOut.Write(8 + 8);
				dataOut.Write(resourceId);
				dataOut.Write(newSize);
			}
		}

		public void SetCheckPoint() {
			lock (this) {
				dataOut.Write(100L);
				dataOut.Write(0);

				// Flush and synch the journal file
				FlushAndSynch();
			}
		}

		public JournalEntry LogPageModification(string resourceName, long pageNumber, byte[] pageBuffer, int off, int len) {
			long reference;
			lock (this) {
				// Build the header,
				long v = WriteResourceName(resourceName, dataOut);

				//// The absolute position of the page,
				//long absolute_position = pageNumber * JournaledSystem.PageSize;

				// Write the header
				long resourceId = v;
				dataOut.Write(1L);
				dataOut.Write((int)8 + 8 + 4 + 4 + len);
				dataOut.Write(resourceId);
				dataOut.Write(pageNumber);
				dataOut.Write(off);
				//data_out.Write((long)(absolute_position / 8192));
				//data_out.Write((int)(off + (int)(absolute_position & 8191)));
				dataOut.Write(len);

				dataOut.Write(pageBuffer, off, len);

				// Flush the changes so we can work output the pointer.
				dataOut.Flush();
				reference = File.Length - len - 36;
			}

			// Returns a JournalEntry object
			return new JournalEntry(this, resourceName, reference, pageNumber);
		}

		private void FlushAndSynch() {
			lock (this) {
				dataOut.Flush();
				File.Synch();
			}
		}

		public void Close() {
			lock (this) {
				if (!IsOpen) {
					throw new IOException("Journal file is already closed.");
				}

				File.Close();
				File.Dispose();
				File = null;
				IsOpen = false;
			}
		}

		public void CloseAndDelete() {
			lock (this) {
				--reference_count;
				if (reference_count == 0) {
					// Close and delete the journal file.
					Close();

					if (!FileSystem.DeleteFile(FilePath))
						throw new IOException(String.Format("Could not delete the journal file '{0}'.", FilePath));
				}
			}
		}

		private void PersistTag(BinaryReader reader, Dictionary<long, string> idNameMap, List<ResourceBase> resourcesUpdated) {
			// Resource id tag
			long id = reader.ReadInt64();
			int len = reader.ReadInt32();
			StringBuilder buf = new StringBuilder(len);
			for (int i = 0; i < len; ++i) {
				buf.Append(reader.ReadChar());
			}

			string resourceName = buf.ToString();

			// Put this input the map
			idNameMap[id] = resourceName;

			JournaledSystem.Context.OnDebug(String.Format("Jounral Command: Tag {0} = {1}", id, resourceName));

			// Add this to the list of resources we updated.
			resourcesUpdated.Add(JournaledSystem.GetResource(resourceName));
		}

		private void PersistDelete(BinaryReader reader, Dictionary<long, string> idNameMap) {
			// Resource delete
			long id = reader.ReadInt64();
			var resourceName = idNameMap[id];
			var resource = JournaledSystem.GetResource(resourceName);

			JournaledSystem.Context.OnDebug(String.Format("Jounral Command: Delete {0}", resourceName));

			resource.PersistDelete();
		}

		internal void Persist(long start, long end) {
			JournaledSystem.Context.OnDebug(String.Format("Persisting file {0}", FilePath));

			File.FileStream.Seek(start, SeekOrigin.Begin);

			using (BinaryReader reader = new BinaryReader(File.FileStream, Encoding.Unicode)) {
				// The list of resources we updated
				var resourcesUpdated = new List<ResourceBase>();

				// A map from resource id to resource name for this journal.
				var idNameMap = new Dictionary<long, string>();

				bool finished = false;
				long position = start;

				while (!finished) {
					long type = reader.ReadInt64();
					int size = reader.ReadInt32();
					position = position + size + 12;

					if (type == 2) {
						PersistTag(reader, idNameMap, resourcesUpdated);
					} else if (type == 6) {
						PersistDelete(reader, idNameMap);
					} else if (type == 3) {
						PersistSizeChange(reader, idNameMap);
					} else if (type == 1) {
						PersistPageModification(reader, idNameMap);
					} else if (type == 100) {
						// Checkpoint (end)

						JournaledSystem.Context.OnDebug("Jounral Command: Check Point");

						if (position == end) {
							finished = true;
						}
					} else {
						throw new Exception("Unknown tag type: " + type + " position = " + position);
					}

				} // while (!finished)

				// Synch all the resources that we have updated.
				foreach (var resource in resourcesUpdated) {
					JournaledSystem.Context.OnDebug(String.Format("Synch: {0}", resource.Name));

					resource.Synch();
				}
			}
		}

		private void PersistPageModification(BinaryReader reader, Dictionary<long, string> idNameMap) {
			// Page modification
			long id = reader.ReadInt64();
			long page = reader.ReadInt64();
			int off = reader.ReadInt32();
			int len = reader.ReadInt32();

			var resourceName = idNameMap[id];
			var resource = JournaledSystem.GetResource(resourceName);

			JournaledSystem.Context.OnDebug(String.Format(
				"Jounral Command: Page Change {0} page= {1} offset = {2} length = {3}", resourceName, page, off, len));

			resource.PersistPageChange(page, off, len, reader.BaseStream);
		}

		private void PersistSizeChange(BinaryReader reader, Dictionary<long, string> idNameMap) {
			// Resource size change
			long id = reader.ReadInt64();
			long newSize = reader.ReadInt64();
			var resourceName = idNameMap[id];
			var resource = JournaledSystem.GetResource(resourceName);

			JournaledSystem.Context.OnInformation(String.Format("Jounral Command: Set Size {0} = {1}", resourceName, newSize));

			resource.PersistSetSize(newSize);
		}

		public void BuildPage(long buildPageNumber, long position, byte[] pageBuffer, int offset) {
			lock (this) {
				File.Read(position, buffer, 0, 36);

				var type = ByteBuffer.ReadInt8(buffer, 0);
				var resourceId = ByteBuffer.ReadInt8(buffer, 12);
				var pageNumber = ByteBuffer.ReadInt8(buffer, 20);
				var pageOffset = ByteBuffer.ReadInt4(buffer, 28);
				var pageLength = ByteBuffer.ReadInt4(buffer, 32);

				// Some asserts,
				if (type != 1)
					throw new IOException(String.Format("Invalid page type '{0}' at position '{1}'", type, position));

				if (pageNumber != buildPageNumber)
					throw new IOException(String.Format(
						"The page number '{0}' does not match the number of the page to build ('{1}')", pageNumber, buildPageNumber));

				// Read the content.
				File.Read(position + 36, pageBuffer, offset + pageOffset, pageLength);
			}
		}
	}
}
