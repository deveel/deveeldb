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
using System.Text;

using Deveel.Data.Util;

namespace Deveel.Data.Store.Journaled {
	class JournalRegistry : IDisposable {
		private BinaryWriter writer;
		private int refCount;

		private Dictionary<string, long> resourceIdCache;
		private int curSeqId;

		public JournalRegistry(JournalingSystem system, string fileName, bool isReadOnly) {
			if (system == null)
				throw new ArgumentNullException("system");
			if (String.IsNullOrEmpty(fileName))
				throw new ArgumentNullException("fileName");

			System = system;
			FileName = fileName;
			IsReadOnly = isReadOnly;

			resourceIdCache = new Dictionary<string, long>(StringComparer.Ordinal);
			curSeqId = 0;
		}

		~JournalRegistry() {
			Dispose(false);
		}

		public JournalingSystem System { get; private set; }

		public string FileName { get; private set; }

		public bool IsReadOnly { get; private set; }

		public JournalFile File { get; private set; }

		public bool IsOpen { get; private set; }

		public long JournalNumber { get; private set; }

		public bool IsDeleted {
			get {
				lock (this) {
					return File == null;
				}
			}
		}

		private long WriteResourceName(string resourceName, BinaryWriter output) {
			lock (resourceIdCache) {
				long id;

				if (!resourceIdCache.TryGetValue(resourceName, out id)) {
					id = ++curSeqId;

					byte[] buf = Encoding.Unicode.GetBytes(resourceName);
					int len = buf.Length;

					// Write the header for this resource
					output.Write((byte)JournalFileCommand.TagResource);
					output.Write(8 + 4 + len);
					output.Write(id);
					output.Write(len);
					output.Write(buf);

					// Put this id input the cache
					resourceIdCache[resourceName] = id;
				}

				return id;
			}
		}

		public void Create(long journalNumber) {
			if (IsOpen)
				throw new IOException(String.Format("Journal file '{0}' is already open.", FileName));

			if (System.FileSystem.FileExists(FileName))
				throw new IOException(String.Format("Journal file '{0}' already exists.", FileName));

			JournalNumber = journalNumber;
			File = new JournalFile(System, FileName, IsReadOnly);

			writer = new BinaryWriter(File.FileStream, Encoding.Unicode);
			writer.Write(JournalNumber);
			IsOpen = true;
		}

		private void CloseFile(bool delete) {
			lock (this) {
				if (!IsOpen)
					throw new IOException(String.Format("Journal file '{0}' is already closed.", FileName));

				File.Close();

				if (delete)
					File.Delete();

				File = null;
				IsOpen = false;
			}			
		}

		public void Close(bool delete) {
			lock (this) {
				if (delete)
					--refCount;

				if ((delete && refCount == 0) || !delete)
					CloseFile(delete);
			}
		}

		public void Reference() {
			lock (this) {
				refCount++;
			}
		}

		public void Unreference() {
			Close(true);
		}

		public void Persist(long start, long end) {
			long position = start;
			bool finished = false;

			var idNameMap = new Dictionary<long, string>();
			var updated = new List<ISystemJournaledResource>();

			var reader = new BinaryReader(File.FileStream, Encoding.Unicode);
			File.FileStream.Seek(start, SeekOrigin.Begin);

			while (!finished) {
				var type = reader.ReadInt64();
				var size = reader.ReadInt32();
				position = position + size + 12;

				if (type == 2) {
					PersistIdTag(reader, idNameMap, updated);
				} else if (type == 6) {
					PersistDelete(reader, idNameMap);
				} else if (type == 3) {
					PersistSizeChange(reader, idNameMap);
				} else if (type == 1) {
					PersistPageModify(reader, idNameMap);
				} else if (type == 100) {
					if (position == end)
						finished = true;
				} else {
					throw new InvalidOperationException();
				}
			}

			foreach (var resource in updated) {
				resource.Synch();
			}

			reader.Close();
		}

		private void PersistIdTag(BinaryReader reader, IDictionary<long, string> idNameMap, IList<ISystemJournaledResource> resources) {
			long id = reader.ReadInt64();
			int len = reader.ReadInt32();
			StringBuilder buf = new StringBuilder(len);
			for (int i = 0; i < len; ++i) {
				buf.Append(reader.ReadChar());
			}

			var name = buf.ToString();

			idNameMap[id] = name;

			resources.Add(System.GetResource(name));
		}

		private void PersistDelete(BinaryReader reader, IDictionary<long, string> idNameMap) {
			var id = reader.ReadInt64();
			var name = idNameMap[id];
			var resource = System.GetResource(name);

			resource.PersistDelete();
		}

		private void PersistSizeChange(BinaryReader reader, IDictionary<long, string> idNameMap) {
			var id = reader.ReadInt64();
			var newSize = reader.ReadInt64();
			var name = idNameMap[id];
			var resource = System.GetResource(name);

			resource.PersistSetSize(newSize);
		}

		private void PersistPageModify(BinaryReader reader, IDictionary<long, string> idNameMap) {
			long id = reader.ReadInt64();
			long page = reader.ReadInt64();
			int off = reader.ReadInt32();
			int len = reader.ReadInt32();

			var resourceName = idNameMap[id];
			var resource = System.GetResource(resourceName);

			resource.PersistPageChange(page, off, len, reader);
		}

		public void Checkpoint() {
			lock (this) {
				writer.Write((byte)JournalFileCommand.Checkpoint);
				writer.Write(0);

				// Flush and synch the journal file
				FlushAndSynch();
			}
		}

		public void DeleteResource(string resourceName) {
			lock (this) {
				var resourceId = WriteResourceName(resourceName, writer);
				writer.Write((byte)JournalFileCommand.DeleteResource);
				writer.Write(8);
				writer.Write(resourceId);
			}
		}

		public void ChangeResourceSize(string resourceName, long newSize) {
			lock (this) {
				// Build the header,
				var resourceId = WriteResourceName(resourceName, writer);

				writer.Write((byte)JournalFileCommand.ResourceSizeChange);
				writer.Write(8 + 8);
				writer.Write(resourceId);
				writer.Write(newSize);
			}

		}

		internal JournalEntry ModifyPage(string resourceName, long pageNumber, byte[] buf, int off, int len) {
			long reference;
			lock (this) {
				long resourceId = WriteResourceName(resourceName, writer);

				// The absolute position of the page,
				long absPos = pageNumber * System.PageSize;
				
				writer.Write((byte)JournalFileCommand.ModifyPage);
				writer.Write(8 + 8 + 4 + 4 + len);
				writer.Write(resourceId);

				writer.Write((long)(absPos / 8192));
				writer.Write((int)(off + (int)(absPos & 8191)));
				writer.Write(len);

				writer.Write(buf, off, len);

				// Flush the changes so we can work output the pointer.
				writer.Flush();
				reference = File.Length - len - 36;
			}

			
			return new JournalEntry(this, resourceName, reference, pageNumber);
		}

		private void FlushAndSynch() {
			lock (this) {
				writer.Flush();
				File.Sync();
			}
		}

		public void BuildPage(long pageNumber, long position, byte[] buffer, int offset) {
			lock (this) {
				File.Read(position, buffer, 0, 36);
				var type = ByteBuffer.ReadInt8(buffer, 0);
				var resourceId = ByteBuffer.ReadInt8(buffer, 12);
				var pageNum = ByteBuffer.ReadInt8(buffer, 20);
				var pageOffset = ByteBuffer.ReadInt4(buffer, 28);
				var pageLength = ByteBuffer.ReadInt4(buffer, 32);

				if (type != 1)
					throw new IOException("Invalid page type. type = " + type + " pos = " + position);

				if (pageNum != pageNumber)
					throw new IOException("Page numbers do not match.");

				// Read the content.
				File.Read(position + 36, buffer, offset + pageOffset, pageLength);
			}
		}

		public JournalRegistryInfo Open() {
			if (IsOpen)
				throw new IOException("The registry is already open.");

			if (!System.FileSystem.FileExists(FileName))
				throw new IOException(string.Format("The file '{0}' does not exist.", FileName));

			var file = System.FileSystem.OpenFile(FileName, IsReadOnly);
			File = new JournalFile(file);

			IsOpen = true;

			var info = new JournalRegistryInfo(this);

			var endOffset = File.Length;

			// if we can't even read the journal number return an empty info
			if (endOffset < 8)
				return info;

			var reader = new BinaryReader(File.FileStream, Encoding.Unicode);

			try {
				var journalNumber = reader.ReadInt64();

				var resources = new List<string>();

				// the start offset is after the journal number
				long offset = 8;

				while (true) {
					// If we can't read 12 bytes ahead, return the summary
					if (offset + 12 > endOffset)
						return info;

					long type = reader.ReadInt64();
					int size = reader.ReadInt32();

					offset = offset + size + 12;

					bool skipBody = true;

					// If checkpoint reached then we are recoverable
					if (type == 100) {
						info.LastCheckPoint = offset;
						info.Recoverable = true;

						// Add the resources input this check point
						info.AddResources(resources);

						// And clear the temporary list.
						resources.Clear();
					} else if (offset >= endOffset || type < 1 || type > 7) {
						// If end reached, or type is not understood then return
						return info;
					}

					if (type == 2) {
						// We don't skip body for this type, we Read the content
						skipBody = false;

						var id = reader.ReadInt64();
						var strLength = reader.ReadInt32();
						StringBuilder str = new StringBuilder(strLength);
						for (int i = 0; i < strLength; ++i) {
							str.Append(reader.ReadChar());
						}

						var resourceName = str.ToString();
						resources.Add(resourceName);
					}

					if (skipBody)
						reader.ReadBytes(size);
				}
			} finally {
				reader.Close();
			}
		}

		public void Dispose() {
			Dispose(true);
			GC.SuppressFinalize(this);
		}

		private void Dispose(bool disposing) {
			if (disposing) {
				if (File != null)
					File.Dispose();

				if (writer != null)
					writer.Close();
			}

			File = null;
			writer = null;
		}
	}
}
