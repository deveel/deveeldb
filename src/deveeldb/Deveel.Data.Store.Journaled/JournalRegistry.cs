using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

using Deveel.Data.Util;

namespace Deveel.Data.Store.Journaled {
	class JournalRegistry {
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

		public JournalingSystem System { get; private set; }

		public string FileName { get; private set; }

		public bool IsReadOnly { get; private set; }

		public File File { get; private set; }

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

			if (System.FileHandleFactory.FileExists(FileName))
				throw new IOException(String.Format("Journal file '{0}' already exists.", FileName));

			JournalNumber = journalNumber;
			File = new File(System, FileName, IsReadOnly);

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
	}
}
