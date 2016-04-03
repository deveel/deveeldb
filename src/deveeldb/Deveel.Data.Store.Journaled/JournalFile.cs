using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

using Deveel.Data.Diagnostics;
using Deveel.Data.Util;

namespace Deveel.Data.Store.Journaled {
	class JournalFile {
		/// <summary>
		/// A <see cref="BinaryWriter"/> object used to Write entries to the journal file.
		/// </summary>
		private BinaryWriter data_out;

		/// <summary>
		/// Small buffer.
		/// </summary>
		private byte[] buffer;

		/// <summary>
		/// A map between a resource name and an id for this journal file.
		/// </summary>
		private Dictionary<string, long> resource_id_map;

		/// <summary>
		/// The sequence id for resources modified input this log.
		/// </summary>
		private long cur_seq_id;

		/// <summary>
		/// True when open.
		/// </summary>
		private bool is_open;

		/// <summary>
		/// The number of threads currently looking at info input this journal.
		/// </summary>
		private int reference_count;

		public JournalFile(JournaledSystem journaledSystem, IFileSystem fileSystem, string path, bool readOnly) {
			JournaledSystem = journaledSystem;
			FileSystem = fileSystem;
			FilePath = path;
			ReadOnly = readOnly;

			this.is_open = false;
			buffer = new byte[36];
			resource_id_map = new Dictionary<string, long>();
			cur_seq_id = 0;
			reference_count = 1;
		}

		public JournaledSystem JournaledSystem { get; private set; }

		public StreamFile File { get; private set; }

		public string FilePath { get; private set; }

		public IFileSystem FileSystem { get; private set; }

		public bool ReadOnly { get; private set; }

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
			if (is_open) {
				throw new IOException("Journal file is already open.");
			}
			if (FileSystem.FileExists(FilePath)) {
				throw new IOException(String.Format("Journal file '{0}' already exists.", FilePath));
			}

			JournalNumber = journalNumber;
			File = new StreamFile(FileSystem, FilePath, ReadOnly);
#if PCL
			data_out = new BinaryWriter(File.FileStream, Encoding.Unicode);
#else
			data_out = new BinaryWriter(new BufferedStream(File.FileStream), Encoding.Unicode);
#endif
			data_out.Write(journalNumber);
			is_open = true;
		}

		internal JournalSummary OpenForRecovery() {
			if (is_open) {
				throw new IOException("Journal file is already open.");
			}
			if (!FileSystem.FileExists(FilePath)) {
				throw new IOException("Journal file does not exists.");
			}

			// Open the random access file to this journal
			File = new StreamFile(FileSystem, FilePath, ReadOnly);
			is_open = true;

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
			lock (resource_id_map) {
				if (!resource_id_map.TryGetValue(resourceName, out v)) {
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
					resource_id_map[resourceName] = v;
				}
			}

			return v;
		}

		public void LogResourceDelete(string resource_name) {
			lock (this) {
				// Build the header,
				long v = WriteResourceName(resource_name, data_out);

				// Write the header
				long resource_id = v;
				data_out.Write(6L);
				data_out.Write(8);
				data_out.Write(resource_id);
			}
		}

		public void LogResourceSizeChange(String resource_name, long new_size) {
			lock (this) {
				// Build the header,
				long v = WriteResourceName(resource_name, data_out);

				// Write the header
				long resource_id = v;
				data_out.Write(3L);
				data_out.Write(8 + 8);
				data_out.Write(resource_id);
				data_out.Write(new_size);
			}
		}

		public void SetCheckPoint() {
			lock (this) {
				data_out.Write(100L);
				data_out.Write(0);

				// Flush and synch the journal file
				FlushAndSynch();
			}
		}

		public JournalEntry LogPageModification(string resource_name, long page_number, byte[] buf, int off, int len) {
			long reference;
			lock (this) {
				// Build the header,
				long v = WriteResourceName(resource_name, data_out);

				// The absolute position of the page,
				long absolute_position = page_number * JournaledSystem.PageSize;

				// Write the header
				long resource_id = v;
				data_out.Write(1L);
				data_out.Write(8 + 8 + 4 + 4 + len);
				data_out.Write(resource_id);
				//        data_out.Write(page_number);
				//        data_out.Write(off);
				data_out.Write((long)(absolute_position / 8192));
				data_out.Write((int)(off + (int)(absolute_position & 8191)));
				data_out.Write(len);

				data_out.Write(buf, off, len);

				// Flush the changes so we can work output the pointer.
				data_out.Flush();
				reference = File.Length - len - 36;
			}

			// Returns a JournalEntry object
			return new JournalEntry(this, resource_name, reference, page_number);
		}

		private void FlushAndSynch() {
			lock (this) {
				data_out.Flush();
				File.Synch();
			}
		}

		public void Close() {
			lock (this) {
				if (!is_open) {
					throw new IOException("Journal file is already closed.");
				}

				File.Close();
				File.Dispose();
				File = null;
				is_open = false;
			}
		}

		public void CloseAndDelete() {
			lock (this) {
				--reference_count;
				if (reference_count == 0) {
					// Close and delete the journal file.
					Close();
					if (!FileSystem.DeleteFile(FilePath)) {
						// TODO: notify the system we couldn't delete the file
					}
				}
			}
		}

		internal void Persist(long start, long end) {
			JournaledSystem.Context.OnInformation(String.Format("Persisting file {0}", FilePath));

			using (BinaryReader din = new BinaryReader(File.FileStream, Encoding.Unicode)) {
				File.FileStream.Seek(start, SeekOrigin.Begin);

				// The list of resources we updated
				var resourcesUpdated = new List<ResourceBase>();

				// A map from resource id to resource name for this journal.
				var idNameMap = new Dictionary<long, string>();

				bool finished = false;
				long position = start;

				while (!finished) {
					long type = din.ReadInt64();
					int size = din.ReadInt32();
					position = position + size + 12;

					if (type == 2) {
						// Resource id tag
						long id = din.ReadInt64();
						int len = din.ReadInt32();
						StringBuilder buf = new StringBuilder(len);
						for (int i = 0; i < len; ++i) {
							buf.Append(din.ReadChar());
						}

						string resourceName = buf.ToString();
						// Put this input the map
						idNameMap[id] = resourceName;

						JournaledSystem.Context.OnInformation(String.Format("Jounral Command: Tag {0} = {1}", id, resourceName));

						// Add this to the list of resources we updated.
						resourcesUpdated.Add(JournaledSystem.GetResource(resourceName));
					} else if (type == 6) {
						// Resource delete
						long id = din.ReadInt64();
						var resourceName = idNameMap[id];
						var resource = JournaledSystem.GetResource(resourceName);

						JournaledSystem.Context.OnInformation(String.Format("Jounral Command: Delete {0}", resourceName));

						resource.PersistDelete();
					} else if (type == 3) {
						// Resource size change
						long id = din.ReadInt64();
						long newSize = din.ReadInt64();
						var resourceName = idNameMap[id];
						var resource = JournaledSystem.GetResource(resourceName);

						JournaledSystem.Context.OnInformation(String.Format("Jounral Command: Set Size {0} = {1}", resourceName, newSize));

						resource.PersistSetSize(newSize);
					} else if (type == 1) {
						// Page modification
						long id = din.ReadInt64();
						long page = din.ReadInt64();
						int off = din.ReadInt32();
						int len = din.ReadInt32();

						var resourceName = idNameMap[id];
						var resource = JournaledSystem.GetResource(resourceName);

						JournaledSystem.Context.OnInformation(
							String.Format("Jounral Command: Page Change {0} page= {1} offset = {2} length = {3}", resourceName, page, off, len));

						resource.PersistPageChange(page, off, len, din);
					} else if (type == 100) {
						// Checkpoint (end)

						JournaledSystem.Context.OnInformation("Jounral Command: Check Point");

						if (position == end) {
							finished = true;
						}
					} else {
						throw new Exception("Unknown tag type: " + type + " position = " + position);
					}

				} // while (!finished)

				// Synch all the resources that we have updated.
				int sz = resourcesUpdated.Count;
				for (int i = 0; i < sz; ++i) {
					var r = resourcesUpdated[i];

					JournaledSystem.Context.OnInformation(String.Format("Synch: {0}", r));

					r.Synch();
				}
			}
		}

		public void BuildPage(long pageNumber, long position, byte[] buffer, int offset) {
			long type;
			long resource_id;
			long page_number;
			int page_offset;
			int page_length;

			lock (this) {
				File.Read(position, buffer, 0, 36);

				type = ByteBuffer.ReadInt8(buffer, 0);
				resource_id = ByteBuffer.ReadInt8(buffer, 12);
				page_number = ByteBuffer.ReadInt8(buffer, 20);
				page_offset = ByteBuffer.ReadInt4(buffer, 28);
				page_length = ByteBuffer.ReadInt4(buffer, 32);

				// Some asserts,
				if (type != 1) {
					throw new IOException("Invalid page type. type = " + type +
										  " pos = " + position);
				}
				if (page_number != pageNumber) {
					throw new IOException("Page numbers do not match.");
				}

				// Read the content.
				File.Read(position + 36, buffer, offset + page_offset, page_length);
			}
		}
	}
}
