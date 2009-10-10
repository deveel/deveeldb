//  
//  JournalledSystem.cs
//  
//  Author:
//       Antonello Provenzano <antonello@deveel.com>
//       Tobias Downer <toby@mckoi.com>
// 
//  Copyright (c) 2009 Deveel
// 
//  This program is free software: you can redistribute it and/or modify
//  it under the terms of the GNU General Public License as published by
//  the Free Software Foundation, either version 3 of the License, or
//  (at your option) any later version.
// 
//  This program is distributed in the hope that it will be useful,
//  but WITHOUT ANY WARRANTY; without even the implied warranty of
//  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
//  GNU General Public License for more details.
// 
//  You should have received a copy of the GNU General Public License
//  along with this program.  If not, see <http://www.gnu.org/licenses/>.

using System;
using System.Collections;
using System.IO;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;

using Deveel.Diagnostics;
using Deveel.Data.Util;

namespace Deveel.Data.Store {
	/// <summary>
	/// Manages a journalling data store management system.
	/// </summary>
	/// <remarks>
	/// All operations are written output to a log that can be easily 
	/// recovered from if a crash occurs.
	/// </remarks>
	class JournalledSystem {
		/// <summary>
		/// Set to true for logging behaviour.
		/// </summary>
		private readonly bool ENABLE_LOGGING;

		/// <summary>
		/// The path to the journal files.
		/// </summary>
		private readonly string journal_path;

		/// <summary>
		/// If the journal system is input Read only mode.
		/// </summary>
		private readonly bool read_only;

		/// <summary>
		/// The page size.
		/// </summary>
		private readonly int page_size;

		/// <summary>
		/// The map of all resources that are available.  (resource_name -> Resource)
		/// </summary>
		private readonly Hashtable all_resources;

		/// <summary>
		/// The unique sequence id counter for this session.
		/// </summary>
		private long seq_id;

		/// <summary>
		/// The archive of journal files currently pending (JournalFile).
		/// </summary>
		private readonly ArrayList journal_archives;

		/// <summary>
		/// The current top journal file.
		/// </summary>
		private JournalFile top_journal_file;

		/// <summary>
		/// The current journal file number.
		/// </summary>
		private long journal_number;

		/// <summary>
		/// A factory that creates <see cref="IStoreDataAccessor"/> objects used 
		/// to access the resource with the given name.
		/// </summary>
		private readonly LoggingBufferManager.IStoreDataAccessorFactory sda_factory;

		/// <summary>
		/// Mutex when accessing the top journal file.
		/// </summary>
		private readonly Object top_journal_lock = new Object();

		/// <summary>
		/// A thread that runs input the background and persists information that is 
		/// input the journal.
		/// </summary>
		private JournalingThread journaling_thread;


		internal JournalledSystem(string journal_path, bool read_only, int page_size,
					   LoggingBufferManager.IStoreDataAccessorFactory sda_factory,
					   bool enable_logging) {
			this.journal_path = journal_path;
			this.read_only = read_only;
			this.page_size = page_size;
			this.sda_factory = sda_factory;
			all_resources = new Hashtable();
			journal_number = 0;
			journal_archives = new ArrayList();
			this.ENABLE_LOGGING = enable_logging;
		}

		/// <summary>
		/// Returns a journal file name with the given number.
		/// </summary>
		/// <param name="number"></param>
		/// <remarks>
		/// The journal number must be between 10 and 73.
		/// </remarks>
		/// <returns></returns>
		private static String GetJournalFileName(int number) {
			if (number < 10 || number > 73) {
				throw new ApplicationException("Journal file name output of range.");
			}
			return "jnl" + number;
		}

		// Lock used during initialization
		private readonly Object init_lock = new Object();

		/// <summary>
		/// Starts the journal system.
		/// </summary>
		internal void start() {
			if (ENABLE_LOGGING) {
				lock (init_lock) {
					if (journaling_thread == null) {
						// Start the background journaling thread,
						journaling_thread = new JournalingThread(this);
						journaling_thread.Start();
						// Scan for any changes and make the changes.
						RollForwardRecover();
						if (!read_only) {
							// Create a new top journal file
							NewTopJournalFile();
						}
					} else {
						throw new ApplicationException("Assertion failed - already started.");
					}
				}
			}
		}

		/// <summary>
		/// Stops the journal system.
		/// </summary>
		/// <remarks>
		/// This will persist any pending changes up to the last check point 
		/// and then finish.
		/// </remarks>
		internal void Stop() {
			if (ENABLE_LOGGING) {
				lock (init_lock) {
					if (journaling_thread != null) {
						// Stop the journal thread
						journaling_thread.PersistArchives(0);
						journaling_thread.Finish();
						journaling_thread.WaitUntilFinished();
						journaling_thread = null;
					} else {
						throw new ApplicationException("Assertion failed - already stopped.");
					}
				}

				if (!read_only) {
					// Close any remaining journals and roll forward recover (shouldn't
					// actually be necessary but just incase...)
					lock (top_journal_lock) {
						// Close all the journals
						int sz = journal_archives.Count;
						for (int i = 0; i < sz; ++i) {
							JournalFile jf = (JournalFile)journal_archives[i];
							jf.Close();
						}
						// Close the top journal
						TopJournal.Close();
						// Scan for journals and make the changes.
						RollForwardRecover();
					}
				}

			}
		}

		/// <summary>
		/// Recovers any lost operations that are currently input the journal.
		/// </summary>
		/// <remarks>
		/// This retries all logged entries. This would typically be called before 
		/// any other IO operations.
		/// </remarks>
		private void RollForwardRecover() {
			//    Console.Out.WriteLine("RollForwardRecover()");

			// The list of all journal files,
			ArrayList journal_files_list = new ArrayList();

			// Scan the journal path for any journal files.
			for (int i = 10; i < 74; ++i) {
				String journal_fn = GetJournalFileName(i);
				string f = Path.Combine(journal_path, journal_fn);
				// If the journal exists, create a summary of the journal
				if (File.Exists(f)) {
					if (read_only) {
						throw new IOException(
							"Journal file " + f + " exists for a Read-only session.  " +
							"There may not be any pending journals for a Read-only session.");
					}

					JournalFile jf = new JournalFile(this, f, read_only);
					// Open the journal file for recovery.  This will set various
					// information about the journal such as the last check point and the
					// id of the journal file.
					JournalSummary summary = jf.OpenForRecovery();
					// If the journal can be recovered from.
					if (summary.can_be_recovered) {
						if (Debug.IsInterestedIn(DebugLevel.Information)) {
							Debug.Write(DebugLevel.Information, this, "Journal " + jf +
															   " found - can be recovered.");
						}
						journal_files_list.Add(summary);
					} else {
						if (Debug.IsInterestedIn(DebugLevel.Information)) {
							Debug.Write(DebugLevel.Information, this, "Journal " + jf +
															   " deleting - nothing to recover.");
						}
						// Otherwise close and delete it
						jf.CloseAndDelete();
					}
				}
			}

			//    if (journal_files_list.size() == 0) {
			//      Console.Out.WriteLine("Nothing to recover.");
			//    }

			// Sort the journal file list from oldest to newest.  The oldest journals
			// are recovered first.
			journal_files_list.Sort(journal_list_comparator);

			long last_journal_number = -1;

			// Persist the journals
			for (int i = 0; i < journal_files_list.Count; ++i) {
				JournalSummary summary = (JournalSummary)journal_files_list[i];

				// Check the resources for this summary
				ArrayList res_list = summary.resource_list;
				for (int n = 0; n < res_list.Count; ++n) {
					String resource_name = (String)res_list[n];
					// This puts the resource into the hash map.
					IJournalledResource resource = CreateResource(resource_name);
				}

				// Assert that we are recovering the journals input the correct order
				JournalFile jf = summary.journal_file;
				if (jf.journal_number < last_journal_number) {
					throw new ApplicationException("Assertion failed, sort failed.");
				}
				last_journal_number = jf.journal_number;

				if (Debug.IsInterestedIn(DebugLevel.Information)) {
					Debug.Write(DebugLevel.Information, this, "Recovering: " + jf +
													   " (8 .. " + summary.last_checkpoint + ")");
				}

				jf.Persist(8, summary.last_checkpoint);
				// Then close and delete.
				jf.CloseAndDelete();

				// Check the resources for this summary and close them
				for (int n = 0; n < res_list.Count; ++n) {
					String resource_name = (String)res_list[n];
					AbstractResource resource =
						(AbstractResource)CreateResource(resource_name);
					// When we finished, make sure the resource is closed again
					// Close the resource
					resource.PersistClose();
					// Post recover notification
					resource.OnPostRecover();
				}
			}
		}

		private IComparer journal_list_comparator = new JournalSummaryComparer();

		private class JournalSummaryComparer : IComparer {
			public int Compare(Object ob1, Object ob2) {
				JournalSummary js1 = (JournalSummary)ob1;
				JournalSummary js2 = (JournalSummary)ob2;

				long jn1 = js1.journal_file.JournalNumber;
				long jn2 = js2.journal_file.JournalNumber;

				if (jn1 > jn2) {
					return 1;
				} else if (jn1 < jn2) {
					return -1;
				} else {
					return 0;
				}
			}
		}

		/// <summary>
		/// Creates a new top journal file.
		/// </summary>
		private void NewTopJournalFile() {
			//    // Move the old journal to the archive?
			//    if (top_journal_file != null) {
			//      journal_archives.add(top_journal_file);
			//    }

			String journal_fn = GetJournalFileName((int)((journal_number & 63) + 10));
			++journal_number;

			string f = Path.Combine(journal_path, journal_fn);
			if (File.Exists(f)) {
				throw new IOException("Journal file already exists.");
			}

			top_journal_file = new JournalFile(this, f, read_only);
			top_journal_file.open(journal_number - 1);
		}

		/// <summary>
		/// Returns the current top journal file.
		/// </summary>
		private JournalFile TopJournal {
			get {
				lock (top_journal_lock) {
					return top_journal_file;
				}
			}
		}

		/// <summary>
		/// Creates a resource.
		/// </summary>
		/// <param name="resource_name"></param>
		/// <returns></returns>
		public IJournalledResource CreateResource(String resource_name) {
			AbstractResource resource;
			lock (all_resources) {
				// Has this resource previously been open?
				resource = (AbstractResource)all_resources[resource_name];
				if (resource == null) {
					// No...
					// Create a unique id for this
					long id = seq_id;
					++seq_id;
					// Create the IStoreDataAccessor for this resource.
					IStoreDataAccessor accessor =
										sda_factory.CreateStoreDataAccessor(resource_name);
					if (ENABLE_LOGGING) {
						resource = new Resource(this, resource_name, id, accessor);
					} else {
						resource = new NonLoggingResource(this, resource_name, id, accessor);
					}
					// Put this input the map.
					all_resources[resource_name] = resource;
				}
			}

			// Return the resource
			return resource;
		}

		/// <summary>
		/// Sets a check point input the log.
		/// </summary>
		/// <param name="flush_journals">If is true then when the method returns we are guarenteed 
		/// that all the journals are flushed and the data is absolutely current, if false then we 
		/// can't assume the journals will be empty when the method returns.</param>
		internal void SetCheckPoint(bool flush_journals) {
			// No Logging
			if (!ENABLE_LOGGING) {
				return;
			}
			// Return if Read-only
			if (read_only) {
				return;
			}

			bool something_to_persist;

			lock (top_journal_lock) {
				JournalFile top_j = TopJournal;

				// When the journal exceeds a threshold then we cycle the top journal
				if (flush_journals || top_j.Length > (256 * 1024)) {
					// Cycle to the next journal file
					NewTopJournalFile();
					// Add this to the archives
					journal_archives.Add(top_j);
				}
				something_to_persist = journal_archives.Count > 0;
				top_j.SetCheckPoint();
			}

			if (something_to_persist) {
				// Notifies the background thread that there is something to persist.
				// This will block until there are at most 10 journal files open.
				journaling_thread.PersistArchives(10);
			}

		}

		/// <summary>
		/// Returns the Resource with the given name.
		/// </summary>
		/// <param name="resource_name"></param>
		/// <returns></returns>
		private AbstractResource GetResource(String resource_name) {
			lock (all_resources) {
				return (AbstractResource)all_resources[resource_name];
			}
		}





		// ---------- Inner classes ----------

		/// <summary>
		/// Represents a file input which modification are logged output to when 
		/// changes are made.
		/// </summary>
		/// <remarks>
		/// Contains instructions for rebuilding a resource to a known stable state.
		/// </remarks>
		private sealed class JournalFile {
			private readonly JournalledSystem js;

			/// <summary>
			/// The path to the file of this journal input the file system.
			/// </summary>
			private readonly string file;

			/// <summary>
			/// True if the journal file is Read only.
			/// </summary>
			private readonly bool read_only;

			/// <summary>
			/// The <see cref="StreamFile"/> object for reading and writing entries 
			/// to/from the journal.
			/// </summary>
			private StreamFile data;

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
			private Hashtable resource_id_map;

			/// <summary>
			/// The sequence id for resources modified input this log.
			/// </summary>
			private long cur_seq_id;

			/// <summary>
			/// The journal number of this journal.
			/// </summary>
			internal long journal_number;

			/// <summary>
			/// True when open.
			/// </summary>
			private bool is_open;

			/// <summary>
			/// The number of threads currently looking at info input this journal.
			/// </summary>
			private int reference_count;

			/// <summary>
			/// Constructs the journal file.
			/// </summary>
			/// <param name="js"></param>
			/// <param name="file"></param>
			/// <param name="read_only"></param>
			public JournalFile(JournalledSystem js, string file, bool read_only) {
				this.js = js;
				this.file = file;
				this.read_only = read_only;
				this.is_open = false;
				buffer = new byte[36];
				resource_id_map = new Hashtable();
				cur_seq_id = 0;
				reference_count = 1;
			}

			/// <summary>
			/// Returns the size of the journal file input bytes.
			/// </summary>
			internal long Length {
				get { return data.Length; }
			}

			/// <summary>
			/// Returns the journal number assigned to this journal.
			/// </summary>
			internal long JournalNumber {
				get { return journal_number; }
			}

			/// <summary>
			/// Opens the journal file.
			/// </summary>
			/// <param name="journal_number"></param>
			/// <exception cref="IOException">
			/// If the journal file exists.
			/// </exception>
			internal void open(long journal_number) {
				if (is_open) {
					throw new IOException("Journal file is already open.");
				}
				if (File.Exists(file)) {
					throw new IOException("Journal file already exists.");
				}

				this.journal_number = journal_number;
				data = new StreamFile(file, read_only ? FileAccess.Read : FileAccess.ReadWrite);
				data_out = new BinaryWriter(new BufferedStream(data.GetOutputStream()), Encoding.UTF8);
				data_out.Write(journal_number);
				is_open = true;
			}

			/// <summary>
			/// Opens the journal for recovery.
			/// </summary>
			/// <remarks>
			/// This scans the journal and generates some statistics about the journal file 
			/// such as the last check point and the journal number.
			/// </remarks>
			/// <returns></returns>
			/// <exception cref="IOException">
			/// If the journal file doesn't exist.
			/// </exception>
			internal JournalSummary OpenForRecovery() {
				if (is_open) {
					throw new IOException("Journal file is already open.");
				}
				if (!File.Exists(file)) {
					throw new IOException("Journal file does not exists.");
				}

				// Open the random access file to this journal
				data = new StreamFile(file, read_only ? FileAccess.Read : FileAccess.ReadWrite);
				is_open = true;

				// Create the summary object (by default, not recoverable).
				JournalSummary summary = new JournalSummary(this);

				long end_pointer = data.Length;

				// If end_pointer < 8 then can't recovert this journal
				if (end_pointer < 8) {
					return summary;
				}

				// The input stream.
				BinaryReader din = new BinaryReader(data.GetInputStream(), Encoding.UTF8);

				try {
					// Set the journal number for this
					this.journal_number = din.ReadInt64();
					long position = 8;

					ArrayList checkpoint_res_list = new ArrayList();

					// Start scan
					while (true) {

						// If we can't Read 12 bytes ahead, return the summary
						if (position + 12 > end_pointer) {
							return summary;
						}

						long type = din.ReadInt64();
						int size = din.ReadInt32();

						//          Console.Out.WriteLine("Scan: " + type + " pos=" + position + " size=" + size);
						position = position + size + 12;

						bool skip_body = true;

						// If checkpoint reached then we are recoverable
						if (type == 100) {
							summary.last_checkpoint = position;
							summary.can_be_recovered = true;

							// Add the resources input this check point
							summary.resource_list.AddRange(checkpoint_res_list);
							// And clear the temporary list.
							checkpoint_res_list.Clear();

						}

						// If end reached, or type is not understood then return
						else if (position >= end_pointer ||
								 type < 1 || type > 7) {
							return summary;
						}

						// If we are resource type, then load the resource
						if (type == 2) {

							// We don't skip body for this type, we Read the content
							skip_body = false;
							long id = din.ReadInt64();
							int str_len = din.ReadInt32();
							StringBuilder str = new StringBuilder(str_len);
							for (int i = 0; i < str_len; ++i) {
								str.Append(din.ReadChar());
							}

							String resource_name = str.ToString();
							checkpoint_res_list.Add(resource_name);

						}

						if (skip_body) {
							int to_skip = size;
							while (to_skip > 0) {
								// original java way...
								// to_skip -= din.skip(to_skip);
								/*
								if (din.BaseStream is InputStream) {
									to_skip -= (int)((InputStream)din.BaseStream).Skip(to_skip);
								} else {
								*/
									long curPos = din.BaseStream.Position;
									long newPos = din.BaseStream.Seek(to_skip, SeekOrigin.Current);
									to_skip -= (int)(newPos - curPos);
								//}
							}
						}

					}

				} finally {
					din.Close();
				}

			}

			/// <summary>
			/// Closes the journal file.
			/// </summary>
			internal void Close() {
				lock (this) {
					if (!is_open) {
						throw new IOException("Journal file is already closed.");
					}

					data.Close();
					data = null;
					is_open = false;
				}
			}

			/// <summary>
			/// Returns true if the journal is deleted.
			/// </summary>
			internal bool IsDeleted {
				get {
					lock (this) {
						return data == null;
					}
				}
			}

			/// <summary>
			/// Closes and deletes the journal file.
			/// </summary>
			/// <remarks>
			/// This may not immediately close and delete the journal file if there are currently 
			/// references to it (for example, input the middle of a read operation).
			/// </remarks>
			internal void CloseAndDelete() {
				lock (this) {
					--reference_count;
					if (reference_count == 0) {
						// Close and delete the journal file.
						Close();
						File.Delete(file);
						if (File.Exists(file)) {
							Console.Out.WriteLine("Unable to delete journal file: " + file);
						}
					}
				}
			}

			/// <summary>
			/// Adds a reference preventing the journal file from being deleted.
			/// </summary>
			internal void AddReference() {
				lock (this) {
					if (reference_count != 0) {
						++reference_count;
					}
				}
			}

			/// <summary>
			/// Removes a reference, if we are at the last reference the journal file 
			/// is deleted.
			/// </summary>
			internal void RemoveReference() {
				CloseAndDelete();
			}

			/// <summary>
			/// Plays the log from the given offset input the file to the next checkpoint.
			/// </summary>
			/// <param name="start"></param>
			/// <param name="end"></param>
			/// <remarks>
			/// This will actually persist the log.  Returns -1 if the end of the journal
			/// is reached.
			/// <para>
			/// <b>Note</b>: This will not verify that the journal is correct. Verification 
			/// should be done before the persist.
			/// </para>
			/// </remarks>
			internal void Persist(long start, long end) {
				if (Debug.IsInterestedIn(DebugLevel.Information)) {
					Debug.Write(DebugLevel.Information, this, "Persisting: " + file);
				}

				BinaryReader din = new BinaryReader(data.GetInputStream(), Encoding.UTF8);
				long count = start;
				// Skip to the offset
				while (count > 0) {
					// original java way...
					// count -= din.skip(count);
					///*
					//if (din.BaseStream is InputStream) {
					//    count -= (int)((InputStream)din.BaseStream).Skip(count);
					//} else {
					//*/
					//    long curPos = din.BaseStream.Position;
					//    long newPos = din.BaseStream.Seek(count, SeekOrigin.Current);
					//    count -= (int)(newPos - curPos);
					////}
					count -= InputStream.Skip(din, count);
				}

				// The list of resources we updated
				ArrayList resources_updated = new ArrayList();

				// A map from resource id to resource name for this journal.
				Hashtable id_name_map = new Hashtable();

				bool finished = false;
				long position = start;

				while (!finished) {
					long type = din.ReadInt64();
					int size = din.ReadInt32();
					position = position + size + 12;

					if (type == 2) {       // Resource id tag
						long id = din.ReadInt64();
						int len = din.ReadInt32();
						/*
						TODO:
						StringBuilder buf = new StringBuilder(len);
						for (int i = 0; i < len; ++i) {
							buf.Append(din.ReadChar());
						}
						String resource_name = buf.ToString();
						*/
						byte[] buf = new byte[len];
						din.Read(buf, 0, len);
						string resource_name = Encoding.UTF8.GetString(buf);

						// Put this input the map
						id_name_map[id] = resource_name;

						if (Debug.IsInterestedIn(DebugLevel.Information)) {
							Debug.Write(DebugLevel.Information, this, "Journal Command: Tag: " + id +
															   " = " + resource_name);
						}

						// Add this to the list of resources we updated.
						resources_updated.Add(js.GetResource(resource_name));

					} else if (type == 6) {  // Resource delete
						long id = din.ReadInt64();
						String resource_name = (String)id_name_map[id];
						AbstractResource resource = js.GetResource(resource_name);

						if (Debug.IsInterestedIn(DebugLevel.Information)) {
							Debug.Write(DebugLevel.Information, this, "Journal Command: Delete: " +
															   resource_name);
						}

						resource.PersistDelete();

					} else if (type == 3) {  // Resource size change
						long id = din.ReadInt64();
						long new_size = din.ReadInt64();
						String resource_name = (String)id_name_map[id];
						AbstractResource resource = js.GetResource(resource_name);

						if (Debug.IsInterestedIn(DebugLevel.Information)) {
							Debug.Write(DebugLevel.Information, this, "Journal Command: Set Size: " +
										resource_name + " size = " + new_size);
						}

						resource.PersistSetSize(new_size);

					} else if (type == 1) {   // Page modification
						long id = din.ReadInt64();
						long page = din.ReadInt64();
						int off = din.ReadInt32();
						int len = din.ReadInt32();

						String resource_name = (String)id_name_map[id];
						AbstractResource resource = js.GetResource(resource_name);

						if (Debug.IsInterestedIn(DebugLevel.Information)) {
							Debug.Write(DebugLevel.Information, this,
									"Journal Command: Page Modify: " + resource_name +
									" page = " + page + " off = " + off +
									" len = " + len);
						}

						resource.PersistPageChange(page, off, len, din);

					} else if (type == 100) { // Checkpoint (end)

						if (Debug.IsInterestedIn(DebugLevel.Information)) {
							Debug.Write(DebugLevel.Information, this, "Journal Command: Check Point.");
						}

						if (position == end) {
							finished = true;
						}
					} else {
						throw new ApplicationException("Unknown tag type: " + type + " position = " + position);
					}

				}  // while (!finished)

				// Synch all the resources that we have updated.
				int sz = resources_updated.Count;
				for (int i = 0; i < sz; ++i) {
					AbstractResource r = (AbstractResource)resources_updated[i];
					if (Debug.IsInterestedIn(DebugLevel.Information)) {
						Debug.Write(DebugLevel.Information, this, "Synch: " + r);
					}
					r.Synch();
				}

				din.Close();

			}

			/// <summary>
			/// Writes a resource identifier to the stream for the resource with the given name.
			/// </summary>
			/// <param name="resource_name"></param>
			/// <param name="output"></param>
			/// <returns></returns>
			private long WriteResourceName(String resource_name, BinaryWriter output) {
				long v;
				lock (resource_id_map) {
					if (!resource_id_map.ContainsKey(resource_name)) {
						++cur_seq_id;

						//TODO: int len = resource_name.Length;
						byte[] buf = Encoding.UTF8.GetBytes(resource_name);

						// Write the header for this resource
						output.Write(2L);
						output.Write(8 + 4 + (buf.Length));
						output.Write(cur_seq_id);
						output.Write(buf.Length);
						output.Write(buf);
						/*
						TODO:
						for (int i = 0; i < resource_name.Length; i++)
							output.Write(resource_name[i]);
						*/
						// Put this id input the cache
						v = cur_seq_id;
						resource_id_map[resource_name] = v;
					} else {
						v = (long)resource_id_map[resource_name];
					}
				}

				return v;
			}

			/// <summary>
			/// Logs that a resource was deleted.
			/// </summary>
			/// <param name="resource_name"></param>
			internal void LogResourceDelete(String resource_name) {
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

			/// <summary>
			/// Logs a resource size change.
			/// </summary>
			/// <param name="resource_name"></param>
			/// <param name="new_size"></param>
			internal void logResourceSizeChange(String resource_name, long new_size) {
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

			/// <summary>
			/// Sets a check point.
			/// </summary>
			/// <remarks>
			/// This will add an entry to the log.
			/// </remarks>
			internal void SetCheckPoint() {
				lock (this) {
					data_out.Write(100L);
					data_out.Write(0);

					// Flush and synch the journal file
					FlushAndSynch();
				}
			}


			/// <summary>
			/// Logs a page modification to the end of the log and returns a pointer
			/// input the file to the modification.
			/// </summary>
			/// <param name="resource_name"></param>
			/// <param name="page_number"></param>
			/// <param name="buf"></param>
			/// <param name="off"></param>
			/// <param name="len"></param>
			/// <returns></returns>
			internal JournalEntry LogPageModification(String resource_name, long page_number, byte[] buf, int off, int len) {
				long reference;
				lock (this) {
					// Build the header,
					long v = WriteResourceName(resource_name, data_out);

					// The absolute position of the page,
					long absolute_position = page_number * js.page_size;

					// Write the header
					long resource_id = v;
					data_out.Write(1L);
					data_out.Write(8 + 8 + 4 + 4 + len);
					data_out.Write(resource_id);
					//        data_out.Write(page_number);
					//        data_out.Write(off);
					data_out.Write(absolute_position / 8192);
					data_out.Write(off + (int)(absolute_position & 8191));
					data_out.Write(len);

					data_out.Write(buf, off, len);

					// Flush the changes so we can work output the pointer.
					data_out.Flush();
					reference = data.Length - len - 36;
				}

				// Returns a JournalEntry object
				return new JournalEntry(resource_name, this, reference, page_number);
			}


			/// <summary>
			/// Reconstructs a modification that is logged input this journal.
			/// </summary>
			/// <param name="in_page_number"></param>
			/// <param name="position"></param>
			/// <param name="buf"></param>
			/// <param name="off"></param>
			internal void BuildPage(long in_page_number, long position, byte[] buf, int off) {
				long type;
				long resource_id;
				long page_number;
				int page_offset;
				int page_length;

				lock (this) {
					data.readFully(position, buffer, 0, 36);
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
					if (page_number != in_page_number) {
						throw new IOException("Page numbers do not match.");
					}

					// Read the content.
					data.readFully(position + 36, buf, off + page_offset, page_length);
				}

			}

			/// <summary>
			/// Synchronizes the log.
			/// </summary>
			void FlushAndSynch() {
				lock (this) {
					data_out.Flush();
					data.Synch();
				}
			}


			public override String ToString() {
				return "[JOURNAL: " + Path.GetFileName(file) + "]";
			}

		}

		/// <summary>
		/// A <see cref="JournalEntry"/> represents a modification that has been 
		/// logging input the journal for a specific page of a resource.
		/// </summary>
		/// <remarks>
		/// It contains the name of the log file, the position input the journal 
		/// of the modification, and the page number.
		/// </remarks>
		private sealed class JournalEntry {
			/// <summary>
			/// The resource that this page is on.
			/// </summary>
			private readonly String resource_name;

			/// <summary>
			/// The journal file.
			/// </summary>
			private readonly JournalFile journal;

			/// <summary>
			/// The position input the journal file.
			/// </summary>
			private readonly long position;

			/// <summary>
			/// The page number of this modification.
			/// </summary>
			private readonly long page_number;


			/// <summary>
			/// The next journal entry with the same page number
			/// </summary>
			internal JournalEntry next_page;


			/// <summary>
			/// Constructs the entry.
			/// </summary>
			/// <param name="resource_name"></param>
			/// <param name="journal"></param>
			/// <param name="position"></param>
			/// <param name="page_number"></param>
			public JournalEntry(String resource_name, JournalFile journal, long position, long page_number) {
				this.resource_name = resource_name;
				this.journal = journal;
				this.position = position;
				this.page_number = page_number;
			}

			/// <summary>
			/// Returns the journal file for this entry.
			/// </summary>
			public JournalFile File {
				get { return journal; }
			}

			/// <summary>
			/// Returns the position of the log entry input the journal file.
			/// </summary>
			public long Position {
				get { return position; }
			}

			/// <summary>
			/// Returns the page number of this modification log entry.
			/// </summary>
			public long PageNumber {
				get { return page_number; }
			}
		}


		/// <summary>
		/// An abstract resource.
		/// </summary>
		private abstract class AbstractResource : IJournalledResource {
			protected readonly JournalledSystem js;

			/// <summary>
			/// The unique name given this resource (the file name).
			/// </summary>
			protected readonly String name;

			/// <summary>
			/// The id assigned to this resource by this session.
			/// </summary>
			/// <remarks>
			/// This id should not be used input any external source.
			/// </remarks>
			private readonly long id;

			/// <summary>
			/// The backing object.
			/// </summary>
			protected readonly IStoreDataAccessor data;

			/// <summary>
			/// True if this resource is read_only.
			/// </summary>
			protected bool read_only;

			/// <summary>
			/// Constructs the resource.
			/// </summary>
			/// <param name="js"></param>
			/// <param name="name"></param>
			/// <param name="id"></param>
			/// <param name="data"></param>
			protected AbstractResource(JournalledSystem js, String name, long id, IStoreDataAccessor data) {
				this.js = js;
				this.name = name;
				this.id = id;
				this.data = data;
			}


			// ---------- Persist methods ----------

			internal abstract void PersistClose();

			internal abstract void PersistDelete();

			internal abstract void PersistSetSize(long new_size);

			internal abstract void PersistPageChange(long page, int off, int len, BinaryReader din);

			internal abstract void Synch();

			// Called after a RollForwardRecover to notify the resource to update its
			// state to reflect the fact that changes have occurred.
			internal abstract void OnPostRecover();

			// ----------

			/// <inheritdoc/>
			public int PageSize {
				get { return js.page_size; }
			}

			/// <inheritdoc/>
			public long Id {
				get { return id; }
			}

			public abstract void Read(long page_number, byte[] buf, int off);

			public abstract void Write(long page_number, byte[] buf, int off, int len);

			public abstract void SetSize(long size);

			public abstract long Size { get; }

			public abstract void Open(bool read_only);

			public abstract void Close();

			public abstract void Delete();

			public abstract bool Exists { get; }


			public override String ToString() {
				return name;
			}

		}

		/// <summary>
		/// An implementation of <see cref="AbstractResource"/> that doesn't log.
		/// </summary>
		private sealed class NonLoggingResource : AbstractResource {
			/// <summary>
			/// Constructs the resource.
			/// </summary>
			/// <param name="js"></param>
			/// <param name="name"></param>
			/// <param name="id"></param>
			/// <param name="data"></param>
			internal NonLoggingResource(JournalledSystem js, String name, long id, IStoreDataAccessor data)
				: base(js, name, id, data) {
			}


			// ---------- Persist methods ----------

			internal override void PersistClose() {
				// No-op
			}

			internal override void PersistDelete() {
				// No-op
			}

			internal override void PersistSetSize(long new_size) {
				// No-op
			}

			internal override void PersistPageChange(long page, int off, int len, BinaryReader din) {
				// No-op
			}

			internal override void Synch() {
				data.Synch();
			}

			internal override void OnPostRecover() {
				// No-op
			}

			// ----------

			/// <inheritdoc/>
			public override void Open(bool read_only) {
				this.read_only = read_only;
				data.Open(read_only);
			}

			/// <inheritdoc/>
			public override void Read(long page_number, byte[] buf, int off) {
				// Read the data.
				long page_position = page_number * js.page_size;
				data.Read(page_position + off, buf, off, js.page_size);
			}

			/// <inheritdoc/>
			public override void Write(long page_number, byte[] buf, int off, int len) {
				long page_position = page_number * js.page_size;
				data.Write(page_position + off, buf, off, len);
			}

			/// <inheritdoc/>
			public override void SetSize(long size) {
				data.SetSize(size);
			}

			/// <inheritdoc/>
			public override long Size {
				get { return data.Size; }
			}

			/// <inheritdoc/>
			public override void Close() {
				data.Close();
			}

			/// <inheritdoc/>
			public override void Delete() {
				data.Delete();
			}

			/// <inheritdoc/>
			public override bool Exists {
				get { return data.Exists; }
			}
		}

		/// <summary>
		/// Represents a resource input this system.
		/// </summary>
		/// <remarks>
		/// A resource is backed by a <see cref="IStoreDataAccessor"/> and may 
		/// have one or more modifications to it input the journal.
		/// </remarks>
		private sealed class Resource : AbstractResource {
			/// <summary>
			/// The size of the resource.
			/// </summary>
			private long size;

			/// <summary>
			/// True if there is actually data to be read input the above object.
			/// </summary>
			private bool there_is_backing_data;

			/// <summary>
			/// True if the underlying resource is really open.
			/// </summary>
			private bool really_open;

			/// <summary>
			/// True if the data store exists.
			/// </summary>
			private bool data_exists;

			/// <summary>
			/// True if the data resource is open.
			/// </summary>
			private bool data_open;

			/// <summary>
			/// True if the data resource was deleted.
			/// </summary>
			private bool data_deleted;

			/// <summary>
			/// The hash of all journal entries on this resource (JournalEntry).
			/// </summary>
			private readonly JournalEntry[] journal_map;

			/// <summary>
			/// A temporary buffer the size of a page.
			/// </summary>
			private readonly byte[] page_buffer;

			/// <summary>
			/// Constructs the resource.
			/// </summary>
			/// <param name="js"></param>
			/// <param name="name"></param>
			/// <param name="id"></param>
			/// <param name="data"></param>
			internal Resource(JournalledSystem js, String name, long id, IStoreDataAccessor data)
				: base(js, name, id, data) {
				journal_map = new JournalEntry[257];
				data_open = false;
				data_exists = data.Exists;
				data_deleted = false;
				if (data_exists) {
					try {
						size = data.Size;
						//          Console.Out.WriteLine("Setting size of " + name + " to " + size);
					} catch (IOException e) {
						throw new ApplicationException("Error getting size of resource: " + e.Message);
					}
				}
				really_open = false;
				page_buffer = new byte[js.page_size];
			}


			// ---------- Persist methods ----------

			private void PersistOpen(bool read_only) {
				//      Console.Out.WriteLine(name + " Open");
				if (!really_open) {
					data.Open(read_only);
					there_is_backing_data = true;
					really_open = true;
				}
			}

			internal override void PersistClose() {
				//      Console.Out.WriteLine(name + " Close");
				if (really_open) {
					// When we close we reset the size attribute.  We do this because of
					// the roll forward recovery.
					size = data.Size;
					data.Synch();
					data.Close();
					really_open = false;
				}
			}

			internal override void PersistDelete() {
				//      Console.Out.WriteLine(name + " Delete");
				// If open then close
				if (really_open) {
					PersistClose();
				}
				data.Delete();
				there_is_backing_data = false;
			}

			internal override void PersistSetSize(long new_size) {
				//      Console.Out.WriteLine(name + " Set Size " + size);
				// If not open then open.
				if (!really_open) {
					PersistOpen(false);
				}
				// Don't let us set a size that's smaller than the current size.
				if (new_size > data.Size) {
					data.SetSize(new_size);
				}
			}

			internal override void PersistPageChange(long page, int off, int len, BinaryReader din) {
				if (!really_open) {
					PersistOpen(false);
				}

				// Buffer to Read the page content into
				byte[] buf;
				if (len <= page_buffer.Length) {
					// If length is smaller or equal to the size of a page then use the
					// local page buffer.
					buf = page_buffer;
				} else {
					// Otherwise create a new buffer of the required size (this may happen
					// if the page size changes between sessions).
					buf = new byte[len];
				}

				// Read the change from the input stream
				din.Read(buf, 0, len);
				// Write the change output to the underlying resource container
				long pos = page * 8192; //page_size;
				data.Write(pos + off, buf, 0, len);
			}

			internal override void Synch() {
				if (really_open) {
					data.Synch();
				}
			}

			internal override void OnPostRecover() {
				data_exists = data.Exists;
			}


			// ----------

			/// <summary>
			/// Opens the resource.
			/// </summary>
			/// <param name="read_only">
			/// </param>
			/// <remarks>
			/// This method will check if the resource exists. If it doesn't exist the 
			/// <see cref="Read"/> method will return just the journal modifications of 
			/// a page. If it does exist it opens the resource and uses	that as the backing 
			/// to any <see cref="Read"/> operations.
			/// </remarks>
			public override void Open(bool read_only) {
				this.read_only = read_only;

				if (!data_deleted && data.Exists) {
					// It does exist so open it.
					PersistOpen(read_only);
				} else {
					there_is_backing_data = false;
					data_deleted = false;
				}
				data_open = true;
				data_exists = true;
			}

			/// <summary>
			/// Reads a page from the resource.
			/// </summary>
			/// <param name="page_number"></param>
			/// <param name="buf"></param>
			/// <param name="off"></param>
			/// <remarks>
			/// This method reconstructs the page from the underlying data, and from any 
			/// journal entries.This should read the data to be write into a buffer input 
			/// memory.
			/// </remarks>
			public override void Read(long page_number, byte[] buf, int off) {

				lock (journal_map) {
					if (!data_open) {
						throw new IOException("Assertion failed: Data file is not open.");
					}
				}

				// The list of all journal entries on this page number
				ArrayList all_journal_entries = new ArrayList(4);
				try {
					// The map index.
					lock (journal_map) {
						int i = ((int)(page_number & 0x0FFFFFFF) % journal_map.Length);
						JournalEntry entry = (JournalEntry)journal_map[i];
						JournalEntry prev = null;

						while (entry != null) {
							bool deleted_hash = false;

							JournalFile file = entry.File;
							// Note that once we have a reference the journal file can not be
							// deleted.
							file.AddReference();

							// If the file is closed (or deleted)
							if (file.IsDeleted) {
								deleted_hash = true;
								// Deleted so remove the reference to the journal
								file.RemoveReference();
								// Remove the journal entry from the chain.
								if (prev == null) {
									journal_map[i] = entry.next_page;
								} else {
									prev.next_page = entry.next_page;
								}
							}
								// Else if not closed then is this entry the page number?
							else if (entry.PageNumber == page_number) {
								all_journal_entries.Add(entry);
							} else {
								// Not the page we are looking for so remove the reference to the
								// file.
								file.RemoveReference();
							}

							// Only move prev is we have NOT deleted a hash entry
							if (!deleted_hash) {
								prev = entry;
							}
							entry = entry.next_page;
						}
					}

					// Read any data from the underlying file
					if (there_is_backing_data) {
						long page_position = page_number * js.page_size;
						// First Read the page from the underlying store.
						data.Read(page_position, buf, off, js.page_size);
					} else {
						// Clear the buffer
						for (int i = off; i < (js.page_size + off); ++i) {
							buf[i] = 0;
						}
					}

					// Rebuild from the journal file(s)
					int sz = all_journal_entries.Count;
					for (int i = 0; i < sz; ++i) {
						JournalEntry entry = (JournalEntry)all_journal_entries[i];
						JournalFile file = entry.File;
						long position = entry.Position;
						lock (file) {
							file.BuildPage(page_number, position, buf, off);
						}
					}

				} finally {

					// Make sure we remove the reference for all the journal files.
					int sz = all_journal_entries.Count;
					for (int i = 0; i < sz; ++i) {
						JournalEntry entry = (JournalEntry)all_journal_entries[i];
						JournalFile file = entry.File;
						file.RemoveReference();
					}

				}

			}

			/// <summary>
			/// Writes a page of some previously specified size to the top log.
			/// </summary>
			/// <param name="page_number"></param>
			/// <param name="buf"></param>
			/// <param name="off"></param>
			/// <param name="len"></param>
			/// <remarks>
			/// This will add a single entry to the log and any <see cref="Read"/> operations 
			/// after will contain the written data.
			/// </remarks>
			public override void Write(long page_number, byte[] buf, int off, int len) {
				lock (journal_map) {
					if (!data_open) {
						throw new IOException("Assertion failed: Data file is not open.");
					}

					// Make this modification input the log
					JournalEntry journal;
					lock (js.top_journal_lock) {
						journal = js.TopJournal.LogPageModification(name, page_number,
																   buf, off, len);
					}

					// This adds the modification to the END of the hash list.  This means
					// when we reconstruct the page the journals will always be input the
					// correct order - from oldest to newest.

					// The map index.
					int i = ((int)(page_number & 0x0FFFFFFF) % journal_map.Length);
					JournalEntry entry = (JournalEntry)journal_map[i];
					// Make sure this entry is added to the END
					if (entry == null) {
						// Add at the head if no first entry
						journal_map[i] = journal;
						journal.next_page = null;
					} else {
						// Otherwise search to the end
						// The number of journal entries input the linked list
						int journal_entry_count = 0;
						while (entry.next_page != null) {
							entry = entry.next_page;
							++journal_entry_count;
						}
						// and add to the end
						entry.next_page = journal;
						journal.next_page = null;

						// If there are over 35 journal entries, scan and remove all entries
						// on journals that have persisted
						if (journal_entry_count > 35) {
							int entries_cleaned = 0;
							entry = (JournalEntry)journal_map[i];
							JournalEntry prev = null;

							while (entry != null) {
								bool deleted_hash = false;

								JournalFile file = entry.File;
								// Note that once we have a reference the journal file can not be
								// deleted.
								file.AddReference();

								// If the file is closed (or deleted)
								if (file.IsDeleted) {
									deleted_hash = true;
									// Deleted so remove the reference to the journal
									file.RemoveReference();
									// Remove the journal entry from the chain.
									if (prev == null) {
										journal_map[i] = entry.next_page;
									} else {
										prev.next_page = entry.next_page;
									}
									++entries_cleaned;
								}
								// Remove the reference
								file.RemoveReference();

								// Only move prev is we have NOT deleted a hash entry
								if (!deleted_hash) {
									prev = entry;
								}
								entry = entry.next_page;
							}

						}
					}
				}

			}

			/// <inheritdoc/>
			public override void SetSize(long size) {
				lock (journal_map) {
					this.size = size;
				}
				lock (js.top_journal_lock) {
					js.TopJournal.logResourceSizeChange(name, size);
				}
			}

			/// <inheritdoc/>
			public override long Size {
				get {
					lock (journal_map) {
						return this.size;
					}
				}
			}

			/// <summary>
			/// Closes the resource.
			/// </summary>
			/// <remarks>
			/// This will actually simply log that the resource has 
			/// been closed.
			/// </remarks>
			public override void Close() {
				lock (journal_map) {
					data_open = false;
				}
			}

			/// <summary>
			/// Deletes the resource.
			/// </summary>
			/// <remarks>
			/// This will actually simply log that the resource has 
			/// been deleted.
			/// </remarks>
			public override void Delete() {
				// Log that this resource was deleted.
				lock (js.top_journal_lock) {
					js.TopJournal.LogResourceDelete(name);
				}
				lock (journal_map) {
					data_exists = false;
					data_deleted = true;
					size = 0;
				}
			}

			/// <inheritdoc/>
			public override bool Exists {
				get { return data_exists; }
			}
		}

		/// <summary>
		/// Summary information about a journal.
		/// </summary>
		private sealed class JournalSummary {
			/// <summary>
			/// The JournalFile object that is a summary of.
			/// </summary>
			internal JournalFile journal_file;

			/// <summary>
			/// True if the journal is recoverable (has one or more complete check
			/// points available).
			/// </summary>
			internal bool can_be_recovered = false;

			/// <summary>
			/// The position of the last checkpoint input the journal.
			/// </summary>
			internal long last_checkpoint;

			/// <summary>
			/// The list of all resource names that this journal 'touches'.
			/// </summary>
			internal ArrayList resource_list = new ArrayList();

			public JournalSummary(JournalFile journal_file) {
				this.journal_file = journal_file;
			}

		}

		/// <summary>
		/// Thread that persists the journal input the backgroudn.
		/// </summary>
		private class JournalingThread {
			private readonly JournalledSystem js;
			private readonly Thread thread;
			private bool finished = false;
			private bool actually_finished;
			private readonly object lockObject = new object();

			internal JournalingThread(JournalledSystem js) {
				this.js = js;
				thread = new Thread(new ThreadStart(run));
				thread.Name = "Background Journaling";
				// This is a daemon thread.  it should be safe if this thread
				// dies at any time.
				thread.IsBackground = true;
			}


			private void run() {
				bool local_finished = false;

				while (!local_finished) {

					ArrayList to_process = null;
					lock (js.top_journal_lock) {
						if (js.journal_archives.Count > 0) {
							to_process = new ArrayList();
							to_process.AddRange(js.journal_archives);
						}
					}

					if (to_process == null) {
						// Nothing to process so wait
						lock (this) {
							if (!finished) {
								try {
									Monitor.Wait(this);
								} catch (ThreadInterruptedException e) { /* ignore */ }
							}
						}

					} else if (to_process.Count > 0) {
						// Something to process, so go ahead and process the journals,
						int sz = to_process.Count;
						// For all journals
						for (int i = 0; i < sz; ++i) {
							// Pick the lowest journal to persist
							JournalFile jf = (JournalFile)to_process[i];
							try {
								// Persist the journal
								jf.Persist(8, jf.Length);
								// Close and then delete the journal file
								jf.CloseAndDelete();
							} catch (IOException e) {
								Debug.Write(DebugLevel.Error, this, "Error persisting journal: " + jf);
								Debug.WriteException(DebugLevel.Error, e);
								// If there is an error persisting the best thing to do is
								// finish
								lock (this) {
									finished = true;
								}
							}
						}
					}

					lock (this) {
						local_finished = finished;
						// Remove the journals that we have just persisted.
						if (to_process != null) {
							lock (js.top_journal_lock) {
								int sz = to_process.Count;
								for (int i = 0; i < sz; ++i) {
									js.journal_archives.RemoveAt(0);
								}
							}
						}
						// Notify any threads waiting
						Monitor.PulseAll(this);
					}

				}

				lock (this) {
					actually_finished = true;
					Monitor.PulseAll(this);
				}
			}

			[MethodImpl(MethodImplOptions.Synchronized)]
			public void Finish() {
				finished = true;
				Monitor.PulseAll(this);
			}

			[MethodImpl(MethodImplOptions.Synchronized)]
			public void WaitUntilFinished() {
				try {
					while (!actually_finished) {
						Monitor.Wait(this);
					}
				} catch (ThreadInterruptedException e) {
					throw new ApplicationException("Interrupted: " + e.Message);
				}
				Monitor.PulseAll(this);
			}

			/// <summary>
			/// Persists the journal_archives list until the list is at least the given size.
			/// </summary>
			/// <param name="until_size"></param>
			[MethodImpl(MethodImplOptions.Synchronized)]
			public void PersistArchives(int until_size) {
				Monitor.PulseAll(this);

				int sz;
				lock (js.top_journal_lock) {
					sz = js.journal_archives.Count;
				}
				// Wait until the sz is smaller than 'until_size'
				while (sz > until_size) {
					try {
						Monitor.Wait(this, 300);
					} catch (ThreadInterruptedException e) {
						/* ignore */
					}

					lock (js.top_journal_lock) {
						sz = js.journal_archives.Count;
					}
				}

			}

			[MethodImpl(MethodImplOptions.Synchronized)]
			public void Start() {
				thread.Start();
			}
		}

	}
}