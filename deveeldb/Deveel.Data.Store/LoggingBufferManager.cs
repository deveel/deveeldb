//  
//  LoggingBufferManager.cs
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
using System.Threading;

using Deveel.Diagnostics;

namespace Deveel.Data.Store {
	/// <summary>
	/// A paged random access buffer manager that caches access between a 
	/// <see cref="IStore"/> and the underlying filesystem and that also 
	/// handles check point logging and crash recovery (via a 
	/// <see cref="JournalledSystem"/> object).
	/// </summary>
	public class LoggingBufferManager {
		/// <summary>
		/// Set to true for extra assertions.
		/// </summary>
		private static bool PARANOID_CHECKS = false;

		/// <summary>
		/// A timer that represents the T value in buffer pages.
		/// </summary>
		private long current_T;

		/// <summary>
		/// The number of pages in this buffer.
		/// </summary>
		private int current_page_count;

		/// <summary>
		/// The list of all pages.
		/// </summary>
		private readonly ArrayList page_list;

		/// <summary>
		/// A lock used when accessing the <see cref="current_T"/>, <see cref="page_list"/>
		/// and <see cref="current_page_count"/> members.
		/// </summary>
		private readonly Object T_lock = new Object();

		/// <summary>
		/// A hash map of all pages currently in memory keyed by store_id and page number.
		/// </summary>
		/// <remarks>
		/// <b>Note</b>: This <b>MUST</b> be final for the <see cref="FetchPage"/> method to 
		/// be safe.
		/// </remarks>
		private readonly BMPage[] page_map;

		/// <summary>
		/// A unique id key counter for all stores using this buffer manager.
		/// </summary>
		private int unique_id_seq;

		/// <summary>
		/// The JournalledSystem object that handles journalling of all data.
		/// </summary>
		private readonly JournalledSystem journalled_system;

		/// <summary>
		/// The maximum number of pages that should be kept in memory before pages
		/// are written out to disk.
		/// </summary>
		private readonly int max_pages;

		/// <summary>
		/// The size of each page.
		/// </summary>
		private readonly int page_size;

		// ---------- Write locks ----------

		/// <summary>
		/// Set to true when a <see cref="SetCheckPoint"/> is in progress.
		/// </summary>
		private bool check_point_in_progress;

		/// <summary>
		/// The number of Write locks currently on the buffer.
		/// </summary>
		/// <remarks>
		/// Any number of write locks can be obtained, however a <see cref="SetCheckPoint"/> can only 
		/// be achieved when there are no Write operations in progress.
		/// </remarks>
		private int write_lock_count;

		/// <summary>
		///  A mutex for when modifying the Write Lock information.
		/// </summary>
		private readonly Object write_lock = new Object();


		/// <summary>
		/// Constructs the manager.
		/// </summary>
		/// <param name="journal_path"></param>
		/// <param name="read_only"></param>
		/// <param name="max_pages"></param>
		/// <param name="page_size"></param>
		/// <param name="sda_factory"></param>
		/// <param name="enable_logging"></param>
		internal LoggingBufferManager(string journal_path, bool read_only,
									int max_pages, int page_size,
									IStoreDataAccessorFactory sda_factory, IDebugLogger logger, bool enable_logging) {
			PageCacheComparer = new PageCacheComparerImpl(this);
			this.max_pages = max_pages;
			this.page_size = page_size;

			check_point_in_progress = false;
			write_lock_count = 0;

			current_T = 0;
			page_list = new ArrayList();
			page_map = new BMPage[257];
			unique_id_seq = 0;

			journalled_system = new JournalledSystem(journal_path, read_only,
			page_size, sda_factory, logger, enable_logging);
		}

		/// <summary>
		/// Constructs the manager with a scattering store implementation that
		/// converts the resource to a file in the given path.
		/// </summary>
		/// <param name="resource_path"></param>
		/// <param name="journal_path"></param>
		/// <param name="read_only"></param>
		/// <param name="max_pages"></param>
		/// <param name="page_size"></param>
		/// <param name="file_ext"></param>
		/// <param name="max_slice_size"></param>
		/// <param name="enable_logging"></param>
		internal LoggingBufferManager(string resource_path, string journal_path, bool read_only, 
			int max_pages, int page_size, String file_ext, long max_slice_size, IDebugLogger logger, bool enable_logging)
			: this(journal_path, read_only, max_pages, page_size, 
			new StoreDataAccessorFactoryImpl(resource_path, file_ext, max_slice_size), logger, enable_logging) {
		}

		private class StoreDataAccessorFactoryImpl : IStoreDataAccessorFactory {
			private readonly string resource_path;
			private readonly string file_ext;
			private readonly long max_slice_size;

			public StoreDataAccessorFactoryImpl(string resourcePath, string fileExt, long maxSliceSize) {
				resource_path = resourcePath;
				max_slice_size = maxSliceSize;
				file_ext = fileExt;
			}

			public IStoreDataAccessor CreateStoreDataAccessor(String resource_name) {
				return new ScatteringStoreDataAccessor(resource_path, resource_name,
													   file_ext, max_slice_size);
			}
		}

		/// <summary>
		/// Starts the buffer manager.
		/// </summary>
		public void Start() {
			journalled_system.Start();
		}

		/// <summary>
		/// Stops the buffer manager.
		/// </summary>
		public void Stop() {
			journalled_system.Stop();
		}

		// ----------

		/// <summary>
		/// Creates a new resource.
		/// </summary>
		/// <param name="resource_name"></param>
		/// <returns></returns>
		internal IJournalledResource CreateResource(String resource_name) {
			return journalled_system.CreateResource(resource_name);
		}

		///<summary>
		/// Obtains a write Lock on the buffer.
		///</summary>
		/// <remarks>
		/// This will block if a <see cref="SetCheckPoint"/> is in progress, 
		/// otherwise it will always succeed.
		/// </remarks>
		public void LockForWrite() {
			lock (write_lock) {
				while (check_point_in_progress) {
					Monitor.Wait(write_lock);
				}
				++write_lock_count;
			}
		}

		///<summary>
		/// Releases a Write Lock on the buffer.
		///</summary>
		/// <remarks>
		/// This <b>must</b> be called if the <see cref="LockForWrite"/> method is 
		/// called. This should be called from a 'finally' clause.
		/// </remarks>
		public void UnlockForWrite() {
			lock (write_lock) {
				--write_lock_count;
				Monitor.PulseAll(write_lock);
			}
		}

		///<summary>
		/// Sets a check point in the log.
		///</summary>
		///<param name="flush_journals"></param>
		/// <remarks>
		/// This logs a point in which a recovery process should at least be able to 
		/// be rebuild back to. This will block if there are any write locks.
		/// <para>
		/// Some things to keep in mind when using this. You must ensure that no writes 
		/// can occur while this operation is occuring. Typically this will happen at the 
		/// end of a commit but you need to ensure that nothing can happen in the background, 
		/// such as records being deleted or items being inserted. It is required that the 
		/// 'no write' restriction is enforced at a high level. If care is not taken then the 
		/// image written will not be clean and if a crash occurs the image that is recovered 
		/// will not be stable.
		/// </para>
		/// </remarks>
		public void SetCheckPoint(bool flush_journals) {

			// Wait until the writes have finished, and then set the
			// 'check_point_in_progress' bool.
			lock (write_lock) {
				while (write_lock_count > 0) {
					Monitor.Wait(write_lock);
				}
				check_point_in_progress = true;
			}

			try {
				//      Console.Out.WriteLine("SET CHECKPOINT");
				lock (page_map) {
					// Flush all the pages out to the log.
					for (int i = 0; i < page_map.Length; ++i) {
						BMPage page = page_map[i];
						BMPage prev = null;

						while (page != null) {
							bool deleted_hash = false;
							lock (page) {
								// Flush the page (will only actually flush if there are changes)
								page.Flush();

								// Remove this page if it is no longer in use
								if (page.NotInUse) {
									deleted_hash = true;
									if (prev == null) {
										page_map[i] = page.hash_next;
									} else {
										prev.hash_next = page.hash_next;
									}
								}

							}
							// Go to next page in hash chain
							if (!deleted_hash) {
								prev = page;
							}
							page = page.hash_next;
						}
					}
				}

				journalled_system.SetCheckPoint(flush_journals);

			} finally {
				// Make sure we unset the 'check_point_in_progress' bool and notify
				// any blockers.
				lock (write_lock) {
					check_point_in_progress = false;
					Monitor.PulseAll(write_lock);
				}
			}

		}


		/// <summary>
		/// Called when a new page is created.
		/// </summary>
		/// <param name="page"></param>
		private void PageCreated(BMPage page) {
			lock (T_lock) {

				if (PARANOID_CHECKS) {
					int i = page_list.IndexOf(page);
					if (i != -1) {
						BMPage f = (BMPage)page_list[i];
						if (f == page) {
							throw new ApplicationException("Same page added multiple times.");
						}
						if (f != null) {
							throw new ApplicationException("Duplicate pages.");
						}
					}
				}

				page.t = current_T;
				++current_T;

				++current_page_count;
				page_list.Add(page);

				// Below is the page purge algorithm.  If the maximum number of pages
				// has been created we sort the page list weighting each page by time
				// since last accessed and total number of accesses and clear the bottom
				// 20% of this list.

				// Check if we should purge old pages and purge some if we do...
				if (current_page_count > max_pages) {
					// Purge 20% of the cache
					// Sort the pages by the current formula,
					//  ( 1 / page_access_count ) * (current_t - page_t)
					// Further, if the page has written data then we multiply by 0.75.
					// This scales down page writes so they have a better chance of
					// surviving in the cache than page writes.
					Object[] pages = page_list.ToArray();
					Array.Sort(pages, PageCacheComparer);

					int purge_size = System.Math.Max((int)(pages.Length * 0.20f), 2);
					for (int i = 0; i < purge_size; ++i) {
						BMPage dpage = (BMPage)pages[pages.Length - (i + 1)];
						lock (dpage) {
							dpage.Dispose();
						}
					}

					// Remove all the elements from page_list and set it with the sorted
					// list (minus the elements we removed).
					page_list.Clear();
					for (int i = 0; i < pages.Length - purge_size; ++i) {
						page_list.Add(pages[i]);
					}

					current_page_count -= purge_size;

				}
			}
		}

		/// <summary>
		/// Called when a page is accessed.
		/// </summary>
		/// <param name="page"></param>
		private void PageAccessed(BMPage page) {
			lock (T_lock) {
				page.t = current_T;
				++current_T;
				++page.access_count;
			}
		}

		/// <summary>
		/// Calculates a hash code given an id value and a page_number value.
		/// </summary>
		/// <param name="id"></param>
		/// <param name="page_number"></param>
		/// <returns></returns>
		private static int CalcHashCode(long id, long page_number) {
			return (int)((id << 6) + (page_number * ((id + 25) << 2)));
		}

		/// <summary>
		/// Fetches and returns a page from a store.
		/// </summary>
		/// <param name="data"></param>
		/// <param name="page_number"></param>
		/// <remarks>
		/// Pages may be cached.  If the page is not available in the cache then a new 
		/// <see cref="BMPage"/> object is created for the page requested.
		/// </remarks>
		/// <returns></returns>
		private BMPage FetchPage(IJournalledResource data, long page_number) {
			long id = data.Id;

			BMPage prev_page = null;
			bool new_page = false;
			BMPage page;

			lock (page_map) {
				// Generate the hash code for this page.
				int p = (CalcHashCode(id, page_number) & 0x07FFFFFFF) %
																	   page_map.Length;
				// Search for this page in the hash
				page = page_map[p];
				while (page != null && !page.IsPage(id, page_number)) {
					prev_page = page;
					page = page.hash_next;
				}

				// Page isn't found so create it and add to the cache
				if (page == null) {
					page = new BMPage(data, page_number, page_size);
					// Add this page to the map
					page.hash_next = page_map[p];
					page_map[p] = page;
				} else {
					// Move this page to the head if it's not already at the head.
					if (prev_page != null) {
						prev_page.hash_next = page.hash_next;
						page.hash_next = page_map[p];
						page_map[p] = page;
					}
				}

				lock (page) {
					// If page not in use then it must be newly setup, so add a
					// reference.
					if (page.NotInUse) {
						page.Reset();
						new_page = true;
						page.ReferenceAdd();
					}
					// Add a reference for this fetch
					page.ReferenceAdd();
				}

			}

			// If the page is new,
			if (new_page) {
				PageCreated(page);
			} else {
				PageAccessed(page);
			}

			// Return the page.
			return page;

		}


		// ------
		// Buffered access methods.  These are all thread safe methods.  When a page
		// is accessed the page is synchronized so no 2 or more operations can
		// Read/Write from the page at the same time.  An operation can Read/Write to
		// different pages at the same time, however, and this requires thread safety
		// at a lower level (in the IJournalledResource implementation).
		// ------

		internal int ReadByteFrom(IJournalledResource data, long position) {
			long page_number = position / page_size;
			int v;

			BMPage page = FetchPage(data, page_number);
			lock (page) {
				try {
					page.Initialize();
					v = ((int)page.Read((int)(position % page_size))) & 0x0FF;
				} finally {
					page.Dispose();
				}
			}

			return v;
		}

		internal int ReadByteArrayFrom(IJournalledResource data, long position, byte[] buf, int off, int len) {
			int orig_len = len;
			long page_number = position / page_size;
			int start_offset = (int)(position % page_size);
			int to_read = System.Math.Min(len, page_size - start_offset);

			BMPage page = FetchPage(data, page_number);
			lock (page) {
				try {
					page.Initialize();
					page.Read(start_offset, buf, off, to_read);
				} finally {
					page.Dispose();
				}
			}

			len -= to_read;
			while (len > 0) {
				off += to_read;
				position += to_read;
				++page_number;
				to_read = System.Math.Min(len, page_size);

				page = FetchPage(data, page_number);
				lock (page) {
					try {
						page.Initialize();
						page.Read(0, buf, off, to_read);
					} finally {
						page.Dispose();
					}
				}
				len -= to_read;
			}

			return orig_len;
		}

		internal void WriteByteTo(IJournalledResource data,
						 long position, int b) {

			if (PARANOID_CHECKS) {
				lock (write_lock) {
					if (write_lock_count == 0) {
						Console.Out.WriteLine("Write without a Lock!");
						Console.Out.WriteLine(new ApplicationException().StackTrace);
					}
				}
			}

			long page_number = position / page_size;

			BMPage page = FetchPage(data, page_number);
			lock (page) {
				try {
					page.Initialize();
					page.Write((int)(position % page_size), (byte)b);
				} finally {
					page.Dispose();
				}
			}
		}

		internal void WriteByteArrayTo(IJournalledResource data, long position, byte[] buf, int off, int len) {
			if (PARANOID_CHECKS) {
				lock (write_lock) {
					if (write_lock_count == 0) {
						Console.Out.WriteLine("Write without a Lock!");
						Console.Out.WriteLine(new ApplicationException().StackTrace);
					}
				}
			}

			long page_number = position / page_size;
			int start_offset = (int)(position % page_size);
			int to_write = System.Math.Min(len, page_size - start_offset);

			BMPage page = FetchPage(data, page_number);
			lock (page) {
				try {
					page.Initialize();
					page.Write(start_offset, buf, off, to_write);
				} finally {
					page.Dispose();
				}
			}
			len -= to_write;

			while (len > 0) {
				off += to_write;
				position += to_write;
				++page_number;
				to_write = System.Math.Min(len, page_size);

				page = FetchPage(data, page_number);
				lock (page) {
					try {
						page.Initialize();
						page.Write(0, buf, off, to_write);
					} finally {
						page.Dispose();
					}
				}
				len -= to_write;
			}

		}

		internal void SetDataAreaSize(IJournalledResource data, long new_size) {
			data.SetSize(new_size);
		}

		internal long GetDataAreaSize(IJournalledResource data) {
			return data.Size;
		}

		internal void Close(IJournalledResource data) {
			long id = data.Id;
			// Flush all changes made to the resource then close.
			lock (page_map) {
				//      Console.Out.WriteLine("Looking for id: " + id);
				// Flush all the pages out to the log.
				// This scans the entire hash for values and could be an expensive
				// operation.  Fortunately 'close' isn't used all that often.
				for (int i = 0; i < page_map.Length; ++i) {
					BMPage page = page_map[i];
					BMPage prev = null;

					while (page != null) {
						bool deleted_hash = false;
						if (page.Id == id) {
							//            Console.Out.WriteLine("Found page id: " + page.getID());
							lock (page) {
								// Flush the page (will only actually flush if there are changes)
								page.Flush();

								// Remove this page if it is no longer in use
								if (page.NotInUse) {
									deleted_hash = true;
									if (prev == null) {
										page_map[i] = page.hash_next;
									} else {
										prev.hash_next = page.hash_next;
									}
								}
							}

						}

						// Go to next page in hash chain
						if (!deleted_hash) {
							prev = page;
						}
						page = page.hash_next;

					}
				}
			}

			data.Close();
		}



		// ---------- Inner classes ----------


		/// <summary>
		/// A page from a store that is currently being cached in memory.
		/// </summary>
		/// <remarks>
		/// This is also an element in the cache.
		/// </remarks>
		private sealed class BMPage {
			/// <summary>
			/// The <see cref="IStoreDataAccessor"/> that the page content is part of.
			/// </summary>
			private readonly IJournalledResource data;

			/// <summary>
			/// The page number.
			/// </summary>
			private readonly long page;

			/// <summary>
			/// The size of the page.
			/// </summary>
			private readonly int page_size;


			/// <summary>
			/// The buffer that contains the data for this page.
			/// </summary>
			private byte[] buffer;

			/// <summary>
			/// True if this page is initialized.
			/// </summary>
			private bool initialized;

			/// <summary>
			///A reference to the next page with this hash key. 
			/// </summary>
			internal BMPage hash_next;

			/// <summary>
			/// The time this page was last accessed.
			/// </summary>
			/// <remarks>
			/// This value is reset each time the page is requested.
			/// </remarks>
			internal long t;

			/// <summary>
			/// The number of times this page has been accessed since it was created.
			/// </summary>
			internal int access_count;

			/// <summary>
			///The first position in the buffer that was last written. 
			/// </summary>
			private int first_write_position;

			/// <summary>
			///The last position in the buffer that was last written. 
			/// </summary>
			private int last_write_position;

			/// <summary>
			///The number of references on this page. 
			/// </summary>
			private int reference_count;


			/// <summary>
			///Constructs the page. 
			/// </summary>
			/// <param name="data"></param>
			/// <param name="page"></param>
			/// <param name="page_size"></param>
			internal BMPage(IJournalledResource data, long page, int page_size) {
				this.data = data;
				this.page = page;
				this.reference_count = 0;
				this.page_size = page_size;
				Reset();
			}

			/// <summary>
			///Resets this object. 
			/// </summary>
			internal void Reset() {
				// Assert that this is 0
				if (reference_count != 0) {
					throw new ApplicationException("reset when 'reference_count' is != 0 ( = " +
									reference_count + " )");
				}
				this.initialized = false;
				this.t = 0;
				this.access_count = 0;
			}

			/// <summary>
			/// Returns the id of the <see cref="IJournalledResource"/> that is being 
			/// buffered.
			/// </summary>
			internal long Id {
				get { return data.Id; }
			}

			/// <summary>
			/// Adds 1 to the reference counter on this page. 
			/// </summary>
			internal void ReferenceAdd() {
				++reference_count;
			}

			/// <summary>
			/// Removes 1 from the reference counter on this page.
			/// </summary>
			private void ReferenceRemove() {
				if (reference_count <= 0) {
					throw new ApplicationException("Too many reference remove.");
				}
				--reference_count;
			}

			/// <summary>
			/// Returns true if this <see cref="BMPage"/> is not in use (has 
			/// 0 reference count and is not inialized.  
			/// </summary>
			internal bool NotInUse {
				get { return reference_count == 0; }
				//      return (reference_count <= 0 && !initialized);
			}

			/// <summary>
			/// Returns true if this page matches the given id/page_number. 
			/// </summary>
			/// <param name="in_id"></param>
			/// <param name="in_page"></param>
			/// <returns>
			/// </returns>
			internal bool IsPage(long in_id, long in_page) {
				return (Id == in_id &&
						page == in_page);
			}

			/// <summary>
			/// Reads the current page content into memory.
			/// </summary>
			/// <param name="page_number"></param>
			/// <param name="buf"></param>
			/// <param name="pos"></param>
			/// <remarks>
			/// This may Read from the data files or from a log.
			/// </remarks>
			private void ReadPageContent(long page_number, byte[] buf, int pos) {
				if (pos != 0) {
					throw new ApplicationException("Assert failed: pos != 0");
				}
				// Read from the resource
				data.Read(page_number, buf, pos);
			}

			/// <summary>
			/// Flushes this page out to disk, but does not remove from memory.
			/// </summary>
			/// <remarks>
			/// In a logging system this will flush the changes out to a log.
			/// </remarks>
			internal void Flush() {
				if (initialized) {
					if (last_write_position > -1) {
						// Write to the store data.
						data.Write(page, buffer, first_write_position,
								   last_write_position - first_write_position);
						//          Console.Out.WriteLine("FLUSH " + data + " off = " + first_write_position +
						//                             " len = " + (last_write_position - first_write_position));
					}
					first_write_position = Int32.MaxValue;
					last_write_position = -1;
				}
			}

			/// <summary>
			/// Initializes the page buffer.
			/// </summary>
			/// <remarks>
			/// If the buffer is already initialized then we just return. If it's 
			/// not initialized we set up any internal structures that are required 
			/// to be set up for access to this page.
			/// </remarks>
			internal void Initialize() {
				if (!initialized) {
					try {

						// Create the buffer to contain the page in memory
						buffer = new byte[page_size];
						// Read the page.  This will either Read the page from the backing
						// store or from a log.
						ReadPageContent(page, buffer, 0);
						initialized = true;

						//          access_count = 0;
						first_write_position = Int32.MaxValue;
						last_write_position = -1;

					} catch (IOException e) {
						// This makes debugging a little clearer if 'ReadPageContent' fails.
						// When 'ReadPageContent' fails, the dispose method fails also.
						Console.Out.WriteLine("IO Error during page initialize: " + e.Message);
						Console.Out.WriteLine(e.StackTrace);
						throw e;
					}

				}
			}

			/// <summary>
			/// Disposes of the page buffer if it can be disposed (there are no references 
			/// to the page and the page is initialized).
			/// </summary>
			/// <remarks>
			/// When disposed the memory used by the page is reclaimed and the content is 
			/// written out to disk.
			/// </remarks>
			internal void Dispose() {
				ReferenceRemove();
				if (reference_count == 0) {
					if (initialized) {

						// Flushes the page from memory.  This will Write the page out to the
						// log.
						Flush();

						// Page is no longer initialized.
						initialized = false;
						// Clear the buffer from memory.
						buffer = null;

					} else {
						// This happens if initialization failed.  If this case we don't
						// flush out the changes, but we do allow the page to be disposed
						// in the normal way.
						// Note that any exception generated by the initialization failure
						// will propogate correctly.
						buffer = null;
						//          throw new RuntimeException(
						//                "Assertion failed: tried to dispose an uninitialized page.");
					}
				}
			}

			/// <summary>
			///Reads a single byte from the cached page from memory. 
			/// </summary>
			/// <param name="pos"></param>
			/// <returns>
			/// </returns>
			internal byte Read(int pos) {
				return buffer[pos];
			}

			/// <summary>
			/// Reads a part of this page into the cached page from memory. 
			/// </summary>
			/// <param name="pos"></param>
			/// <param name="buf"></param>
			/// <param name="off"></param>
			/// <param name="len"></param>
			internal void Read(int pos, byte[] buf, int off, int len) {
				Array.Copy(buffer, pos, buf, off, len);
			}

			/// <summary>
			/// Writes a single byte to the page in memory.
			/// </summary>
			/// <param name="pos"></param>
			/// <param name="v"></param>
			internal void Write(int pos, byte v) {
				first_write_position = System.Math.Min(pos, first_write_position);
				last_write_position = System.Math.Max(pos + 1, last_write_position);

				buffer[pos] = v;
			}

			/// <summary>
			/// Writes to the given part of the page in memory. 
			/// </summary>
			/// <param name="pos"></param>
			/// <param name="buf"></param>
			/// <param name="off"></param>
			/// <param name="len"></param>
			internal void Write(int pos, byte[] buf, int off, int len) {
				first_write_position = System.Math.Min(pos, first_write_position);
				last_write_position = System.Math.Max(pos + len, last_write_position);

				Array.Copy(buf, off, buffer, pos, len);
			}

			public override bool Equals(Object ob) {
				BMPage dest_page = (BMPage)ob;
				return IsPage(dest_page.Id, dest_page.page);
			}

			public override int GetHashCode() {
				return base.GetHashCode();
			}
		}

		/// <summary>
		/// A <see cref="IComparer"/> used to sort cache entries.
		/// </summary>
		private readonly IComparer PageCacheComparer;

		private class PageCacheComparerImpl : IComparer {
			private readonly LoggingBufferManager lbm;

			public PageCacheComparerImpl(LoggingBufferManager lbm) {
				this.lbm = lbm;
			}

			/// <summary>
			/// The calculation for finding the <i>weight</i> of a page in the cache.
			/// </summary>
			/// <param name="page"></param>
			/// <remarks>
			/// A heavier page is sorted lower and is therefore cleared from the 
			/// cache faster.
			/// </remarks>
			/// <returns></returns>
			private float pageEnumValue(BMPage page) {
				// We fix the access counter so it can not exceed 10000 accesses.  I'm
				// a little unsure if we should WriteByte this constant in the equation but it
				// ensures that some old but highly accessed page will not stay in the
				// cache forever.
				long bounded_page_count = System.Math.Min(page.access_count, 10000);
				float v = (1f / bounded_page_count) * (lbm.current_T - page.t);
				return v;
			}

			public int Compare(Object ob1, Object ob2) {
				float v1 = pageEnumValue((BMPage)ob1);
				float v2 = pageEnumValue((BMPage)ob2);
				if (v1 > v2) {
					return 1;
				} else if (v1 < v2) {
					return -1;
				}
				return 0;
			}
		}

		/// <summary>
		/// A factory interface for creating <see cref="IStoreDataAccessor"/> objects 
		/// from resource names. 
		/// </summary>
		internal interface IStoreDataAccessorFactory {

			/**
			 * Returns a IStoreDataAccessor object for the given resource name.
			 */
			IStoreDataAccessor CreateStoreDataAccessor(String resource_name);

		}
	}
}