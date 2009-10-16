//  
//  ScatteringStoreDataAccessor.cs
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
using System.Text;

namespace Deveel.Data.Store {
	/// <summary>
	/// An implementation of <see cref="IStoreDataAccessor"/> that scatters the 
	/// addressible data resource across multiple files in the file system.
	/// </summary>
	/// <remarks>
	/// When one store data resource reaches a certain threshold size, the content 
	/// <i>flows</i> over to the next file.
	/// </remarks>
	public class ScatteringStoreDataAccessor : IStoreDataAccessor {
		/// <summary>
		///The path of this store in the file system. 
		/// </summary>
		private readonly string path;

		/// <summary>
		/// The name of the file in the file system minus the extension.
		/// </summary>
		private readonly String file_name;

		/// <summary>
		/// The extension of the first file in the sliced set.
		/// </summary>
		private readonly String first_ext;

		/// <summary>
		/// The maximum size a file slice can grow too before a new slice is created. 
		/// </summary>
		private readonly long max_slice_size;

		/// <summary>
		/// The list of RandomAccessFile objects for each file that represents a
		/// slice of the store.  (FileSlice objects)
		/// </summary>
		private ArrayList slice_list;

		/// <summary>
		/// The current actual physical size of the store data on disk.
		/// </summary>
		private long true_file_length;

		/// <summary>
		/// A lock when modifying the true_data_size, and slice_list. 
		/// </summary>
		private readonly Object l = new Object();

		/// <summary>
		/// Set when the store is openned.
		/// </summary>
		private bool m_open = false;

		/// <summary>
		/// Constructs the store data accessor.
		/// </summary>
		/// <param name="path"></param>
		/// <param name="file_name"></param>
		/// <param name="first_ext"></param>
		/// <param name="max_slice_size"></param>
		public ScatteringStoreDataAccessor(string path, string file_name, string first_ext, long max_slice_size) {
			slice_list = new ArrayList();
			this.path = path;
			this.file_name = file_name;
			this.first_ext = first_ext;
			this.max_slice_size = max_slice_size;
		}

		/// <summary>
		/// Given a file, this will convert to a scattering file store with files 
		/// no larger than the maximum slice size.
		/// </summary>
		/// <param name="f"></param>
		public void ConvertToScatteringStore(string f) {
			const int BUFFER_SIZE = 65536;

			FileStream src = new FileStream(f, FileMode.OpenOrCreate, FileAccess.ReadWrite);
			long file_size = new FileInfo(f).Length;
			long current_p = max_slice_size;
			long to_write = System.Math.Min(file_size - current_p, max_slice_size);
			int write_to_part = 1;

			byte[] copy_buffer = new byte[BUFFER_SIZE];

			while (to_write > 0) {

				src.Seek(current_p, SeekOrigin.Begin);

				string to_f = SlicePartFile(write_to_part);
				if (File.Exists(to_f)) {
					throw new IOException("Copy error, slice already exists.");
				}
				FileStream to_raf = new FileStream(to_f, FileMode.OpenOrCreate, FileAccess.Write);

				while (to_write > 0) {
					int size_to_copy = (int)System.Math.Min(BUFFER_SIZE, to_write);

					src.Read(copy_buffer, 0, size_to_copy);
					to_raf.Write(copy_buffer, 0, size_to_copy);

					current_p += size_to_copy;
					to_write -= size_to_copy;
				}

				to_raf.Flush();
				to_raf.Close();

				to_write = System.Math.Min(file_size - current_p, max_slice_size);
				++write_to_part;
			}

			// Truncate the source file
			if (file_size > max_slice_size) {
				src.Seek(0, SeekOrigin.Begin);
				src.SetLength(max_slice_size);
			}
			src.Close();

		}

		/// <summary>
		/// Given an index value, this will returns the path to the nth slice 
		/// in the file system.
		/// </summary>
		/// <param name="i"></param>
		/// <remarks>
		/// For example, given '4' will return [file name].004, given 1004 will return 
		/// [file name].1004, etc.
		/// </remarks>
		/// <returns></returns>
		private string SlicePartFile(int i) {
			if (i == 0) {
				return Path.Combine(path, file_name + "." + first_ext);
			}
			StringBuilder fn = new StringBuilder();
			fn.Append(file_name);
			fn.Append(".");
			if (i < 10) {
				fn.Append("00");
			} else if (i < 100) {
				fn.Append("0");
			}
			fn.Append(i);
			return Path.Combine(path, fn.ToString());
		}

		/// <summary>
		/// Counts the number of files in the file store that represent this store.
		/// </summary>
		private int StoreFilesCount {
			get {
				int i = 0;
				string f = SlicePartFile (i);
				while (File.Exists (f)) {
					++i;
					f = SlicePartFile (i);
				}
				return i;
			}
		}

		/// <summary>
		/// Creates a <see cref="IStoreDataAccessor"/> object for accessing a given slice.
		/// </summary>
		/// <param name="file"></param>
		/// <returns>
		/// </returns>
		private IStoreDataAccessor CreateSliceDataAccessor(string file) {
			// Currently we only support an IOStoreDataAccessor object.
			return new IOStoreDataAccessor(file);
		}

		/// <summary>
		/// Discovers the size of the data resource (doesn't require the file 
		/// to be open). 
		/// </summary>
		/// <returns></returns>
		private long DiscoverSize() {
			long running_total = 0;

			lock (l) {
				// Does the file exist?
				int i = 0;
				string f = SlicePartFile(i);
				while (File.Exists(f)) {
					running_total += CreateSliceDataAccessor(f).Size;

					++i;
					f = SlicePartFile(i);
				}
			}

			return running_total;
		}

		// ---------- Implemented from IStoreDataAccessor ----------

		public void Open(bool read_only) {
			long running_length;

			lock (l) {
				slice_list = new ArrayList();

				// Does the file exist?
				string f = SlicePartFile(0);
				bool open_existing = File.Exists(f);

				// If the file already exceeds the threshold and there isn't a secondary
				// file then we need to convert the file.
				if (open_existing && f.Length > max_slice_size) {
					string f2 = SlicePartFile(1);
					if (File.Exists(f2)) {
						throw new IOException("File length exceeds maximum slice size setting.");
					}
					// We need to scatter the file.
					if (!read_only) {
						ConvertToScatteringStore(f);
					} else {
						throw new IOException("Unable to convert to a scattered store because Read-only.");
					}
				}

				// Setup the first file slice
				FileSlice slice = new FileSlice();
				slice.data = CreateSliceDataAccessor(f);
				slice.data.Open(read_only);

				slice_list.Add(slice);
				running_length = slice.data.Size;

				// If we are opening a store that exists already, there may be other
				// slices we need to setup.
				if (open_existing) {
					int i = 1;
					string slice_part = SlicePartFile(i);
					while (File.Exists(slice_part)) {
						// Create the new slice information for this part of the file.
						slice = new FileSlice();
						slice.data = CreateSliceDataAccessor(slice_part);
						slice.data.Open(read_only);

						slice_list.Add(slice);
						running_length += slice.data.Size;

						++i;
						slice_part = SlicePartFile(i);
					}
				}

				true_file_length = running_length;

				m_open = true;
			}
		}

		public void Close() {
			lock (l) {
				int sz = slice_list.Count;
				for (int i = 0; i < sz; ++i) {
					FileSlice slice = (FileSlice)slice_list[i];
					slice.data.Close();
				}
				slice_list = null;
				m_open = false;
			}
		}

		public bool Delete() {
			// The number of files
			int count_files = StoreFilesCount;
			// Delete each file from back to front
			for (int i = count_files - 1; i >= 0; --i) {
				string f = SlicePartFile(i);
				bool delete_success = CreateSliceDataAccessor(f).Delete();
				if (!delete_success) {
					return false;
				}
			}
			return true;
		}

		public bool Exists {
			get { return File.Exists(SlicePartFile(0)); }
		}


		public int Read(long position, byte[] buf, int off, int len) {
			// Reads the array (potentially across multiple slices).
			int count = 0;
			while (len > 0) {
				int file_i = (int)(position / max_slice_size);
				long file_p = (position % max_slice_size);
				int file_len = (int)System.Math.Min((long)len, max_slice_size - file_p);

				FileSlice slice;
				lock (l) {
					// Return if out of bounds.
					if (file_i < 0 || file_i >= slice_list.Count) {
						// Error if not open
						if (!m_open) {
							throw new IOException("Store not open.");
						}
						return 0;
					}
					slice = (FileSlice)slice_list[file_i];
				}

				int read_count =slice.data.Read(file_p, buf, off, file_len);
				if (read_count == 0)
					break;

				position += read_count;
				off += read_count;
				len -= read_count;
				count += read_count;
			}

			return count;
		}

		public void Write(long position, byte[] buf, int off, int len) {
			// Writes the array (potentially across multiple slices).
			while (len > 0) {
				int file_i = (int)(position / max_slice_size);
				long file_p = (position % max_slice_size);
				int file_len = (int)System.Math.Min((long)len, max_slice_size - file_p);

				FileSlice slice;
				lock (l) {
					// Return if out of bounds.
					if (file_i < 0 || file_i >= slice_list.Count) {
						if (!m_open) {
							throw new IOException("Store not open.");
						}
						return;
					}
					slice = (FileSlice)slice_list[file_i];
				}
				slice.data.Write(file_p, buf, off, file_len);

				position += file_len;
				off += file_len;
				len -= file_len;
			}
		}

		public void SetSize(long length) {
			lock (l) {
				// The size we need to grow the data area
				long total_size_to_grow = length - true_file_length;
				// Assert that we aren't shrinking the data area size.
				if (total_size_to_grow < 0) {
					throw new IOException("Unable to make the data area size " +
										  "smaller for this type of store.");
				}

				while (total_size_to_grow > 0) {
					// Grow the last slice by this size
					int last = slice_list.Count - 1;
					FileSlice slice = (FileSlice)slice_list[last];
					long old_slice_length = slice.data.Size;
					long to_grow = System.Math.Min(total_size_to_grow,
											(max_slice_size - old_slice_length));

					// Flush the buffer and set the length of the file
					slice.data.SetSize(old_slice_length + to_grow);
					// Synchronize the file change.  XP appears to defer a file size change
					// and it can result in errors if the JVM is terminated.
					slice.data.Synch();

					total_size_to_grow -= to_grow;
					// Create a new empty slice if we need to extend the data area
					if (total_size_to_grow > 0) {
						string slice_file = SlicePartFile(last + 1);

						slice = new FileSlice();
						slice.data = CreateSliceDataAccessor(slice_file);
						slice.data.Open(false);

						slice_list.Add(slice);
					}
				}
				true_file_length = length;
			}

		}

		public long Size {
			get {
				lock (l) {
					if (m_open) {
						return true_file_length;
					} else {
						return DiscoverSize();
					}
				}
			}
		}

		public void Synch() {
			lock (l) {
				int sz = slice_list.Count;
				for (int i = 0; i < sz; ++i) {
					FileSlice slice = (FileSlice)slice_list[i];
					slice.data.Synch();
				}
			}
		}

		// ---------- Inner classes ----------

		/// <summary>
		/// An object that contains information about a file slice.
		/// </summary>
		/// <remarks>
		/// The information includes the name of the file, the <see cref="FileStream"/>
		/// that represents the slice, and the size of the file.
		/// </remarks>
		private sealed class FileSlice {
			internal IStoreDataAccessor data;

		}

		public void Dispose() {
			int sz = slice_list.Count;
			if (sz > 0) {
				for (int i = 0; i < sz; i++) {
					FileSlice slice = (FileSlice) slice_list[i];
					slice.data.Dispose();
				}
			}
		}
	}
}