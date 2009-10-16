//  
//  IOStoreDataAccessor.cs
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
using System.IO;

namespace Deveel.Data.Store {
	/// <summary>
	/// An implementation of <see cref="IStoreDataAccessor"/> that uses the 
	/// standard .NET I/O API to access data in some underlying file in the filesystem.
	/// </summary>
	class IOStoreDataAccessor : IStoreDataAccessor {
		/// <summary>
		/// A Lock because access to the data is stateful.
		/// </summary>
		private readonly Object l = new Object();

		/// <summary>
		/// The string path representing the file in the file system.
		/// </summary>
		private readonly string file;

		/// <summary>
		/// The underlying <see cref="FileStream"/> containing the data.
		/// </summary>
		private FileStream data;

		/// <summary>
		/// The size of the data area.
		/// </summary>
		private long size;

		/// <summary>
		/// True if the file is open.
		/// </summary>
		private bool is_open;

		internal IOStoreDataAccessor(string file) {
			this.file = file;
			this.is_open = false;
		}

		// ---------- Implemented from IStoreDataAccessor ----------

		public void Open(bool is_read_only) {
			lock (l) {
				data = new FileStream(file, FileMode.OpenOrCreate, is_read_only ? FileAccess.Read : FileAccess.ReadWrite);
				size = data.Length;
				is_open = true;
			}
		}

		public void Close() {
			lock (l) {
				data.Close();
				data = null;
				is_open = false;
			}
		}

		public bool Delete() {
			if (!is_open) {
				File.Delete(file);
				return !Exists;
			}
			return false;
		}

		public bool Exists {
			get { return File.Exists(file); }
		}


		public int Read(long position, byte[] buf, int off, int len) {
			// Make sure we don't Read past the end
			lock (l) {
				len = System.Math.Max(0, System.Math.Min(len, (int)(size - position)));
				int count = 0;
				if (position < size) {
					data.Seek(position, SeekOrigin.Begin);
					count = data.Read(buf, off, len);
				}
				return count;
			}
		}

		public void Write(long position, byte[] buf, int off, int len) {
			// Make sure we don't Write past the end
			lock (l) {
				len = System.Math.Max(0, System.Math.Min(len, (int)(size - position)));
				if (position < size) {
					data.Seek(position, SeekOrigin.Begin);
					data.Write(buf, off, len);
				}
			}
		}

		public void SetSize(long new_size) {
			lock (l) {
				// If expanding the size of the file,
				if (new_size > this.size) {
					// Seek to the new size - 1 and Write a single byte to the end of the
					// file.
					long p = new_size - 1;
					if (p > 0) {
						data.Seek(p, SeekOrigin.Begin);
						data.WriteByte(0);
						this.size = new_size;
					}
				} else if (new_size < this.size) {
					// Otherwise the size of the file is shrinking, so setLength().
					// Note that we don't use 'setLength' to grow the file because of a
					// bug in the Linux 1.2, 1.3 and 1.4 JVM that generates an error when
					// expanding the size of a file via 'setLength' on some file systems
					// (specifically VFAT).
					data.SetLength(new_size);
					this.size = new_size;
				}
			}
		}

		public long Size {
			get {
				lock (l) {
					if (is_open) {
						return size;
					} else {
						return new FileInfo(file).Length;
					}
				}
			}
		}

		public void Synch() {
			lock (l) {
				try {
					data.Flush();
					FSync.Sync(data);
				} catch (SyncFailedException) {
					// There isn't much we can do about this exception.  By itself it
					// doesn't indicate a terminating error so it's a good idea to ignore
					// it.  Should it be silently ignored?
				}
			}
		}

		public void Dispose() {
			if (data != null)
				data.Close();
		}
	}
}