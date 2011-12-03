// 
//  Copyright 2010  Deveel
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
		private bool isOpen;

		internal IOStoreDataAccessor(string file) {
			this.file = file;
			isOpen = false;
		}

		~IOStoreDataAccessor() {
			Dispose(false);
		}

		// ---------- Implemented from IStoreDataAccessor ----------

		public void Open(bool readOnly) {
			lock (l) {
				data = new FileStream(file, FileMode.OpenOrCreate, readOnly ? FileAccess.Read : FileAccess.ReadWrite, FileShare.ReadWrite, 1024, FileOptions.WriteThrough);
				size = data.Length;
				isOpen = true;
			}
		}

		public void Close() {
			lock (l) {
				data.Close();
				data = null;
				isOpen = false;
			}
		}

		public bool Delete() {
			if (!isOpen) {
				File.Delete(file);
				return !Exists;
			}
			return false;
		}

		public bool Exists {
			get { return File.Exists(file); }
		}

		private void Dispose(bool disposing) {
			if (disposing) {
				if (data != null) {
					data.Dispose();
					data = null;
				}
			}
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

		public void SetSize(long newSize) {
			lock (l) {
				// If expanding the size of the file,
				if (newSize > size) {
					// Seek to the new size - 1 and Write a single byte to the end of the
					// file.
					long p = newSize - 1;
					if (p > 0) {
						data.Seek(p, SeekOrigin.Begin);
						data.WriteByte(0);
						size = newSize;
					}
				} else if (newSize < size) {
					data.SetLength(newSize);
					size = newSize;
				}
			}
		}

		public long Size {
			get {
				lock (l) {
					return isOpen ? size : new FileInfo(file).Length;
				}
			}
		}

		public void Synch() {
			lock (l) {
				try {
					data.Flush();
				} catch (IOException) {
					// There isn't much we can do about this exception.  By itself it
					// doesn't indicate a terminating error so it's a good idea to ignore
					// it.  Should it be silently ignored?
				}
			}
		}

		public void Dispose() {
			Dispose(true);
			GC.SuppressFinalize(this);
		}
	}
}