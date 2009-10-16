//  
//  StreamFile.cs
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
	/// A class used to write and read from/to a file on the underlying
	/// filesystem.
	/// </summary>
	public class StreamFile : IDisposable {
		/// <summary>
		/// The path to the file object. 
		/// </summary>
		private readonly string file;

		/// <summary>
		/// The <see cref="FileStream"/>.
		/// </summary>
		private FileStream data;

		/// <summary>
		/// Pointer to the end of the file. 
		/// </summary>
		private long end_pointer;

		/// <summary>
		/// The <see cref="Stream"/> object where to write and read the data
		/// of this file.
		/// </summary>
		private readonly Stream fsstream;

		/// <summary>
		/// Constructor.
		/// </summary>
		/// <param name="file"></param>
		/// <param name="mode"></param>
		public StreamFile(string file, FileAccess mode) {
			this.file = file;
			data = new FileStream(file, FileMode.OpenOrCreate, mode);
			end_pointer = data.Length;
			fsstream = new SFFileStream(this);
		}

		/// <summary>
		/// Closes the file.
		/// </summary>
		public void Close() {
			lock (data) {
				data.Close();
			}
		}

		/// <summary>
		/// Synchs the file.
		/// </summary>
		public void Synch() {
			lock (data) {
				try {
					data.Flush();
					FSync.Sync(data);
				} catch (SyncFailedException) {
					// We ignore the exception which reduces the robustness
					// of the journal file for the OS where this problem occurs.
					// Unfortunately there's no sane way to handle this excption when it
					// does occur.
				}
			}
		}

		/// <summary>
		/// Deletes the file.
		/// </summary>
		public void Delete() {
			File.Delete(file);
		}

		/// <summary>
		/// Fully reads a block from a section of the file into the given 
		/// byte array at the given position.
		/// </summary>
		/// <param name="position"></param>
		/// <param name="buf"></param>
		/// <param name="off"></param>
		/// <param name="len"></param>
		public void Read(long position, byte[] buf, int off, int len) {
			lock (data) {
				data.Seek(position, SeekOrigin.Begin);
				int to_read = len;
				while (to_read > 0) {
					int read = data.Read(buf, off, to_read);
					to_read -= read;
					off += read;
				}
			}
		}

		/// <summary>
		/// Returns the current length of the data.
		/// </summary>
		public long Length {
			get {
				lock (data) {
					return end_pointer;
				}
			}
		}

		public Stream FileStream {
			get { return fsstream; }
		}

		public void Dispose() {
			if (fsstream != null) {
				Synch();
				fsstream.Dispose();
			}
		}

		// ---------- Inner classes ----------

		private class SFFileStream : Stream {
			public SFFileStream(StreamFile file) {
				this.file = file;
			}

			private readonly StreamFile file;
			private long fp = 0;

			public override void Flush() {
			}

			public override long Seek(long offset, SeekOrigin origin) {
				lock (file.data) {
					long file_len = file.end_pointer;
					if (origin == SeekOrigin.Begin && offset > file_len)
						return fp;
					if (origin == SeekOrigin.Current && fp + offset > file_len)
						return fp;
					
					fp = file.data.Seek(offset, origin);
					return fp;
				}
			}

			public override void SetLength(long value) {
				//TODO: implement?
				throw new NotSupportedException();
			}

			public override int Read(byte[] buffer, int offset, int count) {
				lock (file.data) {
					count = (int)System.Math.Min(count, file.end_pointer - fp);
					if (count <= 0)
						return 0;

					fp = file.data.Seek(fp, SeekOrigin.Begin);
					int act_read = file.data.Read(buffer, offset, count);
					fp += act_read;
					return act_read;
				}
			}

			public override void Write(byte[] buffer, int offset, int count) {
				if (count > 0) {
					lock (file.data) {
						file.data.Seek(file.end_pointer, SeekOrigin.Begin);
						file.data.Write(buffer, offset, count);
						file.end_pointer += count;
						fp += count;
					}
				}
			}

			public override bool CanRead {
				get { return file.data.CanRead; }
			}

			public override bool CanSeek {
				get { return file.data.CanSeek; }
			}

			public override bool CanWrite {
				get { return file.data.CanWrite; }
			}

			public override long Length {
				get { return file.end_pointer; }
			}

			public override long Position {
				get { return fp; }
				set {
					//TODO: check if this is correct...
					if (value > file.end_pointer)
						throw new ArgumentOutOfRangeException("value");

					fp = Seek(value, SeekOrigin.Begin);
				}
			}
		}
	}
}