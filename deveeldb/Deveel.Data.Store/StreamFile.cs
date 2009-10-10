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

using Deveel.Data.Util;

namespace Deveel.Data.Store {
	/// <summary>
	/// A class used to write and read from/to a file on the underlying
	/// filesystem.
	/// </summary>
	public class StreamFile {
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
		/// The <see cref="Stream"/> object where to write the data of 
		/// this file.
		/// </summary>
		private Stream output_stream;

		/// <summary>
		/// Constructor.
		/// </summary>
		/// <param name="file"></param>
		/// <param name="mode"></param>
		public StreamFile(string file, FileAccess mode) {
			this.file = file;
			data = new FileStream(file, FileMode.OpenOrCreate, mode);
			end_pointer = data.Length;
			output_stream = new SFOutputStream(this);
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
		public void readFully(long position, byte[] buf, int off, int len) {
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

		/// <summary>
		/// Opens a <see cref="Stream"/> used to write to the file.
		/// </summary>
		/// <remarks>
		/// Only one output stream may be open on the file at once.
		/// </remarks>
		/// <returns></returns>
		public Stream GetOutputStream () {
			return output_stream;
		}
		
		/// <summary>
		/// Returns a <see cref="Stream"/> that allows us to read from the start to 
		/// the end of the file.
		/// </summary>
		/// <returns></returns>
		public Stream GetInputStream() {
			return new SFInputStream(this);
		}

		// ---------- Inner classes ----------



		class SFOutputStream : Stream {
			public SFOutputStream(StreamFile file) {
				this.file = file;
			}

			private readonly StreamFile file;

			public override void WriteByte(byte i) {
				lock (file.data) {
					file.data.Seek(file.end_pointer, SeekOrigin.Begin);
					file.data.WriteByte(i);
					++file.end_pointer;
				}
			}

			public override bool CanRead {
				get { return false; }
			}

			public override bool CanSeek {
				get { return true; }
			}

			public override bool CanWrite {
				get { return true; }
			}

			public override long Length {
				get { return file.Length; }
			}

			public override long Position {
				get { return file.data.Position; }
				set { file.data.Position = value; }
			}

			public override void Flush() {
			}

			public override long Seek(long offset, SeekOrigin origin) {
				return file.data.Seek(offset, origin);
			}

			public override void SetLength(long value) {
				throw new NotSupportedException();
			}

			public override int Read(byte[] buffer, int offset, int count) {
				throw new NotSupportedException();
			}

			public override void Write(byte[] buf, int off, int len) {
				if (len > 0) {
					lock (file.data) {
						file.data.Seek(file.end_pointer, SeekOrigin.Begin);
						file.data.Write(buf, off, len);
						file.end_pointer += len;
					}
				}
			}
		}



		class SFInputStream : InputStream {
			public SFInputStream(StreamFile file) {
				this.file = file;
			}

			private readonly StreamFile file;
			private long fp = 0;

			public override int ReadByte() {
				lock (file.data) {
					if (fp >= file.end_pointer)
						return 0;

					file.data.Seek(fp, SeekOrigin.Begin);
					++fp;
					return file.data.ReadByte();
				}
			}

			public override bool CanSeek {
				get { return true; }
			}

			public override long Length {
				get { return file.data.Length; }
			}

			public override long Position {
				get { return fp; }
				set { fp = value; }
			}

			public override long Seek(long offset, SeekOrigin origin) {
				lock (file.data) {
					if (origin == SeekOrigin.End)
						throw new NotSupportedException();
					if (offset < 0)
						throw new NotSupportedException();

					if (origin == SeekOrigin.Begin)
						fp = offset;
					else
						fp += offset;
					return fp;
				}
			}

			public override void SetLength(long value) {
				throw new NotSupportedException();
			}

			public override int Read(byte[] buf, int off, int len) {
				lock (file.data) {
					len = (int)System.Math.Min((long)len, file.end_pointer - fp);
					if (len <= 0) {
						return 0;
					}

					file.data.Seek(fp, SeekOrigin.Begin);
					int act_read = file.data.Read(buf, off, len);
					fp += act_read;
					return act_read;
				}
			}

			public override long Skip(long v) {
				lock (file.data) {
					fp += v;
				}
				return v;
			}
		}
	}
}