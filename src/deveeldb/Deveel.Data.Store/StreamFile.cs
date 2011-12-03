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
		private long endPointer;

		/// <summary>
		/// The <see cref="Stream"/> object where to write and read the data
		/// of this file.
		/// </summary>
		private Stream fsstream;

		/// <summary>
		/// Constructor.
		/// </summary>
		/// <param name="file"></param>
		/// <param name="mode"></param>
		public StreamFile(string file, FileAccess mode) {
			this.file = file;
			data = new FileStream(file, FileMode.OpenOrCreate, mode, FileShare.Read, 1024, FileOptions.WriteThrough);
			endPointer = data.Length;
			fsstream = new SFFileStream(this);
		}

		~StreamFile() {
			Dispose(false);
		}

		private void Dispose(bool disposing) {
			if (disposing) {
				if (fsstream != null) {
					Synch();
					fsstream.Dispose();
					fsstream = null;
				}
			}
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
				} catch (IOException) {
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
				int toRead = len;
				while (toRead > 0) {
					int read = data.Read(buf, off, toRead);
					toRead -= read;
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
					return endPointer;
				}
			}
		}

		public Stream FileStream {
			get { return fsstream; }
		}

		public void Dispose() {
			Dispose(true);
			GC.SuppressFinalize(this);
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
					long fileLen = file.endPointer;
					if (origin == SeekOrigin.Begin && offset > fileLen)
						return fp;
					if (origin == SeekOrigin.Current && fp + offset > fileLen)
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
					count = (int)System.Math.Min(count, file.endPointer - fp);
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
						file.data.Seek(file.endPointer, SeekOrigin.Begin);
						file.data.Write(buffer, offset, count);
						file.endPointer += count;
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
				get { return file.endPointer; }
			}

			public override long Position {
				get { return fp; }
				set {
					//TODO: check if this is correct...
					if (value > file.endPointer)
						throw new ArgumentOutOfRangeException("value");

					fp = Seek(value, SeekOrigin.Begin);
				}
			}
		}
	}
}