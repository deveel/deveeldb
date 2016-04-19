// 
//  Copyright 2010-2016 Deveel
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
//


using System;
using System.IO;

namespace Deveel.Data.Store.Journaled {
	class StreamFile : IDisposable {
		private bool ownsFile;
		private IFile file;

		private long endPointer;
		private Stream fileStream;

		public StreamFile(IFileSystem fileSystem, string path, bool readOnly) {
			if (fileSystem.FileExists(path)) {
				file = fileSystem.OpenFile(path, readOnly);
			} else {
				file = fileSystem.CreateFile(path);
			}

			endPointer = file.Length;
			fileStream = new StreamFileStream(this);

			ownsFile = true;
		}

		~StreamFile() {
			Dispose(false);
		}

		public long Length {
			get {
				lock (file) {
					return endPointer;
				}
			}
		}

		public Stream FileStream {
			get { return fileStream; }
		}


		public void Dispose() {
			Dispose(true);
			GC.SuppressFinalize(this);
		}

		private void Dispose(bool disposing) {
			if (disposing) {
				if (fileStream != null) {
					fileStream.Dispose();
				}

				if (ownsFile)
					file.Dispose();
			}

			fileStream = null;
			file = null;
		}

		public void Close() {
			lock (file) {
				if (!dataSynched)
					Synch();

				file.Close();
			}
		}

		private bool dataSynched;

		public void Synch() {
			lock (file) {
				try {
					file.Flush(true);
				} catch (IOException) {
				} finally {
					dataSynched = true;
				}
			}
		}

		public void Delete() {
			lock (file) {
				file.Delete();
			}
		}

		public void Read(long position, byte[] buf, int off, int len) {
			lock (file) {
				file.Seek(position, SeekOrigin.Begin);
				int toRead = len;
				while (toRead > 0) {
					int read = file.Read(buf, off, toRead);
					toRead -= read;
					off += read;
				}
			}
		}


		#region StreamFileStream

		class StreamFileStream : Stream {
			private readonly StreamFile file;
			private long pointer;

			public StreamFileStream(StreamFile file) {
				this.file = file;
			}

			public override void Flush() {
			}

			public override long Seek(long offset, SeekOrigin origin) {
				lock (file.file) {
					long fileLen = file.endPointer;
					if (origin == SeekOrigin.Begin && offset > fileLen)
						return pointer;
					if (origin == SeekOrigin.Current && pointer + offset > fileLen)
						return pointer;

					pointer = file.file.Seek(offset, origin);
					return pointer;
				}
			}

			public override void SetLength(long value) {
				throw new NotImplementedException();
			}

			public override int Read(byte[] buffer, int offset, int count) {
				lock (file.file) {
					count = (int)System.Math.Min(count, file.endPointer - pointer);
					if (count <= 0)
						return 0;

					pointer = file.file.Seek(pointer, SeekOrigin.Begin);
					int actRead = file.file.Read(buffer, offset, count);
					pointer += actRead;
					return actRead;
				}
			}

			public override void Write(byte[] buffer, int offset, int count) {
				if (count > 0) {
					lock (file.file) {
						file.file.Seek(file.endPointer, SeekOrigin.Begin);
						file.file.Write(buffer, offset, count);
						file.endPointer += count;
						pointer += count;
					}
				}
			}

			public override bool CanRead {
				get { return true; }
			}

			public override bool CanSeek {
				get { return true; }
			}

			public override bool CanWrite {
				get { return !file.file.IsReadOnly; }
			}

			public override long Length {
				get { return file.endPointer; }
			}

			public override long Position {
				get { return pointer; }
				set {
					//TODO: check if this is correct...
					if (value > file.endPointer)
						throw new ArgumentOutOfRangeException("value");

					pointer = Seek(value, SeekOrigin.Begin);
				}
			}
		}

		#endregion
	}
}
