// 
//  Copyright 2010-2015 Deveel
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
	public sealed class JournalFile : IDisposable {
		private readonly bool ownsFile;
		private long endPosition;

		public JournalFile(IFile file) {
			if (file == null)
				throw new ArgumentNullException("file");

			File = file;

			endPosition = File.Length;
			FileStream = new AccessStream(this);
		}

		public JournalFile(JournalingSystem system, string fileName, bool readOnly)
			: this(CreateHandle(system, fileName, readOnly)) {
			ownsFile = true;
		}

		~JournalFile() {
			Dispose(false);
		}

		public IFile File { get; private set; }

		public string FileName {
			get { return File.FileName; }
		}

		public long Length {
			get {
				lock (File) {
					return endPosition;
				}
			}
		}

		public Stream FileStream { get; private set; }

		private static IFile CreateHandle(JournalingSystem system, string fileName, bool readOnly) {
			if (!system.FileSystem.FileExists(fileName)) {
				return system.FileSystem.CreateFile(fileName);
			}

			return system.FileSystem.OpenFile(fileName, readOnly);
		}

		public void Dispose() {
			Dispose(true);
			GC.SuppressFinalize(this);
		}

		private void Dispose(bool disposing) {
			if (disposing) {
				try {
					Sync();

					if (FileStream != null)
						FileStream.Dispose();

					if (ownsFile && File != null)
						File.Dispose();
				} catch (Exception) {
				}
			}

			File = null;
			FileStream = null;
		}

		public void Close() {
			lock (File) {
				File.Close();
			}
		}

		public void Sync() {
			lock (File) {
				try {
					File.Flush(true);
				} catch (Exception) {
				}
			}
		}

		public void Delete() {
			lock (File) {
				File.Delete();
			}
		}

		public void Read(long position, byte[] buffer, int offset, int length) {
			lock (File) {
				File.Seek(position, SeekOrigin.Begin);
				
				int toRead = length;
				while (toRead > 0) {
					int read = File.Read(buffer, offset, toRead);
					toRead -= read;
					offset += read;
				}
			}
		}

		#region AccessStream

		class AccessStream : Stream {
			private JournalFile file;
			private long fp;

			public AccessStream(JournalFile file) {
				this.file = file;
			}

			public override void Flush() {
				lock (file.File) {
					file.File.Flush(false);
				}
			}

			public override long Seek(long offset, SeekOrigin origin) {
				lock (file.File) {
					long fileLen = file.endPosition;
					if (origin == SeekOrigin.Begin && offset > fileLen)
						return fp;

					if (origin == SeekOrigin.Current && fp + offset > fileLen)
						return fp;

					fp = file.File.Seek(offset, origin);
					return fp;
				}
			}

			public override void SetLength(long value) {
				lock (file.File) {
					file.File.SetLength(value);
					file.endPosition = value;
				}
			}

			public override int Read(byte[] buffer, int offset, int count) {
				lock (file.File) {
					count = (int)System.Math.Min(count, file.endPosition - fp);
					if (count <= 0)
						return 0;

					fp = file.File.Seek(fp, SeekOrigin.Begin);
					int readCount = file.File.Read(buffer, offset, count);
					fp += readCount;
					return readCount;
				}
			}

			public override void Write(byte[] buffer, int offset, int count) {
				if (count > 0) {
					lock (file.File) {
						file.File.Seek(file.endPosition, SeekOrigin.Begin);
						file.File.Write(buffer, offset, count);
						file.endPosition += count;
						fp += count;
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
				get { return !file.File.IsReadOnly; }
			}

			public override long Length {
				get { return file.endPosition; }
			}

			public override long Position {
				get { return fp; }
				set {
					//TODO: check if this is correct...
					if (value > file.endPosition)
						throw new ArgumentOutOfRangeException("value");

					fp = Seek(value, SeekOrigin.Begin);
				}
			}

			protected override void Dispose(bool disposing) {
				file = null;
				base.Dispose(disposing);
			}
		}

		#endregion
	}
}
