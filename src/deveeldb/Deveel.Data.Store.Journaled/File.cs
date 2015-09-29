using System;
using System.IO;

namespace Deveel.Data.Store.Journaled {
	public sealed class File : IDisposable {
		private readonly bool createdHandle;
		private long endPosition;

		public File(IFileHandle fileHandle) {
			if (fileHandle == null)
				throw new ArgumentNullException("fileHandle");

			FileHandle = fileHandle;

			endPosition = FileHandle.Length;
			FileStream = new AccessStream(this);
		}

		public File(JournalingSystem system, string fileName, bool readOnly)
			: this(CreateHandle(system, fileName, readOnly)) {
			createdHandle = true;
		}

		~File() {
			Dispose(false);
		}

		public IFileHandle FileHandle { get; private set; }

		public string FileName {
			get { return FileHandle.FileName; }
		}

		public long Length {
			get {
				lock (FileHandle) {
					return endPosition;
				}
			}
		}

		public Stream FileStream { get; private set; }

		private static IFileHandle CreateHandle(JournalingSystem system, string fileName, bool readOnly) {
			return system.FileHandleFactory.CreateHandle(fileName, readOnly);
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

					if (createdHandle && FileHandle != null)
						FileHandle.Dispose();
				} catch (Exception) {
				}
			}

			FileHandle = null;
			FileStream = null;
		}

		public void Close() {
			lock (FileHandle) {
				FileHandle.Close();
			}
		}

		public void Sync() {
			lock (FileHandle) {
				try {
					FileHandle.Flush(true);
				} catch (Exception) {
				}
			}
		}

		public void Delete() {
			lock (FileHandle) {
				FileHandle.Delete();
			}
		}

		public void Read(long position, byte[] buffer, int offset, int length) {
			lock (FileHandle) {
				FileHandle.Seek(position, SeekOrigin.Begin);
				
				int toRead = length;
				while (toRead > 0) {
					int read = FileHandle.Read(buffer, offset, toRead);
					toRead -= read;
					offset += read;
				}
			}
		}

		#region AccessStream

		class AccessStream : Stream {
			private File file;
			private long fp;

			public AccessStream(File file) {
				this.file = file;
			}

			public override void Flush() {
				lock (file.FileHandle) {
					file.FileHandle.Flush(false);
				}
			}

			public override long Seek(long offset, SeekOrigin origin) {
				lock (file.FileHandle) {
					long fileLen = file.endPosition;
					if (origin == SeekOrigin.Begin && offset > fileLen)
						return fp;

					if (origin == SeekOrigin.Current && fp + offset > fileLen)
						return fp;

					fp = file.FileHandle.Seek(offset, origin);
					return fp;
				}
			}

			public override void SetLength(long value) {
				lock (file.FileHandle) {
					file.FileHandle.SetLength(value);
					file.endPosition = value;
				}
			}

			public override int Read(byte[] buffer, int offset, int count) {
				lock (file.FileHandle) {
					count = (int)System.Math.Min(count, file.endPosition - fp);
					if (count <= 0)
						return 0;

					fp = file.FileHandle.Seek(fp, SeekOrigin.Begin);
					int readCount = file.FileHandle.Read(buffer, offset, count);
					fp += readCount;
					return readCount;
				}
			}

			public override void Write(byte[] buffer, int offset, int count) {
				if (count > 0) {
					lock (file.FileHandle) {
						file.FileHandle.Seek(file.endPosition, SeekOrigin.Begin);
						file.FileHandle.Write(buffer, offset, count);
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
				get { return !file.FileHandle.IsReadOnly; }
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
