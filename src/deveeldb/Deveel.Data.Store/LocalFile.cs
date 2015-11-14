using System;
using System.IO;

namespace Deveel.Data.Store {
	public class LocalFile : IFile {
		private FileStream fileStream;

		public void Dispose() {
			
		}

		public string FileName { get; }

		public bool IsReadOnly { get; }

		public long Position { get; }

		public long Length { get; }

		public bool Exists { get; }

		public long Seek(long offset, SeekOrigin origin) {
			throw new NotImplementedException();
		}

		public void SetLength(long value) {
			throw new NotImplementedException();
		}

		public int Read(byte[] buffer, int offset, int length) {
			throw new NotImplementedException();
		}

		public void Write(byte[] buffer, int offset, int length) {
			throw new NotImplementedException();
		}

		public void Flush(bool writeThrough) {
			throw new NotImplementedException();
		}

		public void Close() {
			throw new NotImplementedException();
		}

		public void Delete() {
			throw new NotImplementedException();
		}
	}
}
