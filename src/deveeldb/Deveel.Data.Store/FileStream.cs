using System;
using System.IO;

namespace Deveel.Data.Store {
	public sealed class FileStream : Stream {
		public IFile File { get; private set; }

		public FileStream(IFile file) {
			if (file == null)
				throw new ArgumentNullException("file");

			File = file;
		}

		public override void Flush() {
			File.Flush(true);
		}

		public override long Seek(long offset, SeekOrigin origin) {
			return File.Seek(offset, origin);
		}

		public override void SetLength(long value) {
			File.SetLength(value);
		}

		public override int Read(byte[] buffer, int offset, int count) {
			return File.Read(buffer, offset, count);
		}

		public override void Write(byte[] buffer, int offset, int count) {
			File.Write(buffer, offset, count);
		}

		public override bool CanRead {
			get { return true; }
		}

		public override bool CanSeek {
			get { return true; }
		}

		public override bool CanWrite {
			get { return !File.IsReadOnly; }
		}

		public override long Length {
			get { return File.Length; }
		}

		public override long Position {
			get { return File.Position; }
			set { Seek(value, SeekOrigin.Begin); }
		}

		protected override void Dispose(bool disposing) {
			File = null;
			base.Dispose(disposing);
		}
	}
}
