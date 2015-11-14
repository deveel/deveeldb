using System;
using System.IO;

namespace Deveel.Data.Store {
	public interface IFile : IDisposable {
		string FileName { get; }

		bool IsReadOnly { get; }

		long Position { get; }

		long Length { get; }

		bool Exists { get; }


		long Seek(long offset, SeekOrigin origin);

		void SetLength(long value);

		int Read(byte[] buffer, int offset, int length);

		void Write(byte[] buffer, int offset, int length);

		void Flush(bool writeThrough);

		void Close();

		void Delete();
	}
}
