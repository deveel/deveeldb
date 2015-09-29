using System;
using System.IO;

namespace Deveel.Data.Store.Journaled {
	public interface IFileHandle : IDisposable {
		string FileName { get; }

		bool IsReadOnly { get; }

		long Position { get; }

		long Length { get; }


		long Seek(long offset, SeekOrigin origin);


		int Read(byte[] buffer, int offset, int length);

		void Write(byte[] buffer, int offset, int length);

		void Flush(bool writeThrough);

		void Close();

		void Delete();

		void SetLength(long value);
	}
}
