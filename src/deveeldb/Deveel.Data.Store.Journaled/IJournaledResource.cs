using System;

namespace Deveel.Data.Store.Journaled {
	public interface IJournaledResource : IDisposable {
		long Id { get; }

		int PageSize { get; }

		long Size { get; }

		bool Exists { get; }


		void Read(long pageNumber, byte[] buffer, int offset);

		void Write(long pageNumber, byte[] buffer, int offset, int count);

		void SetSize(long value);

		void Open(bool readOnly);

		void Close();

		void Delete();
	}
}
