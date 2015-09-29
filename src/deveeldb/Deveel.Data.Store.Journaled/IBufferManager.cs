using System;

namespace Deveel.Data.Store.Journaled {
	public interface IBufferManager : IDisposable {
		IJournaledResource CreateResource(string resourceName);

		int Read(IJournaledResource data, long position, byte[] buffer, int offset, int length);

		void Write(IJournaledResource data, long position, byte[] buffer, int offset, int length);

		void Lock();

		void Release();

		void Checkpoint();
	}
}
