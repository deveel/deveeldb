using System;

using Deveel.Data.Configuration;

namespace Deveel.Data.Store.Journaled {
	public sealed class BufferManager : IBufferManager, IConfigurable {
		public void Dispose() {
			throw new NotImplementedException();
		}

		public IJournaledResource CreateResource(string resourceName) {
			throw new NotImplementedException();
		}

		public int Read(IJournaledResource data, long position, byte[] buffer, int offset, int length) {
			throw new NotImplementedException();
		}

		public void Write(IJournaledResource data, long position, byte[] buffer, int offset, int length) {
			throw new NotImplementedException();
		}

		public void Lock() {
			throw new NotImplementedException();
		}

		public void Release() {
			throw new NotImplementedException();
		}

		public void Checkpoint() {
			throw new NotImplementedException();
		}

		void IConfigurable.Configure(IConfiguration config) {
			throw new NotImplementedException();
		}
	}
}
