using System;

namespace Deveel.Data.Store.Journaled {
	public sealed class JournaledStoreSystem : IStoreSystem {
		public void Dispose() {
		}

		public void Load() {
			throw new NotImplementedException();
		}

		public bool StoreExists(string name) {
			throw new NotImplementedException();
		}

		public IStore CreateStore(string name) {
			throw new NotImplementedException();
		}

		public IStore OpenStore(string name) {
			throw new NotImplementedException();
		}

		public bool CloseStore(IStore store) {
			throw new NotImplementedException();
		}

		public bool DeleteStore(IStore store) {
			throw new NotImplementedException();
		}

		public void SetCheckPoint() {
			throw new NotImplementedException();
		}

		public void Lock(string lockName) {
			throw new NotImplementedException();
		}

		public void Unlock(string lockName) {
			throw new NotImplementedException();
		}
	}
}
