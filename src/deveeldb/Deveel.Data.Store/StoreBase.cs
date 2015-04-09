using System;
using System.Collections.Generic;

namespace Deveel.Data.Store {
	public abstract class StoreBase : IStore {
		public void Dispose() {
			throw new NotImplementedException();
		}

		public IArea CreateArea(long size) {
			throw new NotImplementedException();
		}

		public void DeleteArea(long id) {
			throw new NotImplementedException();
		}

		public IArea GetArea(long id, bool readOnly) {
			throw new NotImplementedException();
		}

		public void LockForWrite() {
			throw new NotImplementedException();
		}

		public void UnlockForWrite() {
			throw new NotImplementedException();
		}

		public void CheckPoint() {
			throw new NotImplementedException();
		}

		public bool ClosedClean { get; private set; }

		public IEnumerable<long> GetAllAreas() {
			throw new NotImplementedException();
		}

		public IEnumerable<long> FindAllocatedAreasNotIn(List<long> usedAreas) {
			throw new NotImplementedException();
		}
	}
}