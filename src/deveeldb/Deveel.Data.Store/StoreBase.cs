// 
//  Copyright 2010-2015 Deveel
// 
//    Licensed under the Apache License, Version 2.0 (the "License");
//    you may not use this file except in compliance with the License.
//    You may obtain a copy of the License at
// 
//        http://www.apache.org/licenses/LICENSE-2.0
// 
//    Unless required by applicable law or agreed to in writing, software
//    distributed under the License is distributed on an "AS IS" BASIS,
//    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//    See the License for the specific language governing permissions and
//    limitations under the License.
//

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