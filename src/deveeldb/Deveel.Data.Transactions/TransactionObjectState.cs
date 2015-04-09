using System;
using System.Collections.Generic;

namespace Deveel.Data.Transactions {
	class TransactionObjectState {
		public TransactionObjectState(long commitId, IEnumerable<ObjectName> createdObjects, IEnumerable<ObjectName> droppedObjects) {
			CommitId = commitId;
			CreatedObjects = createdObjects;
			DroppedObjects = droppedObjects;
		}

		public long CommitId { get; private set; }

		public IEnumerable<ObjectName> CreatedObjects { get; private set; }

		public IEnumerable<ObjectName> DroppedObjects { get; private set; } 
	}
}