// 
//  Copyright 2010-2016 Deveel
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
using System.Linq;

using Deveel.Data.Diagnostics;
using Deveel.Data.Sql;

namespace Deveel.Data.Transactions {
	public sealed class TransactionRegistry : IEventRegistry, IDisposable {
		private List<ITransactionEvent> events;
		private List<ObjectName> objectsCreated;
		private List<ObjectName> objectsDropped;
		private List<int> touchedTables; 
 
		internal TransactionRegistry() {
			events = new List<ITransactionEvent>();
		}

		~TransactionRegistry() {
			Dispose(false);
		}

		public IEnumerable<ObjectName> ObjectsCreated {
			get {
				lock (this) {
					return objectsCreated == null ? new ObjectName[0] : objectsCreated.ToArray();
				}
			}
		}

		public IEnumerable<ObjectName> ObjectsDropped {
			get {
				lock (this) {
					return objectsDropped == null ? new ObjectName[0] : objectsDropped.ToArray();	
				}
			}
		}

		public IEnumerable<int> TouchedTables {
			get {
				lock (this) {
					return touchedTables == null ? new int[0] : touchedTables.ToArray();
				}
			}
		}

		public IEnumerable<int> TablesCreated {
			get {
				lock (this) {
					return events.OfType<TableCreatedEvent>().Select(x => x.TableId);
				}
			}
		}

		//public IEnumerable<int> TablesChanged {
		//	get {
		//		lock (this) {
		//			return events.OfType<ITableEvent>()
		//				.Select(x => new {
		//					id = x.TableId,
		//					table = Transaction.GetTableManager().AccessedTables.First(y => y.TableInfo.Id == x.TableId)
		//				})
		//				.Where(x => x.table.EventRegistry.EventCount > 0)
		//				.Select(x => x.id);
		//		}
		//	}
		//} 

		public IEnumerable<int> TablesDropped {
			get {
				lock (this) {
					return events.OfType<TableDroppedEvent>().Select(x => x.TableId);
				}
			}
		}

		public IEnumerable<int> TablesConstraintAltered {
			get {
				lock (this) {
					return events.OfType<TableConstraintAlteredEvent>().Select(x => x.TableId);
				}
			}
		}

		void IEventRegistry.RegisterEvent(IEvent e) {
			if (!(e is ITransactionEvent))
				return;

			RegisterEvent((ITransactionEvent)e);
		}

		private void RegisterObjectDropped(ObjectName objName) {
			bool created = false;

			if (objectsCreated != null)
				created = objectsCreated.Remove(objName);

			// If the above operation didn't remove a table name then add to the
			// dropped database objects list.
			if (!created) {
				if (objectsDropped == null)
					objectsDropped = new List<ObjectName>();

				objectsDropped.Add(objName);
			}
		}

		private void RegisterObjectCreated(ObjectName objName) {
			// If this table name was dropped, then remove from the drop list
			bool dropped = false;
			if (objectsDropped != null)
				dropped = objectsDropped.Remove(objName);

			// If the above operation didn't remove a table name then add to the
			// created database objects list.
			if (!dropped) {
				if (objectsCreated == null)
					objectsCreated = new List<ObjectName>();

				objectsCreated.Add(objName);
			}
		}

		private void RegisterEvent(ITransactionEvent e) {
			lock (this) {
				if (e == null)
					throw new ArgumentNullException("e");

				//if (Transaction.ReadOnly())
				//	throw new InvalidOperationException("Transaction is read-only.");

				if (e is ObjectCreatedEvent) {
					var createdEvent = (ObjectCreatedEvent) e;
					RegisterObjectCreated(createdEvent.ObjectName);
				} else if (e is ObjectDroppedEvent) {
					var droppedEvent = (ObjectDroppedEvent) e;
					RegisterObjectDropped(droppedEvent.ObjectName);
				}

				if (e is ITableEvent) {
					var tableEvent = (ITableEvent) e;
					TouchTable(tableEvent.TableId);
				}

				events.Add(e);
			}
		}

		private void TouchTable(int tableId) {
			lock (this) {
				if (touchedTables == null)
					touchedTables = new List<int>();

				var index = touchedTables.LastIndexOf(tableId);
				if (index > 0 && touchedTables[index] == tableId)
					return;

				if (index < 0) {
					touchedTables.Add(tableId);
				} else {
					touchedTables.Insert(index, tableId);
				}
			}
		}

		public IEnumerable<ITransactionEvent> GetEvents() {
			lock (this) {
				return events.ToArray();
			}
		}

		public void Dispose() {
			Dispose(true);
			GC.SuppressFinalize(this);
		}

		private void Dispose(bool disposing) {
			if (disposing) {
				if (objectsCreated != null)
					objectsCreated.Clear();

				if (objectsDropped != null)
					objectsDropped.Clear();

				events.Clear();
			}

			objectsDropped = null;
			objectsCreated = null;
			events = null;
		}
	}
}
