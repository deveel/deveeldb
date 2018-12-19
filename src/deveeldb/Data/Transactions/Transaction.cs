using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;

using Deveel.Data.Configurations;
using Deveel.Data.Events;
using Deveel.Data.Services;
using Deveel.Data.Sql;
using Deveel.Data.Sql.Indexes;
using Deveel.Data.Sql.Tables;

namespace Deveel.Data.Transactions {
	public sealed class Transaction : Context, ITransaction {
		private List<LockHandle> lockHandles;

		internal Transaction(IDatabase database, ITableSystem tableSystem, long commitId, IsolationLevel isolationLevel, ITableSource[] visibleTables, IRowIndexSet[] indexSets)
			: base(database, KnownScopes.Transaction) {
			Database = database;
			TableSystem = tableSystem;
			CommitId = commitId;

			if (isolationLevel == IsolationLevel.Unspecified)
				throw new ArgumentException("Unspecified transaction isolation level");

			IsolationLevel = isolationLevel;

			Configuration = new Configuration();

			if (database.Configuration != null)
				Configuration = Configuration.MergeWith(database.Configuration);

			TableManager = this.GetTableManager();

			State = new TransactionState(visibleTables, indexSets);

			Status = TransactionStatus.Started;
		}

		public IConfiguration Configuration { get; }

		public long CommitId { get; }

		public IsolationLevel IsolationLevel { get; }

		IEventRegistry IEventHandler.Registry => Registry;

		public ITransactionEventRegistry Registry { get; }

		IEventSource IEventSource.ParentSource => Database;

		IDictionary<string, object> IEventSource.Metadata => GetMetadata();

		public IDatabase Database { get; }

		public TransactionState State { get; }

		public TransactionStatus Status { get; set; }

		private ITableSystem TableSystem { get; }

		private bool IsReadOnly => Database.IsReadOnly();

		private TableManager TableManager { get; }

		private bool IsClosed { get; set; }

		private IDictionary<string, object> GetMetadata() {
			// TODO: more data?
			return new Dictionary<string, object> {
				{"commit.id", CommitId},
				{"isolationLevel", IsolationLevel.ToString().ToUpperInvariant()},
				{"readOnly", IsReadOnly}
			};
		}

		private void ReleaseLocks() {
			if (Database == null)
				return;

			lock (Database) {
				if (lockHandles != null) {
					foreach (var handle in lockHandles) {
						if (handle != null) {
							Database.Locker.Release(handle);
						}
					}

					lockHandles.Clear();
				}

				lockHandles = null;
			}
		}

		private void Finish() {
			try {
				// Dispose all the table we touched
				try {
					ReleaseLocks();
				} catch (Exception ex) {
					// TODO: log error
				}

			} finally {
				IsClosed = true;
			}
		}


		//public void Enter(IEnumerable<IDbObject> objects, AccessType accessType) {
		//	if (Database == null)
		//		return;

		//	lock (Database) {
		//		var lockables = objects.OfType<ILockable>().ToArray();
		//		if (lockables.Length == 0)
		//			return;

		//		var timeout = this.LockTimeout();

		//		if (lockables.Any(x => Database.Locker.IsLocked(x))) {
		//			if (IsolationLevel == IsolationLevel.ReadCommitted) {
		//				Database.Locker.Wait(lockables, AccessType.Read, timeout);
		//			} else if (IsolationLevel == IsolationLevel.Serializable) {
		//				Database.Locker.Wait(lockables, AccessType.ReadWrite, timeout);
		//			}
		//		}

		//		var handle = Database.Locker.Lock(lockables, AccessType.ReadWrite, LockingMode.Exclusive);

		//		var tables = lockables.OfType<IDbObject>().Where(x => x.ObjectInfo.ObjectType == DbObjectType.Table)
		//			.Select(x => x.ObjectInfo.FullName);
		//		foreach (var table in tables) {
		//			TableManager.SelectTable(table);
		//		}

		//		if (handle != null) {
		//			if (lockHandles == null)
		//				lockHandles = new List<LockHandle>();

		//			lockHandles.Add(handle);
		//		}

		//		// TODO:
		//		//var lockedNames = objects.Where(x => x is ILockable).Select(x => x.ObjectInfo.FullName);
		//		//this.RaiseEvent<LockEnterEvent>(lockedNames, LockingMode.Exclusive, accessType);
		//	}
		//}

		//public void Exit(IEnumerable<IDbObject> objects, AccessType accessType) {
		//	if (Database == null)
		//		return;

		//	lock (Database) {
		//		var lockables = objects.OfType<ILockable>().ToArray();
		//		if (lockables.Length == 0)
		//			return;

		//		if (lockHandles != null) {
		//			for (int i = lockables.Length - 1; i >= 0; i--) {
		//				var handle = lockHandles[i];

		//				bool handled = true;
		//				foreach (var lockable in lockables) {
		//					if (!handle.IsHandled(lockable)) {
		//						handled = false;
		//						break;
		//					}
		//				}

		//				if (handled) {
		//					Database.Locker.Release(handle);
		//					lockHandles.RemoveAt(i);
		//				}
		//			}
		//		}

		//		// TODO:
		//		//var lockedNames = objects.Where(x => x is ILockable).Select(x => x.ObjectInfo.FullName);
		//		//this.RaiseEvent<LockExitEvent>(lockedNames, LockingMode.Exclusive, accessType);
		//	}
		//}

		public void Commit(string savePoint) {
			if (!String.IsNullOrEmpty(savePoint))
				throw new NotSupportedException();

			if (IsClosed)
				return;

			try {
				Status = TransactionStatus.Commit;

				TableSystem.Commit(this);

				// TODO: fire an event
			} catch (Exception e) {
				throw;
			} finally {
				Finish();
			}
		}

		public void Rollback(string savePoint) {
			if (!String.IsNullOrEmpty(savePoint))
				throw new NotSupportedException();

			if (IsClosed)
				return;

			try {
				Status = TransactionStatus.Rollback;

				TableSystem.Rollback(this);

				// TODO: fire an event
			} catch (Exception e) {
				throw;
			} finally {
				Finish();
			}
		}

		protected override void Dispose(bool disposing) {
			if (disposing) {
				if (!IsClosed) {
					Rollback(null);
				} else {
					Finish();
				}
			}

			base.Dispose(disposing);
		}

		#region TransactionEventRegistry

		class TransactionEventRegistry : InMemoryEventRegistry, ITransactionEventRegistry {
			private List<IEvent> events;
			private List<ObjectName> objectsCreated;
			private List<ObjectName> objectsDropped;
			private List<int> touchedTables; 

			public TransactionEventRegistry() {
				events = new List<IEvent>();
			}

			public IEnumerable<ObjectName> CreatedObjects {
				get {
					lock (this) {
						return objectsCreated == null ? new ObjectName[0] : objectsCreated.ToArray();
					}
				}
			}

			public IEnumerable<ObjectName> DroppedObjects {
				get {
					lock (this) {
						return objectsDropped == null ? new ObjectName[0] : objectsDropped.ToArray();	
					}
				}
			}

			public IEnumerable<int> CreatedTables {
				get {
					lock (this) {
						return events.OfType<TableCreatedEvent>().Select(x => x.TableId);
					}
				}
			}

			public IEnumerable<int> DroppedTables{
				get {
					lock (this) {
						return events.OfType<TableDroppedEvent>().Select(x => x.TableId);
					}
				}
			}

			public IEnumerable<int> ConstraintAlteredTables {
				get {
					lock (this) {
						return events.OfType<TableConstraintAlteredEvent>().Select(x => x.TableId);
					}
				}
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

			protected override void OnEventRegistered(IEvent e) {
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
		}

		#endregion
	}
}