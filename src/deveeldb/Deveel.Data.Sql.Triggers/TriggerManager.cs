using System;
using System.Collections.Generic;

using Deveel.Data.DbSystem;
using Deveel.Data.Transactions;

namespace Deveel.Data.Sql.Triggers {
	public sealed class TriggerManager : IObjectManager {
		private ITransaction transaction;

		public TriggerManager(ITransaction transaction) {
			this.transaction = transaction;
		}

		public void Dispose() {
		}

		DbObjectType IObjectManager.ObjectType {
			get { return DbObjectType.Trigger; }
		}

		public void Create() {
			// TODO: Create the tables
		}

		void IObjectManager.CreateObject(IObjectInfo objInfo) {
			var triggerInfo = objInfo as TriggerInfo;
			if (triggerInfo == null)
				throw new ArgumentException();

			CreateTrigger(triggerInfo);
		}

		bool IObjectManager.RealObjectExists(ObjectName objName) {
			return TriggerExists(objName);
		}

		bool IObjectManager.ObjectExists(ObjectName objName) {
			return TriggerExists(objName);
		}

		IDbObject IObjectManager.GetObject(ObjectName objName) {
			return GetTrigger(objName);
		}

		bool IObjectManager.AlterObject(IObjectInfo objInfo) {
			var triggerInfo = objInfo as TriggerInfo;
			if (triggerInfo == null)
				throw new ArgumentException();

			return AlterTrigger(triggerInfo);
		}

		bool IObjectManager.DropObject(ObjectName objName) {
			return DropTrigger(objName);
		}

		public ObjectName ResolveName(ObjectName objName, bool ignoreCase) {
			throw new NotImplementedException();
		}

		public void CreateTrigger(TriggerInfo triggerInfo) {
			throw new NotImplementedException();
		}

		public bool DropTrigger(ObjectName triggerName) {
			throw new NotImplementedException();
		}

		public bool TriggerExists(ObjectName triggerName) {
			throw new NotImplementedException();
		}

		public Trigger GetTrigger(ObjectName triggerName) {
			throw new NotImplementedException();
		}

		public bool AlterTrigger(TriggerInfo triggerInfo) {
			throw new NotImplementedException();
		}

		public IEnumerable<Trigger> FindTriggers(TriggerEventInfo eventInfo) {
			throw new NotImplementedException();
		}
	}
}
