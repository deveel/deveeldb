using System;
using System.Collections.Generic;

namespace Deveel.Data.Sql.Triggers {
	public sealed class CallbackTriggerManager : ITriggerManager {
		private Dictionary<string, CallbackTriggerInfo> triggers;
		 
		public CallbackTriggerManager(ITriggerScope scope) {
			if (scope == null)
				throw new ArgumentNullException("scope");

			Scope = scope;
			triggers = new Dictionary<string, CallbackTriggerInfo>();
		}

		~CallbackTriggerManager() {
			Dispose(false);
		}

		public ITriggerScope Scope { get; private set; }

		public void Dispose() {
			GC.SuppressFinalize(this);
			Dispose(true);
		}

		private void Dispose(bool disposing) {
			if (disposing) {
				if (triggers != null)
					triggers.Clear();
			}

			triggers = null;
		}

		void ITriggerManager.CreateTrigger(ITriggerInfo triggerInfo) {
			CreateTrigger((CallbackTriggerInfo) triggerInfo);
		}

		public void CreateTrigger(CallbackTriggerInfo triggerInfo) {
			triggers[triggerInfo.TriggerName] = triggerInfo;
		}

		bool ITriggerManager.DropTrigger(ObjectName triggerName) {
			return DropTrigger(triggerName.FullName);
		}

		public bool DropTrigger(string triggerName) {
			return triggers.Remove(triggerName);
		}

		bool ITriggerManager.TriggerExists(ObjectName triggerName) {
			return TriggerExists(triggerName.FullName);
		}

		public bool TriggerExists(string triggerName) {
			return triggers.ContainsKey(triggerName);
		}

		public void FireTriggers(IRequest context, TableEvent tableEvent) {
			foreach (var trigger in triggers.Values) {
				if (trigger.CanFire(tableEvent))
					Scope.OnTriggerEvent(new TriggerEvent(new ObjectName(trigger.TriggerName), tableEvent.Table.FullName,
						tableEvent.EventType, tableEvent.OldRowId, tableEvent.NewRow));
			}
		}
	}
}
