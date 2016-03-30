using System;
using System.Collections.Generic;

namespace Deveel.Data.Sql.Triggers {
	public sealed class CallbackTriggerManager : ITriggerManager {
		private Dictionary<string, CallbackTrigger> triggers;
		 
		public CallbackTriggerManager(ITriggerScope scope) {
			if (scope == null)
				throw new ArgumentNullException("scope");

			Scope = scope;
			triggers = new Dictionary<string, CallbackTrigger>();
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

		void ITriggerManager.CreateTrigger(TriggerInfo triggerInfo) {
			CreateTrigger((CallbackTriggerInfo) triggerInfo);
		}

		public void CreateTrigger(CallbackTriggerInfo triggerInfo) {
			triggers[triggerInfo.TriggerName.FullName] = new CallbackTrigger(triggerInfo);
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

		Trigger ITriggerManager.GetTrigger(ObjectName triggerName) {
			return GetTrigger(triggerName.Name);
		}

		public CallbackTrigger GetTrigger(string name) {
			CallbackTrigger trigger;
			if (!triggers.TryGetValue(name, out trigger))
				return null;

			return trigger;
		}

		public void FireTriggers(IRequest context, TableEvent tableEvent) {
			foreach (var trigger in triggers.Values) {
				if (trigger.CanFire(tableEvent))
					trigger.Fire(tableEvent, context);
			}
		}
	}
}
