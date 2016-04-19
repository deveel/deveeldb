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
