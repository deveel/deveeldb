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

using Deveel.Data.Diagnostics;

namespace Deveel.Data.Sql.Triggers {
	public abstract class Trigger : IDbObject, IDisposable {
		protected Trigger(TriggerInfo triggerInfo) {
			if (triggerInfo ==null)
				throw new ArgumentNullException("triggerInfo");

			TriggerInfo = triggerInfo;
		}

		public TriggerInfo TriggerInfo { get; private set; }

		public ObjectName Name {
			get { return TriggerInfo.TriggerName; }
		}

		IObjectInfo IDbObject.ObjectInfo {
			get { return TriggerInfo; }
		}

		protected virtual void Dispose(bool disposing) {
			TriggerInfo = null;
		}

		public void Dispose() {
			Dispose(true);
			GC.SuppressFinalize(this);
		}

		public bool CanFire(TableEvent tableEvent) {
			return TriggerInfo.CanFire(tableEvent);
		}

		public void Fire(TableEvent tableEvent, IRequest context) {
			try {
				var triggerType = TriggerInfo.TriggerType;
				var tableName = tableEvent.Table.TableInfo.TableName;
				context.Context.OnEvent(new TriggerEvent(triggerType, Name, tableName, tableEvent.EventTime, tableEvent.EventType, tableEvent.OldRowId, tableEvent.NewRow));
				using (var block = context.CreateBlock()) {
					FireTrigger(tableEvent, block);
				}
			} catch (TriggerException) {
				throw;
			} catch (Exception ex) {
				throw new TriggerException(String.Format("An unknown error occurred while executing trigger '{0}'.", Name), ex);
			}
		}

		protected abstract void FireTrigger(TableEvent tableEvent, IBlock context);
	}
}
