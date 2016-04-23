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

using Deveel.Data.Routines;
using Deveel.Data.Sql.Expressions;

namespace Deveel.Data.Sql.Triggers {
	public sealed class ProcedureTrigger : Trigger {
		public ProcedureTrigger(ProcedureTriggerInfo triggerInfo)
			: base(triggerInfo) {
		}

		public ObjectName ProcedureName {
			get { return ((ProcedureTriggerInfo) TriggerInfo).ProcedureName; }
		}

		public SqlExpression[] Arguments {
			get { return ((ProcedureTriggerInfo) TriggerInfo).Arguments; }
		}

		protected override void FireTrigger(TableEvent tableEvent, IBlock context) {
			var procedure = context.Access().GetObject(DbObjectType.Routine, ProcedureName) as IProcedure;

			if (procedure == null)
				throw new TriggerException(String.Format("The procedure '{0}' was not found.", ProcedureName));

			// TODO: The whole routine invoke API must be optimized...
			var invoke = new Invoke(ProcedureName, Arguments);

			try {
				procedure.Execute(new InvokeContext(invoke, procedure, null, null, context));
			} catch (Exception ex) {
				throw new TriggerException(String.Format("Error while invoking '{0}'.",invoke), ex);
			}

		}
	}
}
