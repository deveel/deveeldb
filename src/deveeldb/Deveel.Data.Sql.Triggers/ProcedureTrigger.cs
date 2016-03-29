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

		public override void Fire(TableEvent tableEvent, IRequest context) {
			var procedure = context.Access.GetObject(DbObjectType.Routine, ProcedureName) as IProcedure;

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
