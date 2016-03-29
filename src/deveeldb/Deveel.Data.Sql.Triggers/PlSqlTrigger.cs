using System;

using Deveel.Data.Sql.Statements;

namespace Deveel.Data.Sql.Triggers {
	public sealed class PlSqlTrigger : Trigger {
		public PlSqlTrigger(PlSqlTriggerInfo triggerInfo)
			: base(triggerInfo) {
		}

		public PlSqlBlockStatement Body {
			get { return ((PlSqlTriggerInfo) TriggerInfo).Body; }
		}

		public override void Fire(TableEvent tableEvent, IRequest context) {
			// TODO: Should pass arguments?
			try {
				Body.Execute(new ExecutionContext(context, Body));
			} catch (Exception ex) {
				throw new TriggerException(this, ex);
			}
		}
	}
}
