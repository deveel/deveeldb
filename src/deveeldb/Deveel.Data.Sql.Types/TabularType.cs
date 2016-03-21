using System;

namespace Deveel.Data.Sql.Types {
	public sealed class TabularType : SqlType {
		public TabularType()
			: base("TABLE", SqlTypeCode.QueryPlan) {
		}
	}
}
