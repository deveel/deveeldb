using System;

namespace Deveel.Data.Types {
	public sealed class TabularType : SqlType {
		public TabularType()
			: base("TABLE", SqlTypeCode.QueryPlan) {
		}
	}
}
