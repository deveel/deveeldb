using System;
using System.Collections.Generic;

using Deveel.Data.Sql;

namespace Deveel.Data.Diagnostics {
	public sealed class QueryEvent : Event {
		public QueryEvent(SqlQuery query) {
			if (query == null)
				throw new ArgumentNullException("query");

			Query = query;
		}

		public SqlQuery Query { get; private set; }

		protected override void GetEventData(Dictionary<string, object> data) {
			data["query.text"] = Query.Text;
			data["query.paramStyle"] = Query.ParameterStyle;

			int i = 0;
			foreach (var parameter in Query.Parameters) {
				data[String.Format("query.param[{0}].name", i)] = parameter.Name;
				data[String.Format("query.param[{0}].type", i)] = parameter.SqlType.ToString();
				data[String.Format("query.param[{0}].value", i)] = parameter.Value;

				i++;
			}
		}
	}
}
