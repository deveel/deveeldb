using System;
using System.Collections.Generic;
using System.Linq;

using Deveel.Data.Sql;

namespace Deveel.Data.Diagnostics {
	public class QueryEvent : NotificationEvent {
		public QueryEvent(SqlQuery sourceQuery, string statementText) 
			: base(NotificationLevel.Verbose, EventClasses.Runtime, 1000, null) {
			if (sourceQuery == null)
				throw new ArgumentNullException("sourceQuery");

			SourceQuery = sourceQuery;
			StatementText = statementText;
		}


		public SqlQuery SourceQuery { get; private set; }

		public string StatementText { get; private set; }

		protected override void FillEventData(IDictionary<string, object> eventData) {
			eventData["SourceQueryText"] = SourceQuery.Text;
			eventData["SourceQueryParameters"] = FormatParameters(SourceQuery.Parameters);
			eventData["StatementText"] = StatementText;		}

		private string FormatParameters(IEnumerable<QueryParameter> parameters) {
			if (parameters == null)
				return String.Empty;

			var texts = parameters.Select(x => String.Format("{0} ({1}) => {2}", x.Name, x.DataType, x.Value)).ToArray();
			return String.Join(", ", texts);
		}
	}
}
