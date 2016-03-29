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

using Deveel.Data.Sql;
using Deveel.Data.Sql.Tables;

namespace Deveel.Data.Diagnostics {
	public sealed class QueryEvent : Event {
		public QueryEvent(SqlQuery query, QueryEventType eventType, ITable[] result) {
			if (query == null)
				throw new ArgumentNullException("query");

			Query = query;
			EventType = eventType;
			Result = result;
		}

		public SqlQuery Query { get; private set; }

		public QueryEventType EventType { get; private set; }

		public ITable[] Result { get; private set; }

		protected override void GetEventData(Dictionary<string, object> data) {
			data["query.text"] = Query.Text;
			data["query.eventType"] = (int) EventType;
			data["query.paramStyle"] = Query.ParameterStyle;
			data["query.paramCount"] = Query.Parameters.Count;

			int i = 0;
			foreach (var parameter in Query.Parameters) {
				data[String.Format("query.param[{0}].name", i)] = parameter.Name;
				data[String.Format("query.param[{0}].type", i)] = parameter.SqlType.ToString();
				data[String.Format("query.param[{0}].value", i)] = parameter.Value;

				i++;
			}

			// TODO: pass also the results as meta?
		}
	}
}
