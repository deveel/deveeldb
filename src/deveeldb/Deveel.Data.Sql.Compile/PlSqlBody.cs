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

using Deveel.Data.Sql.Statements;

namespace Deveel.Data.Sql.Compile {
	class PlSqlBody : SqlStatement {
		public PlSqlBody() {
			Statements = new List<SqlStatement>();
			ExceptionHandlers = new List<ExceptionHandler>();
		}

		public List<SqlStatement> Statements { get; private set; }
		
		public List<ExceptionHandler> ExceptionHandlers { get; private set; }
		public string Label { get; set; }

		public PlSqlBlockStatement AsPlSqlStatement() {
			var statement = new PlSqlBlockStatement {
				Label = Label
			};

			foreach (var sqlStatement in Statements) {
				statement.Statements.Add(sqlStatement);				
			}

			foreach (var handler in ExceptionHandlers) {
				statement.ExceptionHandlers.Add(handler);
			}

			return statement;
		}
	}
}
