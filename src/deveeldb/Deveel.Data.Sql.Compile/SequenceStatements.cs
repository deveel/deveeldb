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
using System.Linq;

using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Statements;

namespace Deveel.Data.Sql.Compile {
	static class SequenceStatements {
		public static SqlStatement Create(PlSqlParser.CreateSequenceStatementContext context) {
			var seqName = Name.Object(context.objectName());

			var statement = new CreateSequenceStatement(seqName);

			var startWith = context.sequenceStartClause();
			if (startWith != null) {
				statement.StartWith = SqlExpression.Constant(Number.PositiveInteger(startWith.UNSIGNED_INTEGER()));
			}

			var specs = context.sequenceSpec();
			if (specs != null && specs.Length > 0) {
				foreach (var spec in specs) {
					if (spec.INCREMENT() != null &&
					    spec.BY() != null) {
						statement.IncrementBy = SqlExpression.Constant(Number.PositiveInteger(spec.UNSIGNED_INTEGER()));
					} else if (spec.MAXVALUE() != null) {
						statement.MaxValue = SqlExpression.Constant(Number.PositiveInteger(spec.UNSIGNED_INTEGER()));
					} else if (spec.NOMAXVALUE() != null) {
						statement.MaxValue = SqlExpression.Constant(null);
					} else if (spec.MINVALUE() != null) {
						statement.MinValue = SqlExpression.Constant(Number.PositiveInteger(spec.UNSIGNED_INTEGER()));
					} else if (spec.NOMINVALUE() != null) {
						statement.MinValue = SqlExpression.Constant(null);
					} else if (spec.CACHE() != null) {
						statement.Cache = SqlExpression.Constant(Number.PositiveInteger(spec.UNSIGNED_INTEGER()));
					} else if (spec.NOCACHE() != null) {
						statement.Cache = SqlExpression.Constant(null);
					} else if (spec.CYCLE() != null) {
						statement.Cycle = true;
					} else if (spec.NOCYCLE() != null) {
						statement.Cycle = false;
					}
				}
			}

			return statement;
		}

		public static SqlStatement Drop(PlSqlParser.DropSequenceStatementContext context) {
			var names = context.objectName().Select(Name.Object).ToArray();
			var ifExists = context.IF() != null && context.EXISTS() != null;

			if (names.Length == 1)
				return new DropSequenceStatement(names[0]);

			var sequence = new SequenceOfStatements();
			foreach (var name in names) {
				sequence.Statements.Add(new DropSequenceStatement(name));
			}

			return sequence;
		}
	}
}
