// 
//  Copyright 2010-2015 Deveel
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

using Deveel.Data;
using Deveel.Data.Sql;
using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Objects;
using Deveel.Data.Sql.Sequences;
using Deveel.Data.Sql.Tables;
using Deveel.Data.Sql.Types;

namespace Deveel.Data.Routines {
	public static class SystemFunctions {
		static SystemFunctions() {
			Provider = new SystemFunctionsProvider();
		}

		public static FunctionProvider Provider { get; private set; }

		public static Field Or(Field ob1, Field ob2) {
			return ob1 != null ? (ob2.IsNull ? ob1 : (!ob1.IsNull ? ob1.Or(ob2) : ob2)) : ob2;
		}

		public static Field User(IRequest query) {
			return Field.String(query.User().Name);
		}


		public static Field ToDate(Field obj) {
			return obj.CastTo(PrimitiveTypes.Date());
		}

		public static Field ToDate(SqlString value) {
			return ToDate(Field.String(value));
		}

		public static Field ToDateTime(Field obj) {
			return obj.CastTo(PrimitiveTypes.DateTime());
		}

		public static Field ToDateTime(SqlString value) {
			return ToDateTime(Field.String(value));
		}

		public static Field ToTimeStamp(Field obj) {
			return obj.CastTo(PrimitiveTypes.TimeStamp());
		}

		public static Field ToTimeStamp(SqlString value) {
			return ToTimeStamp(Field.String(value));
		}

		public static Field Cast(Field value, SqlType destType) {
			return value.CastTo(destType);
		}

		public static Field Cast(IQuery query, Field value, SqlString typeString) {
			var destType = SqlType.Parse(query.Context, typeString.ToString());
			return Cast(value, destType);
		}

		public static Field ToNumber(Field value) {
			return value.CastTo(PrimitiveTypes.Numeric());
		}

		public static Field ToString(Field value) {
			return value.CastTo(PrimitiveTypes.String());
		}

		public static Field ToBinary(Field value) {
			return value.CastTo(PrimitiveTypes.Binary());
		}

		public static Field UniqueKey(IRequest query, Field tableName) {
			var tableNameString = (SqlString)tableName.Value;
			var value = UniqueKey(query, tableNameString);
			return Field.Number(value);
		}

		public static SqlNumber UniqueKey(IRequest query, SqlString tableName) {
			var tableNameString = tableName.ToString();
			var resolvedName = query.Query.Session.Access.ResolveTableName(tableNameString);
			return query.Query.Session.Access.GetNextValue(resolvedName);
		}

		public static Field CurrentValue(IRequest query, Field tableName) {
			var tableNameString = (SqlString)tableName.Value;
			var value = CurrentValue(query, tableNameString);
			return Field.Number(value);
		}

		public static SqlNumber CurrentValue(IRequest query, SqlString tableName) {
			var tableNameString = tableName.ToString();
			var resolvedName = query.Query.Session.Access.ResolveTableName(tableNameString);
			return query.Query.Session.Access.GetCurrentValue(resolvedName);
		}

		internal static InvokeResult Iif(InvokeContext context) {
			var result = Field.Null();

			var evalContext = new EvaluateContext(context.Request, context.VariableResolver, context.GroupResolver);

			var condition = context.Arguments[0].EvaluateToConstant(evalContext);
			if (condition.Type is BooleanType) {
				if (condition.Equals(Field.BooleanTrue)) {
					result = context.Arguments[1].EvaluateToConstant(evalContext);
				} else if (condition.Equals(Field.BooleanFalse)) {
					result = context.Arguments[2].EvaluateToConstant(evalContext);
				}
			}

			return context.Result(result);
		}

		internal static Field FRuleConvert(Field obj) {
			if (obj.Type is StringType) {
				String str = null;
				if (!obj.IsNull) {
					str = obj.Value.ToString();
				}
				int v;
				if (str == null || str.Equals("") || str.Equals("NO ACTION")) {
					v = ImportedKey.NoAction;
				} else if (str.Equals("CASCADE")) {
					v = ImportedKey.Cascade;
				} else if (str.Equals("SET NULL")) {
					v = ImportedKey.SetNull;
				} else if (str.Equals("SET DEFAULT")) {
					v = ImportedKey.SetDefault;
				} else if (str.Equals("RESTRICT")) {
					v = ImportedKey.Restrict;
				} else {
					throw new InvalidOperationException("Unrecognised foreign key rule: " + str);
				}

				// Return the correct enumeration
				return Field.Integer(v);
			}
			if (obj.Type is NumericType) {
				var code = ((SqlNumber)obj.AsBigInt().Value).ToInt32();
				string v;
				if (code == (int)ForeignKeyAction.Cascade) {
					v = "CASCADE";
				} else if (code == (int)ForeignKeyAction.NoAction) {
					v = "NO ACTION";
				} else if (code == (int)ForeignKeyAction.SetDefault) {
					v = "SET DEFAULT";
				} else if (code == (int)ForeignKeyAction.SetNull) {
					v = "SET NULL";
				} else {
					throw new InvalidOperationException("Unrecognised foreign key rule: " + code);
				}

				return Field.String(v);
			}

			throw new InvalidOperationException("Unsupported type in function argument");
		}
	}
}