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
using Deveel.Data.Types;

namespace Deveel.Data.Routines {
	public static class SystemFunctions {
		static SystemFunctions() {
			Provider = new SystemFunctionsProvider();
		}

		public static FunctionProvider Provider { get; private set; }

		public static DataObject Or(DataObject ob1, DataObject ob2) {
			return ob1 != null ? (ob2.IsNull ? ob1 : (!ob1.IsNull ? ob1.Or(ob2) : ob2)) : ob2;
		}

		public static DataObject User(IQueryContext context) {
			return DataObject.String(context.User().Name);
		}


		public static DataObject ToDate(DataObject obj) {
			return obj.CastTo(PrimitiveTypes.Date());
		}

		public static DataObject ToDate(SqlString value) {
			return ToDate(DataObject.String(value));
		}

		public static DataObject ToDateTime(DataObject obj) {
			return obj.CastTo(PrimitiveTypes.DateTime());
		}

		public static DataObject ToDateTime(SqlString value) {
			return ToDateTime(DataObject.String(value));
		}

		public static DataObject ToTimeStamp(DataObject obj) {
			return obj.CastTo(PrimitiveTypes.TimeStamp());
		}

		public static DataObject ToTimeStamp(SqlString value) {
			return ToTimeStamp(DataObject.String(value));
		}

		public static DataObject Cast(DataObject value, SqlType destType) {
			return value.CastTo(destType);
		}

		public static DataObject Cast(IQueryContext context, DataObject value, SqlString typeString) {
			var destType = SqlType.Parse(context, typeString.ToString());
			return Cast(value, destType);
		}

		public static DataObject ToNumber(DataObject value) {
			return value.CastTo(PrimitiveTypes.Numeric());
		}

		public static DataObject ToString(DataObject value) {
			return value.CastTo(PrimitiveTypes.String());
		}

		public static DataObject ToBinary(DataObject value) {
			return value.CastTo(PrimitiveTypes.Binary());
		}

		public static DataObject UniqueKey(IQueryContext context, DataObject tableName) {
			var tableNameString = (SqlString)tableName.Value;
			var value = UniqueKey(context, tableNameString);
			return DataObject.Number(value);
		}

		public static SqlNumber UniqueKey(IQueryContext context, SqlString tableName) {
			var tableNameString = tableName.ToString();
			var resolvedName = context.ResolveTableName(tableNameString);
			return context.GetNextValue(resolvedName);
		}

		public static DataObject CurrentValue(IQueryContext context, DataObject tableName) {
			var tableNameString = (SqlString)tableName.Value;
			var value = CurrentValue(context, tableNameString);
			return DataObject.Number(value);
		}

		public static SqlNumber CurrentValue(IQueryContext context, SqlString tableName) {
			var tableNameString = tableName.ToString();
			var resolvedName = context.ResolveTableName(tableNameString);
			return context.GetCurrentValue(resolvedName);
		}

		internal static ExecuteResult Iif(ExecuteContext context) {
			var result = DataObject.Null();

			var evalContext = new EvaluateContext(context.QueryContext, context.VariableResolver, context.GroupResolver);

			var condition = context.Arguments[0].EvaluateToConstant(evalContext);
			if (condition.Type is BooleanType) {
				if (condition.Equals(DataObject.BooleanTrue)) {
					result = context.Arguments[1].EvaluateToConstant(evalContext);
				} else if (condition.Equals(DataObject.BooleanFalse)) {
					result = context.Arguments[2].EvaluateToConstant(evalContext);
				}
			}

			return context.Result(result);
		}

		internal static DataObject FRuleConvert(DataObject obj) {
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
				return DataObject.Integer(v);
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

				return DataObject.String(v);
			}

			throw new InvalidOperationException("Unsupported type in function argument");
		}
	}
}