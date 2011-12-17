// 
//  Copyright 2011 Deveel
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

using System;

namespace Deveel.Data.Sql {
	[Serializable]
	public sealed class CreateSequenceStatement : Statement {
		protected override Table Evaluate(IQueryContext context) {
			string seqNameString = GetString("seq_name");
			TableName seqName = ResolveTableName(context, seqNameString);

			Expression increment = GetExpression("increment");
			Expression minValue = GetExpression("min_value");
			Expression maxValue = GetExpression("max_value");
			Expression startValue = GetExpression("start");
			Expression cacheValue = GetExpression("cache");
			bool cycle = GetValue("cycle") != null;

			// Does the schema exist?
			SchemaDef schema = ResolveSchemaName(context, seqName.Schema);

			if (schema == null)
				throw new DatabaseException("Schema '" + seqName.Schema + "' doesn't exist.");

			seqName = new TableName(schema.Name, seqName.Name);

			// Does the user have privs to create this sequence generator?
			if (!context.Connection.Database.CanUserCreateSequenceObject(context, seqName))
				throw new UserAccessException("User not permitted to create sequence: " + seqName);

			// Does a table already exist with this name?
			if (context.Connection.TableExists(seqName))
				throw new DatabaseException("Database object with name '" + seqName + "' already exists.");

			// Resolve the expressions,
			long vStartValue = 0;
			if (startValue != null)
				vStartValue = startValue.Evaluate(null, null, context).ToBigNumber().ToInt64();

			long vIncrementBy = 1;
			if (increment != null)
				vIncrementBy = increment.Evaluate(null, null, context).ToBigNumber().ToInt64();

			long vMinValue = 0;
			if (minValue != null)
				vMinValue = minValue.Evaluate(null, null, context).ToBigNumber().ToInt64();

			long vMaxValue = Int64.MaxValue;
			if (maxValue != null)
				vMaxValue = maxValue.Evaluate(null, null, context).ToBigNumber().ToInt64();

			long vCache = 16;
			if (cacheValue != null) {
				vCache = cacheValue.Evaluate(null, null, context).ToBigNumber().ToInt64();
				if (vCache <= 0)
					throw new DatabaseException("Cache size can not be <= 0");
			}

			if (vMinValue >= vMaxValue) {
				throw new DatabaseException("Min value can not be >= the max value.");
			}
			if (vStartValue < vMinValue ||
			    vStartValue >= vMaxValue)
				throw new DatabaseException("Start value is outside the min/max sequence bounds.");

			context.Connection.CreateSequenceGenerator(seqName, vStartValue, vIncrementBy, vMinValue, vMaxValue, vCache, cycle);

			// The initial grants for a sequence is to give the user who created it
			// full access.
			context. Connection.GrantManager.Grant(
				Privileges.ProcedureAll, GrantObject.Table,
				seqName.ToString(), context.UserName, true,
				Database.InternalSecureUsername);

			// Return an update result table.
			return FunctionTable.ResultTable(context, 0);
		}
	}
}