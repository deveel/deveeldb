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

using Deveel.Data.Routines;
using Deveel.Data.Query;
using Deveel.Data.Security;
using Deveel.Data.Types;
using Deveel.Diagnostics;

namespace Deveel.Data.DbSystem {
	public class SubsetQueryContext : QueryContext {
		private readonly IQueryContext parent;
		private readonly VariablesManager variables;

		public SubsetQueryContext(IQueryContext parent) {
			if (parent == null)
				throw new ArgumentNullException("parent");

			this.parent = parent;
			variables = new VariablesManager();
		}

		public IQueryContext Parent {
			get { return parent; }
		}

		public override IDatabaseConnection Connection {
			get { return parent.Connection; }
		}

		public override ILogger Logger {
			get { return parent.Logger; }
		}

		public override IRoutineResolver RoutineResolver {
			get { return parent.RoutineResolver; }
		}

		public override ISystemContext Context {
			get { return parent.Context; }
		}

		public override string UserName {
			get { return parent.UserName; }
		}

		public override Variable GetVariable(string name) {
			return variables.GetVariable(name) ?? parent.GetVariable(name);
		}

		public override Variable DeclareVariable(string name, TType type, bool constant, bool notNull) {
			if (parent.GetVariable(name) != null)
				throw new InvalidOperationException("Variable '" + name + "' was already defined in a parent scope.");

			return variables.DeclareVariable(name, type, constant, notNull);
		}

		public override void RemoveVariable(string name) {
			variables.RemoveVariable(name);
		}

		public override void SetVariable(string name, Expression value) {
			if (variables.SetVariable(name, value, this))
				return;

			parent.SetVariable(name, value);
		}

		public override long CurrentSequenceValue(string generatorName) {
			return parent.CurrentSequenceValue(generatorName);
		}

		public override long NextSequenceValue(string generatorName) {
			return parent.NextSequenceValue(generatorName);
		}

		public override void SetSequenceValue(string generatorName, long value) {
			parent.SetSequenceValue(generatorName, value);
		}

		public override Table GetTable(TableName tableName) {
			return parent.GetTable(tableName);
		}

		public override Cursor DeclareCursor(TableName name, IQueryPlanNode planNode, CursorAttributes attributes) {
			//TODO: declare the cursor in this scope ...
			return parent.DeclareCursor(name, planNode, attributes);
		}

		public override void OpenCursor(TableName name) {
			parent.OpenCursor(name);
		}

		public override Cursor GetCursor(TableName name) {
			return parent.GetCursor(name);
		}

		public override void CloseCursor(TableName name) {
			parent.CloseCursor(name);
		}

		public override Privileges GetUserGrants(GrantObject objType, string objName) {
			return parent.GetUserGrants(objType, objName);
		}
	}
}