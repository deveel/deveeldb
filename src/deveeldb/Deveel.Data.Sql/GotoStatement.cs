// 
//  Copyright 2010-2014 Deveel
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
using System.Collections.Generic;

using Deveel.Data.DbSystem;

namespace Deveel.Data.Sql {
	[Serializable]
	public sealed class GotoStatement : Statement {
		public string Label {
			get { return GetString("label_name"); }
			set { SetValue("label_name", value); }
		}

		private static bool IsLabel(Statement statement, string labelName) {
			return statement is LabelStatement && ((LabelStatement) statement).LabelName == labelName;
		}

		private Statement FindLabelStatement(string labelName) {
			Statement root = Root;
			if (IsLabel(root, labelName))
				return root;

			IList<Statement> childStatements = root.GetAllPreparedStatements();
			foreach (Statement child in childStatements) {
				if (IsLabel(child, labelName))
					return child;
			}

			return null;
		}

		protected override Table Evaluate(IQueryContext context) {
			string labelName = GetString("label_name");
			Statement statement = FindLabelStatement(labelName);
			if (statement == null)
				throw new DatabaseException("Label '" + labelName + "' was not found in the current context.");

			return statement.EvaluateStatement(context);
		}
	}
}