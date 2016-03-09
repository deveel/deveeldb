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
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Runtime.Serialization;

namespace Deveel.Data.Sql.Statements {
	[Serializable]
	public abstract class CodeBlockStatement : SqlStatement, IDisposable {
		internal CodeBlockStatement() {
			Statements = new StatementCollection(this);
		}

		internal CodeBlockStatement(SerializationInfo info, StreamingContext context)
			: base(info, context) {
			Label = info.GetString("Label");
			Statements = DeserializeObjects(info);
		}

		~CodeBlockStatement() {
			Dispose(false);
		}

		public string Label { get; set; }

		public ICollection<SqlStatement> Statements { get; private set; }

		protected override void GetData(SerializationInfo info) {
			info.AddValue("Label", Label);
			SerializeObjects(info);
		}

		private void SerializeObjects(SerializationInfo info) {
			throw new NotImplementedException();
		}

		protected override void ExecuteStatement(ExecutionContext context) {
			foreach (var obj in Statements) {
				obj.Execute(context);
			}
		}

		private ICollection<SqlStatement> DeserializeObjects(SerializationInfo info) {
			throw new NotImplementedException();
		}

		protected override void AppendTo(SqlStringBuilder builder) {
			if (!String.IsNullOrEmpty(Label)) {
				builder.Append("<<{0}>>", Label);
				builder.AppendLine();
			}

			builder.AppendLine("BEGIN");
			builder.Indent();

			foreach (var statement in Statements) {
				statement.Append(builder);
			}

			builder.DeIndent();
			builder.AppendLine("END");
		}

		protected virtual void Dispose(bool disposing) {
			if (disposing) {
				if (Statements != null)
					Statements.Clear();
			}

			Statements = null;
		}

		void IDisposable.Dispose() {
			Dispose(true);
			GC.SuppressFinalize(this);
		}

		private void AssertPlSqlStatement(SqlStatement obj) {
			if (!(obj is IPlSqlStatement)) {
				throw new ArgumentException(String.Format("The statement of type '{0}' cannot be inserted into a PL/SQL block.",
					obj.GetType()));
			}
		}

		private void AssertNotLoopControl(SqlStatement obj) {
			if (obj is LoopControlStatement) {
				var statement = obj.Parent;
				while (statement != null) {
					if (statement is LoopStatement)
						return;

					statement = statement.Parent;
				}

				throw new ArgumentException("A loop control statement cannot be contained in a non-loop block");
			}
		}

		private void AssertAllowedObject(SqlStatement obj) {
			AssertPlSqlStatement(obj);
			AssertNotLoopControl(obj);
		}

		#region StatementCollection

		class StatementCollection : Collection<SqlStatement> {
			private readonly CodeBlockStatement block;

			public StatementCollection(CodeBlockStatement block) {
				this.block = block;
			}

			private void AssertPlSqlObject(SqlStatement obj) {
				if (obj.Parent != null)
					throw new ArgumentException("The statement has already a parent");

				block.AssertAllowedObject(obj);
			}

			protected override void InsertItem(int index, SqlStatement item) {
				AssertPlSqlObject(item);

				item.Parent = block;
				base.InsertItem(index, item);
			}

			protected override void SetItem(int index, SqlStatement item) {
				AssertPlSqlObject(item);

				item.Parent = block;
				base.SetItem(index, item);
			}
		}

		#endregion

	}
}
