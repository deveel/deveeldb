// 
//  Copyright 2010  Deveel
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
using System.Collections;

using Deveel.Data.DbSystem;
using Deveel.Data.Query;
using Deveel.Data.Types;

namespace Deveel.Data.Procedures {
	/// <summary>
	/// An implementation of <see cref="IQueryContext"/> that is used within
	/// a <see cref="StoredProcedure">procedure</see> to encapsulate
	/// an execution context.
	/// </summary>
	/// <remarks>
	/// This class wraps an higher-level execution context to resolve most of 
	/// the references (such as tables, views, functions, etc.), except for
	/// cursors and variables, which are declared within the scope of the
	/// procedure.
	/// <para>
	/// Cursros and variables declared within the scope of a procedure
	/// will be destroyed at the end of the scope and won't be accessible
	/// outside the scope of the procedure.
	/// </para>
	/// </remarks>
	public class ProcedureQueryContext : QueryContext, ICursorContext {
		/// <summary>
		/// Constructs a new <see cref="ProcedureQueryContext"/>.
		/// </summary>
		/// <param name="procedure">The procedure defining the execution context.</param>
		/// <param name="context">The reference to an higher level context (eg.
		/// <see cref="DatabaseQueryContext"/>) used to resolve most of the references.</param>
		public ProcedureQueryContext(StoredProcedure procedure, IQueryContext context) {
			if (procedure == null)
				throw new ArgumentNullException("procedure");
			if (context == null)
				throw new ArgumentNullException("context");

			this.procedure = procedure;
			this.context = context;
		}

		private readonly StoredProcedure procedure;
		private readonly IQueryContext context;
		private readonly ArrayList cursors = new ArrayList();
		private readonly ArrayList variables = new ArrayList();

		#region Overrides of QueryContext

		/// <summary>
		/// Gets a reference to the <see cref="StoredProcedure"/> that
		/// represents the context of execution.
		/// </summary>
		public StoredProcedure Procedure {
			get { return procedure; }
		}

		private bool CursorExists(TableName name) {
			return GetCursor(name) != null;
		}

		public override Cursor DeclareCursor(TableName name, IQueryPlanNode planNode, CursorAttributes attributes) {
			if (CursorExists(name))
				throw new InvalidOperationException("The cursor '" + name + "' is already declared.");

			return new Cursor(this, name, planNode, attributes);
		}

		public override void OpenCursor(TableName name) {
			Cursor cursor = GetCursor(name);
			if (name == null)
				throw new ProcedureException("The cursor '" + name + "' is not declared within the context of the procedure.");

			cursor.Open(this);
		}

		public override void CloseCursor(TableName name) {
			Cursor cursor = GetCursor(name);
			if (name == null)
				throw new ProcedureException("The cursor '" + name + "' is not declared within the context of the procedure.");

			cursor.Close();
		}

		public override Cursor GetCursor(TableName name) {
			for (int i = 0; i < cursors.Count; i++) {
				Cursor cursor = (Cursor)cursors[i];
				if (cursor.Name == name)
					return cursor;
			}

			return null;
		}

		#endregion

		private void RemoveCursor(TableName name) {
			lock(cursors) {
				for (int i = cursors.Count - 1; i >= 0; i--) {
					Cursor cursor = (Cursor) cursors[i];
					if (cursor.Name == name) {
						cursors.RemoveAt(i);
						break;
					}
				}
			}
		}

		#region Implementation of ICursorContext

		void ICursorContext.OnCursorCreated(Cursor cursor) {
			cursors.Add(cursor);
		}

		void ICursorContext.OnCursorDisposing(Cursor cursor) {
			if (cursor.State == CursorState.Opened)
				cursor.Close();

			RemoveCursor(cursor.Name);
		}

		#endregion

		public override Variable DeclareVariable(string name, TType type, bool constant, bool notNull) {
			if (GetVariable(name) != null)
				throw new ProcedureException("The variable '" + name + "' was already declared within the current scope.");

			Variable variable = new Variable(name, type, constant, notNull);
			variables.Add(variable);
			return variable;
		}

		public override Variable GetVariable(string name) {
			for (int i = 0; i < variables.Count; i++) {
				Variable variable = (Variable) variables[i];
				if (variable.Name == name)
					return variable;
			}

			return null;
		}

		public override void SetVariable(string name, Expression value) {
			Variable var = GetVariable(name);
			if (var == null)
				throw new ProcedureException("The variable '" + name + "' was not declared within the current context.");

			var.SetValue(value, this);
		}
	}
}