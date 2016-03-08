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
using System.Linq;
using System.Runtime.Serialization;

namespace Deveel.Data.Sql.Statements {
	[Serializable]
	public class PlSqlBlock : CodeBlock {
		public PlSqlBlock() {
			Declarations = new DeclarationCollection();
			ExceptionHandlers = new ExceptionHandlerCollection();
		}

		protected PlSqlBlock(SerializationInfo info, StreamingContext context)
			: base(info, context) {
		}

		~PlSqlBlock() {
			Dispose(false);
		}

		public ICollection<SqlStatement> Declarations { get; private set; } 

		public ICollection<ExceptionHandler> ExceptionHandlers { get; private set; }

		protected override void GetData(SerializationInfo info) {
		}

		protected override void ExecuteStatement(ExecutionContext context) {
			if (Declarations != null) {
				foreach (var declaration in Declarations) {
					declaration.Execute(context);
				}
			}

			try {
				base.ExecuteStatement(context);
			} catch (SqlErrorException ex) {
				FireHandler(context, ex);
			} catch (Exception ex) {
				FireOthersHandler(context, ex);
			}
		}

		private void FireOthersHandler(ExecutionContext context, Exception error) {
			var handler = ExceptionHandlers.FirstOrDefault(x => x.Handled.IsForOthers);
			if (handler == null)
				throw error;

			handler.Handle(context);
		}

		private void FireHandler(ExecutionContext context, SqlErrorException error) {
			// TODO: find the named exception and if none found...
			FireOthersHandler(context, error);
		}

		protected override void Dispose(bool disposing) {
			if (disposing) {
				if (Declarations != null)
					Declarations.Clear();

				if (ExceptionHandlers != null)
					ExceptionHandlers.Clear();
			}

			Declarations = null;
			ExceptionHandlers = null;
		}

		protected override void AppendTo(SqlStringBuilder builder) {
			if (!String.IsNullOrEmpty(Label)) {
				builder.Append("<<{0}>>", Label);
				builder.AppendLine();
			}

			if (Declarations != null) {
				builder.AppendLine("DECLARE");
				builder.Indent();

				foreach (var declaration in Declarations.OfType<IDeclarationStatement>()) {
					declaration.AppendDeclarationTo(builder);
					builder.AppendLine();
				}

				builder.DeIndent();
			}

			builder.AppendLine("BEGIN");
			builder.Indent();

			foreach (var statement in Statements) {
				statement.Append(builder);
				builder.AppendLine();
			}
			
			builder.DeIndent();

			if (ExceptionHandlers != null) {
				builder.AppendLine("EXCEPTION");
				builder.Indent();

				foreach (var handler in ExceptionHandlers) {
					handler.PrintTo(builder);
				}

				builder.DeIndent();
			}

			builder.AppendLine("END");
		}

		#region DeclarationCollection

		class DeclarationCollection : Collection<SqlStatement> {
			private static void AssertDeclaration(SqlStatement statement) {
				if (!(statement is IDeclarationStatement))
					throw new ArgumentException(String.Format("The statement of type '{0}' is not a declaration.", statement.GetType()));
			}

			protected override void InsertItem(int index, SqlStatement item) {
				AssertDeclaration(item);
				base.InsertItem(index, item);
			}

			protected override void SetItem(int index, SqlStatement item) {
				AssertDeclaration(item);
				base.SetItem(index, item);
			}
		}

		#endregion

		#region ExceptionHandlerCollection

		class ExceptionHandlerCollection : Collection<ExceptionHandler> {
			private void AssertNotHandled(HandledExceptions handled) {
				foreach (var handler in base.Items) {
					if (handler.Handled.IsForOthers &&
						handled.IsForOthers)
						throw new ArgumentException("The OTHERS exception handler is already defined in the block.");
					foreach (var exceptionName in handled.ExceptionNames) {
						if (handler.Handles(exceptionName))
							throw new ArgumentException(String.Format("Trying to add a handler for exception '{0}' that is already handled.", exceptionName));
					}
				}
			}
			
			protected override void InsertItem(int index, ExceptionHandler item) {
				AssertNotHandled(item.Handled);
				base.InsertItem(index, item);
			}

			protected override void SetItem(int index, ExceptionHandler item) {
				AssertNotHandled(item.Handled);
				base.SetItem(index, item);
			}
		}

		#endregion
	}
}
