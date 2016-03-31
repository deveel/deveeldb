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
using System.Collections.ObjectModel;
using System.Linq;
using System.Runtime.Serialization;

namespace Deveel.Data.Sql.Statements {
	[Serializable]
	public class PlSqlBlockStatement : CodeBlockStatement {
		public PlSqlBlockStatement() {
			Declarations = new DeclarationCollection();
			ExceptionHandlers = new ExceptionHandlerCollection();
		}

		protected PlSqlBlockStatement(SerializationInfo info, StreamingContext context)
			: base(info, context) {
			Declarations = new DeclarationCollection();
			ExceptionHandlers = new ExceptionHandlerCollection();

			var decls = (SqlStatement[]) info.GetValue("Declarations", typeof (SqlStatement[]));
			foreach (var statement in decls) {
				Declarations.Add(statement);
			}

			var handlers = (ExceptionHandler[]) info.GetValue("ExceptionHandlers", typeof (ExceptionHandler[]));
			foreach (var handler in handlers) {
				ExceptionHandlers.Add(handler);
			}
		}

		public ICollection<SqlStatement> Declarations { get; private set; } 

		public ICollection<ExceptionHandler> ExceptionHandlers { get; private set; }

		protected override void GetData(SerializationInfo info) {
			info.AddValue("Declarations", Declarations.ToArray());
			info.AddValue("ExceptionHandlers", ExceptionHandlers.ToArray());
			base.GetData(info);
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

		internal void FireHandler(ExecutionContext context, string exception) {
			var handler = ExceptionHandlers.FirstOrDefault(x => x.Handles(exception));
			if (handler == null)
				throw new InvalidOperationException(String.Format("Exception '{0}' is not handled in this context.", exception));

			handler.Handle(context);
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
