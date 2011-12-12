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
using System.Collections.Generic;

namespace Deveel.Data.Sql {
	/// <summary>
	/// Represents an action in an <c>ALTER TABLE</c> SQL statement.
	/// </summary>
	[Serializable]
	public sealed class AlterTableAction : IStatementTreeObject {
		/// <summary>
		/// Element parameters to do with the action.
		/// </summary>
		private List<object> elements;

		/// <summary>
		/// The action to perform.
		/// </summary>
		private readonly AlterTableActionType actionType;

		///<summary>
		///</summary>
		public AlterTableAction(AlterTableActionType actionType) {
			this.actionType = actionType;
			elements = new List<object>();
		}

		/// <summary>
		/// Gets or sets the action to perform.
		/// </summary>
		public AlterTableActionType ActionType {
			get { return actionType; }
		}

		/// <summary>
		/// Returns the list of parameters of this action.
		/// </summary>
		internal IList<object> Elements {
			get { return elements; }
		}

		internal void AddElements(IEnumerable collection) {
			foreach (object ob in collection) {
				elements.Add(ob);
			}
		}

		/// <inheritdoc/>
		void IStatementTreeObject.PrepareExpressions(IExpressionPreparer preparer) {
			// This must search throw 'elements' for objects that we can prepare
			foreach (object ob in elements) {
				if (ob is String) {
					// Do not need to prepare this
				} else if (ob is Expression) {
					((Expression)ob).Prepare(preparer);
				} else if (ob is IStatementTreeObject) {
					((IStatementTreeObject)ob).PrepareExpressions(preparer);
				} else {
					throw new DatabaseException("Unrecognised expression: " + ob.GetType());
				}
			}
		}

		/// <inheritdoc/>
		public object Clone() {
			// Shallow clone
			AlterTableAction v = (AlterTableAction)MemberwiseClone();
			v.elements = new List<object>();

			foreach (object element in elements) {
				object ob = element;

				if (ob is string) {
					// Do not need to clone this
				} else if (ob is Expression) {
					ob = ((Expression)ob).Clone();
				} else if (ob is IStatementTreeObject) {
					ob = ((IStatementTreeObject)ob).Clone();
				} else {
					throw new ApplicationException(ob.GetType().ToString());
				}

				v.elements.Add(ob);
			}

			return v;
		}

	}
}