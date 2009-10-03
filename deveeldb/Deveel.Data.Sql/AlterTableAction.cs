// 
//  AlterTableAction.cs
//  
//  Author:
//       Antonello Provenzano <antonello@deveel.com>
//       Tobias Downer <toby@mckoi.com>
//  
//  Copyright (c) 2009 Deveel
// 
//  This program is free software: you can redistribute it and/or modify
//  it under the terms of the GNU Lesser General Public License as published by
//  the Free Software Foundation, either version 3 of the License, or
//  (at your option) any later version.
// 
//  This program is distributed in the hope that it will be useful,
//  but WITHOUT ANY WARRANTY; without even the implied warranty of
//  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
//  GNU Lesser General Public License for more details.
// 
//  You should have received a copy of the GNU Lesser General Public License
//  along with this program.  If not, see <http://www.gnu.org/licenses/>.

using System;
using System.Collections;

namespace Deveel.Data.Sql {
	/// <summary>
	/// Represents an action in an <c>ALTER TABLE</c> SQL statement.
	/// </summary>
	[Serializable]
	public sealed class AlterTableAction : IStatementTreeObject {
		/// <summary>
		/// Element parameters to do with the action.
		/// </summary>
		private ArrayList elements;

		/// <summary>
		/// The action to perform.
		/// </summary>
		private String action;

		///<summary>
		///</summary>
		public AlterTableAction() {
			elements = new ArrayList();
		}

		/// <summary>
		/// Gets or sets the action to perform.
		/// </summary>
		public string Action {
			set { action = value; }
			get { return action; }
		}

		/// <summary>
		/// Returns the ArrayList that represents the parameters of this action.
		/// </summary>
		public ArrayList Elements {
			get { return elements; }
		}


		/// <inheritdoc/>
		public void PrepareExpressions(IExpressionPreparer preparer) {
			// This must search throw 'elements' for objects that we can prepare
			for (int i = 0; i < elements.Count; ++i) {
				Object ob = elements[i];
				if (ob is String) {
					// Do not need to prepare this
				} else if (ob is Expression) {
					((Expression)ob).Prepare(preparer);
				} else if (ob is IStatementTreeObject) {
					((IStatementTreeObject)ob).PrepareExpressions(preparer);
				} else {
					throw new DatabaseException(
											"Unrecognised expression: " + ob.GetType());
				}
			}
		}

		/// <inheritdoc/>
		public Object Clone() {
			// Shallow clone
			AlterTableAction v = (AlterTableAction)MemberwiseClone();
			ArrayList cloned_elements = new ArrayList();
			v.elements = cloned_elements;

			for (int i = 0; i < elements.Count; ++i) {
				Object ob = elements[i];
				if (ob is String) {
					// Do not need to clone this
				} else if (ob is Expression) {
					ob = ((Expression)ob).Clone();
				} else if (ob is IStatementTreeObject) {
					ob = ((IStatementTreeObject)ob).Clone();
				} else {
					throw new ApplicationException(ob.GetType().ToString());
				}
				cloned_elements.Add(ob);
			}

			return v;
		}

	}
}