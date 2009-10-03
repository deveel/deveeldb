// 
//  IQueryPlanNode.cs
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
using System.Text;

namespace Deveel.Data {
	///<summary>
	/// A node element of a query plan tree.
	///</summary>
	/// <remarks>
	/// A plan of a query is represented as a tree structure of such 
	/// nodes.  The design allows for plan nodes to be easily reorganised 
	/// for the construction of better plans.
	/// </remarks>
	public interface IQueryPlanNode : ICloneable {
		/// <summary>
		/// Evaluates the node and returns the result as a <see cref="Table"/>.
		/// </summary>
		/// <param name="context">The context the query is part of.</param>
		/// <remarks>
		/// The <see cref="IVariableResolver"/> instance of the given context 
		/// resolves any outer variables.
		/// </remarks>
		/// <returns>
		/// Returns a <see cref="Table"/> containing the result of the query.
		/// </returns>
		Table Evaluate(IQueryContext context);

		/// <summary>
		/// Discovers a list of <see cref="TableName"/> that represent the sources 
		/// that this query requires to complete itself.
		/// </summary>
		/// <param name="list"></param>
		/// <remarks>
		/// For example, if this is a query plan of two joined table, the fully 
		/// resolved names of both tables are returned.
		/// <para>
		/// The resultant list will not contain the same table name more than 
		/// once.
		/// </para>
		/// <para>
		/// <b>Note</b> If a table is aliased, the unaliased name is returned.
		/// </para>
		/// </remarks>
		/// <returns></returns>
		ArrayList DiscoverTableNames(ArrayList list);

		/// <summary>
		/// Discovers all the correlated variables in the plan (and plan children)
		/// that reference a particular layer.
		/// </summary>
		/// <param name="level"></param>
		/// <param name="list"></param>
		/// <remarks>
		/// For example, if we wanted to find all the CorrelatedVariable objects 
		/// that reference the current layer, we would typically call 
		/// <c>DiscoverCorrelatedVariables(0)</c>
		/// </remarks>
		/// <returns></returns>
		ArrayList DiscoverCorrelatedVariables(int level, ArrayList list);

		/// <summary>
		/// Writes a textural representation of the node to the <see cref="StringBuilder"/>
		/// at the given indent level.
		/// </summary>
		/// <param name="indent"></param>
		/// <param name="output"></param>
		void DebugString(int indent, StringBuilder output);
	}
}