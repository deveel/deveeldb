// 
//  ITableQueryDef.cs
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

namespace Deveel.Data {
	/// <summary>
	/// An interface to an object that describes characteristics of a table based
	/// object in the database.
	/// </summary>
	/// <remarks>
	/// This can represent anything that evaluates to a <see cref="Table"/>
	/// when the query plan is evaluated. It is used to represent data tables and views.
	/// <para>
	/// This object is used by the planner to see ahead of time what sort of table
	/// we are dealing with. 
	/// For example, a view is stored with a <see cref="DataTableDef"/>
	/// describing the resultant columns, and the <see cref="IQueryPlanNode"/>
	/// to produce the view result. The query planner requires the information in 
	/// <see cref="DataTableDef"/> to resolve references in the query, and the 
	/// <see cref="IQueryPlanNode"/> to add into the resultant plan tree.
	/// </para>
	/// </remarks>
	public interface ITableQueryDef {
		/// <summary>
		/// Returns an immutable <see cref="DataTableDef"/> that describes 
		/// the columns in this table source, and the name of the table.
		/// </summary>
		DataTableDef DataTableDef { get; }

		/// <summary>
		/// Returns a <see cref="IQueryPlanNode"/> that can be put into a plan 
		/// tree and can be evaluated to find the result of the table.
		/// </summary>
		/// <remarks>
		/// This property should always return a new object representing 
		/// the query plan.
		/// </remarks>
		IQueryPlanNode QueryPlanNode { get; }
	}
}