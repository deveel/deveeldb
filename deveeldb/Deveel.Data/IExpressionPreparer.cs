//  
//  IExpressionPreparer.cs
//  
//  Author:
//       Antonello Provenzano <antonello@deveel.com>
//       Tobias Downer <toby@mckoi.com>
// 
//  Copyright (c) 2009 Deveel
// 
//  This program is free software: you can redistribute it and/or modify
//  it under the terms of the GNU General Public License as published by
//  the Free Software Foundation, either version 3 of the License, or
//  (at your option) any later version.
// 
//  This program is distributed in the hope that it will be useful,
//  but WITHOUT ANY WARRANTY; without even the implied warranty of
//  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
//  GNU General Public License for more details.
// 
//  You should have received a copy of the GNU General Public License
//  along with this program.  If not, see <http://www.gnu.org/licenses/>.

using System;

using Deveel.Data.Sql;

namespace Deveel.Data {
	/// <summary>
	/// An interface used to prepare an Expression object.
	/// </summary>
	/// <remarks>
	/// This interface is used to mutate an element of an <see cref="Expression"/>
	/// from one form to another.  For example, we may use this to translate a 
	/// <see cref="StatementTree"/> object to a <see cref="Statement"/> object.
	/// </remarks>
	public interface IExpressionPreparer {
		/// <summary>
		/// Verifies whether the instance of the interface can prepare
		/// the given element.
		/// </summary>
		/// <param name="element">The element object to verify.</param>
		/// <returns>
		/// Returns <b>true</b> if this preparer will prepare the given object in 
		/// an expression.
		/// </returns>
		bool CanPrepare(Object element);

		/// <summary>
		/// Returns the new translated object to be mutated from the given element.
		/// </summary>
		/// <param name="element"></param>
		/// <returns></returns>
		object Prepare(Object element);
	}
}