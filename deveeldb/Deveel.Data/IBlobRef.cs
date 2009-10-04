//  
//  IBlobRef.cs
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

using Deveel.Data.Store;

namespace Deveel.Data {
	/// <summary>
	/// A lightweight interface that is a reference to a blob in a <see cref="IBlobStore"/>.
	/// </summary>
	/// <remarks>
	/// This interface allows for data to be Read and written to a blob.  Writing to 
	/// a blob may be restricted depending on the state setting of the blob.
	/// </remarks>
	public interface IBlobRef : IBlobAccessor, IRef {
	}
}